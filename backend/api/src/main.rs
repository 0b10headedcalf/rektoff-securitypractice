//! HTTP API for the practice platform.
//!
//! Accepts a player's source file, compiles it to SBF, grades the result with
//! the grader, and reports progress over Server-Sent Events.
//!
//! Jobs run one at a time through a bounded queue. All state is in memory.

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};

mod sandbox;

use sandbox::Sandbox;

const MAX_SOURCE_BYTES: usize = 64 * 1024;
const MAX_COMPILE_ERROR_BYTES: usize = 8 * 1024;
const QUEUE_CAPACITY: usize = 20;
const BUILD_TIMEOUT: Duration = Duration::from_secs(60);
const TEST_TIMEOUT: Duration = Duration::from_secs(20);
const JOB_RETENTION: Duration = Duration::from_secs(600);

struct Level {
    description: String,
    source: String,
}

struct Job {
    id: String,
    level: String,
    source: String,
    enqueued: Instant,
    state: watch::Sender<JobState>,
}

#[derive(Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Stage {
    #[default]
    Queued,
    Building,
    Testing,
    Done,
}

#[derive(Clone, Default, Serialize)]
struct CheckOutcome {
    name: String,
    passed: bool,
    /// Why the check failed, in the test's own words.
    detail: Option<String>,
}

/// Everything the player is allowed to see about a job.
#[derive(Clone, Default, Serialize)]
struct JobState {
    stage: Stage,
    passed: Option<bool>,
    checks: Vec<CheckOutcome>,
    compile_error: Option<String>,
    error: Option<String>,
    queue_ms: Option<u64>,
    build_ms: Option<u64>,
    test_ms: Option<u64>,
}

struct App {
    sandbox: Option<Arc<Sandbox>>,
    levels: HashMap<String, Level>,
    jobs: Mutex<HashMap<String, (Instant, watch::Receiver<JobState>)>>,
    queue: mpsc::Sender<Job>,
}

#[derive(Serialize)]
struct LevelResponse<'a> {
    id: &'a str,
    description: &'a str,
    source: &'a str,
}

#[derive(Deserialize)]
struct SubmitRequest {
    level: String,
    source: String,
}

#[derive(Serialize)]
struct SubmitResponse {
    id: String,
}

type ApiError = (StatusCode, &'static str);

#[derive(Serialize)]
struct LevelSummary<'a> {
    id: &'a str,
    title: &'a str,
}

/// Every level in play order (ids start with their number), for the level picker.
async fn list_levels(State(app): State<Arc<App>>) -> Json<serde_json::Value> {
    let mut levels: Vec<LevelSummary> = app
        .levels
        .iter()
        .map(|(id, level)| {
            let heading = level
                .description
                .lines()
                .find_map(|line| line.strip_prefix("# "));
            LevelSummary {
                id,
                title: heading.unwrap_or(id),
            }
        })
        .collect();
    levels.sort_by_key(|level| level.id);
    Json(serde_json::to_value(levels).expect("levels serialize"))
}

async fn get_level(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let level = app
        .levels
        .get(&id)
        .ok_or((StatusCode::NOT_FOUND, "unknown level"))?;
    let body = LevelResponse {
        id: &id,
        description: &level.description,
        source: &level.source,
    };
    Ok(Json(serde_json::to_value(body).expect("level serializes")))
}

async fn submit(
    State(app): State<Arc<App>>,
    Json(request): Json<SubmitRequest>,
) -> Result<Json<SubmitResponse>, ApiError> {
    if !app
        .sandbox
        .as_ref()
        .is_some_and(|sandbox| sandbox.available())
    {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "submissions are unavailable because the sandbox is not ready",
        ));
    }
    if !app.levels.contains_key(&request.level) {
        return Err((StatusCode::NOT_FOUND, "unknown level"));
    }
    if request.source.len() > MAX_SOURCE_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "source too large"));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let (state, receiver) = watch::channel(JobState::default());
    let job = Job {
        id: id.clone(),
        level: request.level,
        source: request.source,
        enqueued: Instant::now(),
        state,
    };
    app.queue.try_send(job).map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "queue is full, try again shortly",
        )
    })?;

    let mut jobs = app.jobs.lock().expect("jobs lock");
    jobs.retain(|_, (created, _)| created.elapsed() < JOB_RETENTION);
    jobs.insert(id.clone(), (Instant::now(), receiver));
    Ok(Json(SubmitResponse { id }))
}

fn job_receiver(app: &App, id: &str) -> Result<watch::Receiver<JobState>, ApiError> {
    let jobs = app.jobs.lock().expect("jobs lock");
    jobs.get(id)
        .map(|(_, rx)| rx.clone())
        .ok_or((StatusCode::NOT_FOUND, "unknown job"))
}

async fn get_job(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<JobState>, ApiError> {
    let receiver = job_receiver(&app, &id)?;
    let state = receiver.borrow().clone();
    Ok(Json(state))
}

/// Streams every state change. A watch channel always yields its current
/// value first, so a client that connects after the job finished still gets
/// the final result.
async fn job_events(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let receiver = job_receiver(&app, &id)?;
    let mut finished = false;
    let stream = WatchStream::new(receiver)
        .take_while(move |state| {
            let keep = !finished;
            finished = state.stage == Stage::Done;
            keep
        })
        .map(|state| {
            let name = if state.stage == Stage::Done {
                "done"
            } else {
                "status"
            };
            let data = serde_json::to_string(&state).expect("state serializes");
            Ok(Event::default().event(name).data(data))
        });
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(5))))
}

struct BuildFailure(String);

async fn build(sandbox: &Sandbox, level: &str, source: &str) -> Result<Vec<u8>, BuildFailure> {
    let output = sandbox
        .run("build", level, source.as_bytes(), BUILD_TIMEOUT)
        .await
        .map_err(BuildFailure)?;
    if !output.success {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let start = stderr.len().saturating_sub(MAX_COMPILE_ERROR_BYTES);
        let start = (start..=stderr.len())
            .find(|&i| stderr.is_char_boundary(i))
            .unwrap_or(0);
        let message = if stderr.is_empty() {
            "sandbox build failed"
        } else {
            &stderr[start..]
        };
        return Err(BuildFailure(message.to_string()));
    }
    if !output.stdout.starts_with(b"\x7fELF") {
        return Err(BuildFailure("build produced no program".into()));
    }
    Ok(output.stdout)
}

#[derive(Deserialize)]
struct GraderCheck {
    name: String,
    passed: bool,
    detail: Option<String>,
}

#[derive(Deserialize)]
struct GraderReport {
    passed: bool,
    checks: Vec<GraderCheck>,
}

async fn grade(sandbox: &Sandbox, level: &str, program: &[u8]) -> Result<GraderReport, String> {
    let output = sandbox.run("grade", level, program, TEST_TIMEOUT).await?;
    if !output.success {
        return Err(format!(
            "sandbox grader failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().last().unwrap_or_default();
    serde_json::from_str(line).map_err(|e| format!("bad grader output: {e}"))
}

async fn run_job(app: &App, job: Job) {
    let Some(sandbox) = app.sandbox.as_deref().filter(|sandbox| sandbox.available()) else {
        job.state.send_modify(|s| {
            s.stage = Stage::Done;
            s.passed = Some(false);
            s.error = Some("the sandbox is unavailable, please try again later".into());
        });
        return;
    };
    let queue_ms = job.enqueued.elapsed().as_millis() as u64;
    job.state.send_modify(|s| {
        s.stage = Stage::Building;
        s.queue_ms = Some(queue_ms);
    });

    let started = Instant::now();
    let built = build(sandbox, &job.level, &job.source).await;
    let build_ms = started.elapsed().as_millis() as u64;
    let program = match built {
        Ok(program) => program,
        Err(BuildFailure(message)) => {
            job.state.send_modify(|s| {
                s.stage = Stage::Done;
                s.build_ms = Some(build_ms);
                s.passed = Some(false);
                s.compile_error = Some(message);
            });
            println!(
                "job {} level={} queue={queue_ms}ms build={build_ms}ms result=build-failed",
                job.id, job.level
            );
            return;
        }
    };
    job.state.send_modify(|s| {
        s.stage = Stage::Testing;
        s.build_ms = Some(build_ms);
    });

    let started = Instant::now();
    let graded = grade(sandbox, &job.level, &program).await;
    let test_ms = started.elapsed().as_millis() as u64;
    match graded {
        Ok(report) => {
            for check in report.checks.iter().filter(|c| !c.passed) {
                println!(
                    "job {} check failed: {:?}: {:?}",
                    job.id, check.name, check.detail
                );
            }
            println!(
                "job {} level={} queue={queue_ms}ms build={build_ms}ms test={test_ms}ms passed={}",
                job.id, job.level, report.passed
            );
            job.state.send_modify(|s| {
                s.stage = Stage::Done;
                s.test_ms = Some(test_ms);
                s.passed = Some(report.passed);
                s.checks = report
                    .checks
                    .into_iter()
                    .map(|c| CheckOutcome {
                        name: c.name,
                        passed: c.passed,
                        detail: c.detail,
                    })
                    .collect();
            });
        }
        Err(message) => {
            eprintln!("job {} grading error: {message}", job.id);
            job.state.send_modify(|s| {
                s.stage = Stage::Done;
                s.test_ms = Some(test_ms);
                s.passed = Some(false);
                s.error = Some("the grader failed, please try again".into());
            });
        }
    }
}

/// Source and descriptions are served by the API. All compilation and its
/// prewarming happen in the sandbox image build, never on the API host.
async fn load_levels(levels_dir: &Path) -> HashMap<String, Level> {
    let mut levels = HashMap::new();
    let mut entries = tokio::fs::read_dir(levels_dir)
        .await
        .expect("LEVELS_DIR is readable");
    while let Some(entry) = entries.next_entry().await.expect("read LEVELS_DIR") {
        let program_dir = entry.path().join("program");
        let Ok(source) = tokio::fs::read_to_string(program_dir.join("src/lib.rs")).await else {
            continue;
        };
        let id = entry.file_name().to_string_lossy().to_string();
        let description = tokio::fs::read_to_string(entry.path().join("description.md"))
            .await
            .unwrap_or_default();
        levels.insert(
            id,
            Level {
                description,
                source,
            },
        );
    }
    levels
}

fn env_path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(env::var(key).unwrap_or_else(|_| default.to_string()))
}

#[tokio::main]
async fn main() {
    let levels_dir = env_path("LEVELS_DIR", "levels");
    let port = env::var("PORT").unwrap_or_else(|_| "8787".to_string());
    let sandbox = match Sandbox::discover().await {
        Ok(sandbox) => {
            println!("local submission sandbox is ready");
            Some(Arc::new(sandbox))
        }
        Err(error) => {
            eprintln!("submissions disabled: {error}");
            None
        }
    };
    let levels = load_levels(&levels_dir).await;
    assert!(
        !levels.is_empty(),
        "no levels found in {}",
        levels_dir.display()
    );

    let (queue, mut jobs) = mpsc::channel::<Job>(QUEUE_CAPACITY);
    let app = Arc::new(App {
        sandbox,
        levels,
        jobs: Mutex::new(HashMap::new()),
        queue,
    });

    let worker_app = app.clone();
    tokio::spawn(async move {
        while let Some(job) = jobs.recv().await {
            run_job(&worker_app, job).await;
        }
    });

    let router = Router::new()
        .route("/api/levels", get(list_levels))
        .route("/api/levels/{id}", get(get_level))
        .route("/api/submit", post(submit))
        .route("/api/submit/{id}", get(get_job))
        .route("/api/submit/{id}/events", get(job_events))
        .with_state(app);

    let address = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("bind port");
    println!("api listening on http://{address}");
    axum::serve(listener, router).await.expect("server runs");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(sandbox_ready: bool) -> (Arc<App>, mpsc::Receiver<Job>) {
        let (queue, jobs) = mpsc::channel(QUEUE_CAPACITY);
        let levels = HashMap::from([(
            "test-level".into(),
            Level {
                description: String::new(),
                source: String::new(),
            },
        )]);
        (
            Arc::new(App {
                sandbox: sandbox_ready.then(|| Arc::new(Sandbox::test_config())),
                levels,
                jobs: Mutex::new(HashMap::new()),
                queue,
            }),
            jobs,
        )
    }

    #[tokio::test]
    async fn disabled_submissions_never_reach_worker() {
        let (app, mut queue) = app(false);
        let result = submit(
            State(app.clone()),
            Json(SubmitRequest {
                level: "test-level".into(),
                source: "compile_error!(include_str!(\"/etc/passwd\"));".into(),
            }),
        )
        .await;

        assert!(matches!(result, Err((StatusCode::SERVICE_UNAVAILABLE, _))));
        assert!(matches!(
            queue.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(app.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ready_sandbox_accepts_jobs_without_running_them_in_the_api() {
        let (app, mut queue) = app(true);
        let result = submit(
            State(app.clone()),
            Json(SubmitRequest {
                level: "test-level".into(),
                source: "// trusted demo source".into(),
            }),
        )
        .await
        .unwrap();

        let job = queue.try_recv().unwrap();
        assert_eq!(job.id, result.0.id);
        assert_eq!(job.source, "// trusted demo source");
        assert!(app.jobs.lock().unwrap().contains_key(&job.id));
    }
}
