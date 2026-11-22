//! Local, disposable Docker workers. Never execute submitted code in the API.
//!
//! The controller has Docker access; workers have no host mounts or socket.
//! All payloads travel over stdin/stdout, with bounded output and wall time.

use std::env;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIAGNOSTIC_BYTES: usize = 256 * 1024;
const MAX_REPORT_BYTES: usize = 1024 * 1024;
const WORKER_LABEL: &str = "rektoff.submission-worker=true";

pub struct Sandbox {
    // Resolve the configured tag to an immutable image ID at startup.
    image: String,
    runtime: Option<String>,
    healthy: AtomicBool,
}

#[derive(Debug)]
pub struct Output {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

struct Captured {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Also request cleanup if the worker future is cancelled or panics. The
/// image's PID-1 timeout bounds worker lifetime if the API process itself dies.
struct WorkerGuard(Option<String>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Some(name) = self.0.take()
            && let Ok(mut child) = std::process::Command::new("docker")
                .args(["rm", "--force", &name])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
        {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
    }
}

fn docker() -> Command {
    let mut command = Command::new("docker");
    command.kill_on_drop(true);
    command
}

async fn read_bounded(reader: impl AsyncRead + Unpin, limit: usize) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| format!("could not read worker output: {e}"))?;
    if bytes.len() > limit {
        return Err("sandbox output limit exceeded".into());
    }
    Ok(bytes)
}

async fn capture(
    mut command: Command,
    input: &[u8],
    stdout_limit: usize,
    stderr_limit: usize,
    deadline: Duration,
) -> Result<Captured, String> {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not start Docker: {e}"))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let operation = async {
        let write = async {
            let result = stdin.write_all(input).await;
            drop(stdin);
            // A worker may reject input and exit early; its exit code and
            // diagnostics are more useful than a broken pipe from stdin.
            match result {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
                Err(e) => Err(format!("could not send worker input: {e}")),
            }
        };
        let wait = async {
            child
                .wait()
                .await
                .map_err(|e| format!("Docker wait failed: {e}"))
        };
        let (_, stdout, stderr, status) = tokio::try_join!(
            write,
            read_bounded(stdout, stdout_limit),
            read_bounded(stderr, stderr_limit),
            wait
        )?;
        Ok(Captured {
            status,
            stdout,
            stderr,
        })
    };
    let result = tokio::time::timeout(deadline, operation)
        .await
        .unwrap_or_else(|_| Err("sandbox timed out".into()));
    if result.is_err() {
        let _ = child.kill().await;
    }
    result
}

async fn control(args: &[&str]) -> Result<Vec<u8>, String> {
    let mut command = docker();
    command.args(args);
    let output = capture(command, &[], 64 * 1024, 16 * 1024, CONTROL_TIMEOUT).await?;
    if !output.status.success() {
        return Err(format!(
            "Docker command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output.stdout)
}

impl Sandbox {
    pub async fn discover() -> Result<Self, String> {
        let info = control(&["info", "--format", "{{json .}}"]).await?;
        let info: serde_json::Value =
            serde_json::from_slice(&info).map_err(|e| format!("invalid Docker info: {e}"))?;
        if info["OSType"] != "linux" {
            return Err("submission workers require Linux containers".into());
        }
        for feature in ["MemoryLimit", "SwapLimit", "PidsLimit", "CpuCfsQuota"] {
            if info[feature] != true {
                return Err(format!("Docker cannot enforce required limit: {feature}"));
            }
        }
        let seccomp = info["SecurityOptions"].as_array().is_some_and(|options| {
            options
                .iter()
                .any(|s| s.as_str().is_some_and(|s| s.starts_with("name=seccomp")))
        });
        if !seccomp {
            return Err("Docker seccomp support is required".into());
        }
        let image = env::var("SANDBOX_IMAGE").unwrap_or_else(|_| "rektoff-sandbox:local".into());
        if image.starts_with('-') || image.is_empty() {
            return Err("invalid SANDBOX_IMAGE".into());
        }
        let id = control(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
        let id = String::from_utf8_lossy(&id).trim().to_string();
        if !id
            .strip_prefix("sha256:")
            .is_some_and(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err("Docker did not return an immutable sandbox image ID".into());
        }
        let sandbox = Self {
            image: id,
            runtime: env::var("SANDBOX_RUNTIME").ok(),
            healthy: AtomicBool::new(true),
        };
        // Probe the actual restricted runtime, not just whether Docker exists.
        let probe = sandbox
            .run("probe", "probe", &[], Duration::from_secs(15))
            .await?;
        if !probe.success || probe.stdout != b"sandbox-ready\n" {
            return Err(format!(
                "sandbox startup probe failed: {}",
                String::from_utf8_lossy(&probe.stderr)
            ));
        }
        Ok(sandbox)
    }

    pub fn available(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn create_command(&self, name: &str, action: &str, level: &str) -> Command {
        let mut command = docker();
        command.args([
            "create",
            "--pull=never",
            "--interactive",
            "--name",
            name,
            "--label",
            WORKER_LABEL,
            "--network=none",
            "--ipc=private",
            "--cgroupns=private",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges=true",
            "--user=1000:1000",
            "--pids-limit=128",
            "--memory=4g",
            "--memory-swap=4g",
            "--cpus=2",
            "--ulimit=nofile=256:256",
            "--ulimit=core=0:0",
            "--tmpfs=/work:rw,nosuid,nodev,exec,size=2g,mode=0700,uid=1000,gid=1000",
            "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777",
            "--log-driver=none",
            "--restart=no",
        ]);
        if let Some(runtime) = &self.runtime {
            command.arg("--runtime").arg(runtime);
        }
        command.arg(&self.image).arg(action).arg(level);
        command
    }

    pub async fn run(
        &self,
        action: &str,
        level: &str,
        input: &[u8],
        deadline: Duration,
    ) -> Result<Output, String> {
        if !self.available() {
            return Err("sandbox is unavailable".into());
        }
        if !matches!(action, "build" | "grade" | "probe")
            || !level
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_alphanumeric())
            || !level
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("invalid sandbox request".into());
        }
        let name = format!("rektoff-job-{}", uuid::Uuid::new_v4());
        let mut guard = WorkerGuard(Some(name.clone()));
        let result = async {
            let created = capture(
                self.create_command(&name, action, level),
                &[],
                1024,
                16 * 1024,
                CONTROL_TIMEOUT,
            )
            .await?;
            if !created.status.success() {
                return Err(format!(
                    "could not create sandbox: {}",
                    String::from_utf8_lossy(&created.stderr)
                ));
            }
            let mut command = docker();
            command.args(["start", "--attach", "--interactive", &name]);
            let limit = if action == "build" {
                MAX_ARTIFACT_BYTES
            } else {
                MAX_REPORT_BYTES
            };
            let output = capture(command, input, limit, MAX_DIAGNOSTIC_BYTES, deadline).await?;
            let code = control(&["wait", &name]).await?;
            let success = output.status.success() && String::from_utf8_lossy(&code).trim() == "0";
            Ok(Output {
                success,
                stdout: output.stdout,
                stderr: output.stderr,
            })
        }
        .await;

        // Killing the docker CLI alone does not kill a container. Remove the
        // entire worker, including descendants, on success, error, or timeout.
        if let Err(error) = control(&["rm", "--force", &name]).await {
            // A failed create may leave nothing to remove. Confirm absence;
            // otherwise stop accepting jobs until the operator fixes Docker.
            let names = control(&[
                "ps",
                "--all",
                "--filter",
                &format!("name=^/{name}$"),
                "--format",
                "{{.Names}}",
            ])
            .await;
            if !names.is_ok_and(|names| names.is_empty()) {
                self.healthy.store(false, Ordering::Relaxed);
                return Err(format!(
                    "sandbox cleanup failed; submissions disabled: {error}"
                ));
            }
        }
        guard.0 = None;
        result
    }

    #[cfg(test)]
    pub fn test_config() -> Self {
        Self {
            image: "sha256:test".into(),
            runtime: None,
            healthy: AtomicBool::new(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn output_is_bounded_before_it_reaches_api_memory() {
        assert_eq!(read_bounded(&b"1234"[..], 4).await.unwrap(), b"1234");
        assert!(
            read_bounded(&b"12345"[..], 4)
                .await
                .unwrap_err()
                .contains("limit")
        );
    }

    #[tokio::test]
    async fn rejects_paths_and_arguments_before_starting_docker() {
        let sandbox = Sandbox::test_config();
        for level in ["../secret", "--privileged", "x;id", "/etc/passwd", ""] {
            assert!(
                sandbox
                    .run("build", level, &[], Duration::from_secs(1))
                    .await
                    .unwrap_err()
                    .contains("invalid")
            );
        }
    }

    #[test]
    fn workers_have_no_mounts_or_inherited_environment() {
        let sandbox = Sandbox::test_config();
        let command = sandbox.create_command("test-worker", "build", "level-01");
        let args: Vec<_> = command
            .as_std()
            .get_args()
            .map(|s| s.to_str().unwrap())
            .collect();
        for required in [
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--memory=4g",
            "--memory-swap=4g",
            "--pids-limit=128",
            "--log-driver=none",
        ] {
            assert!(args.contains(&required), "missing {required}");
        }
        assert!(!args.iter().any(|arg| matches!(
            *arg,
            "-v" | "--volume" | "--mount" | "--env" | "--env-file" | "--privileged"
        )));
    }

    #[tokio::test]
    #[ignore = "requires local Docker and a built rektoff-sandbox:local image"]
    async fn real_worker_build_grade_and_isolation() {
        let sandbox = Sandbox::discover()
            .await
            .expect("sandbox runtime available");
        let before = control(&[
            "ps",
            "--all",
            "--filter",
            &format!("label={WORKER_LABEL}"),
            "--format",
            "{{.Names}}",
        ])
        .await
        .unwrap();
        let levels_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../levels");
        let mut levels: Vec<_> = std::fs::read_dir(&levels_root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.join("program/src/lib.rs").is_file())
            .collect();
        levels.sort();
        for path in levels {
            let level = path.file_name().unwrap().to_str().unwrap();
            for (source_path, expected) in
                [("solution/lib.rs", true), ("program/src/lib.rs", false)]
            {
                let source = tokio::fs::read(path.join(source_path)).await.unwrap();
                let started = std::time::Instant::now();
                let built = sandbox
                    .run("build", level, &source, Duration::from_secs(60))
                    .await
                    .unwrap();
                let build_ms = started.elapsed().as_millis();
                assert!(
                    built.success,
                    "{level}: {}",
                    String::from_utf8_lossy(&built.stderr)
                );
                assert!(built.stdout.starts_with(b"\x7fELF"));
                let started = std::time::Instant::now();
                let graded = sandbox
                    .run("grade", level, &built.stdout, Duration::from_secs(20))
                    .await
                    .unwrap();
                assert!(
                    graded.success,
                    "{}",
                    String::from_utf8_lossy(&graded.stderr)
                );
                let stdout = String::from_utf8_lossy(&graded.stdout);
                let report: serde_json::Value =
                    serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
                assert_eq!(
                    report["passed"], expected,
                    "{level} {source_path}: {report}"
                );
                eprintln!(
                    "{level} {source_path}: build={build_ms}ms grade={}ms",
                    started.elapsed().as_millis()
                );
            }
        }
        let level = "level-01-missing-signer";
        let path = env::temp_dir().join(format!("rektoff-canary-{}", uuid::Uuid::new_v4()));
        let secret = format!("HOST_SECRET_{}", uuid::Uuid::new_v4());
        tokio::fs::write(&path, &secret).await.unwrap();
        let source = format!(
            "compile_error!(include_str!({:?}));",
            path.to_str().unwrap()
        );
        let attack = sandbox
            .run("build", level, source.as_bytes(), Duration::from_secs(60))
            .await
            .unwrap();
        tokio::fs::remove_file(path).await.unwrap();
        assert!(!attack.success);
        let diagnostics = String::from_utf8_lossy(&attack.stderr);
        assert!(diagnostics.contains("No such file"), "{diagnostics}");
        assert!(!diagnostics.contains(&secret));
        assert!(attack.stdout.is_empty());

        let source = include_bytes!("../../levels/level-01-missing-signer/solution/lib.rs");
        let error = sandbox
            .run("build", level, source, Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(error.contains("timed out"), "{error}");

        // A small source can generate enormous diagnostics. Stop reading and
        // remove the whole worker as soon as the diagnostic bound is crossed.
        let message = "A".repeat(4096);
        let repetitions = vec!["message!()"; 40].join(",");
        let bomb = format!(
            "macro_rules! message {{ () => {{ \"{message}\" }}; }} \
             macro_rules! big {{ () => {{ concat!({repetitions}) }}; }} \
             compile_error!(big!()); compile_error!(big!());"
        );
        assert!(bomb.len() < 64 * 1024);
        let error = sandbox
            .run("build", level, bomb.as_bytes(), Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(error.contains("output limit"), "{error}");
        let after = control(&[
            "ps",
            "--all",
            "--filter",
            &format!("label={WORKER_LABEL}"),
            "--format",
            "{{.Names}}",
        ])
        .await
        .unwrap();
        assert_eq!(
            before, after,
            "no workers may remain after success, failure, timeout, or output overflow"
        );
    }
}
