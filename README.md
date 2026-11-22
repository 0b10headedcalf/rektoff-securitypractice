# Rektoff Practice (demo)

Fix a planted bug in a Solana program in the browser, submit, and tests attack
the compiled result.

- `frontend/`: Bun server and CodeMirror page. Forwards `/api/*` to the backend.
- `backend/api/`: Rust (axum) API. Queues submissions, compiles them, runs the grader.
- `backend/grader/`: loads a compiled program into LiteSVM and runs a level's checks.
- `backend/levels/`: one folder per level.

## Add a level

Add a folder to `backend/levels/` and rebuild. The grader picks it up by itself.

```
backend/levels/level-04-some-bug/
  description.md       shown next to the editor; starts with "# Level 4: ..."
  program/             the buggy Anchor crate; players edit src/lib.rs
  solution/lib.rs      the reference fix
  tests/checks.rs      the attack, plus checks that normal use still works
```

Copy an existing level to start. Give the program a new id in `declare_id!` and
use the same id in `checks.rs`. The folder name is the level id, and levels
appear in name order. Helpers for checks are in `backend/grader/src/support.rs`.

## Run locally

Needs Rust, Bun, and Docker running with Linux containers and cgroup resource
limits. The host no longer needs the Solana CLI. Build the trusted images once:

```sh
docker build -t rektoff-practice .
docker build -f Dockerfile.sandbox -t rektoff-sandbox:local .
./start.sh
```

Then open http://localhost:3000. Ports: `WEB_PORT` (3000), `API_PORT` (8787).
The API user needs permission to use the local Docker daemon. Prefer a rootless
Docker daemon; the controller's Docker access is privileged infrastructure access.

The API probes the actual restricted worker at startup. If Docker, the image,
seccomp, namespaces, or required resource limits are unavailable, submission
requests return HTTP 503. There is no unsandboxed fallback or unsafe opt-in.
After fixing runtime configuration, restart the API to repeat the probe.

`SANDBOX_IMAGE` selects an already-built local image (default:
`rektoff-sandbox:local`); its image ID is pinned for the API process lifetime.
`SANDBOX_RUNTIME` optionally selects an installed Docker runtime, such as `runsc`.
Runtime settings must pass the same startup probe. Image pulls never happen on
submission. Rebuild both images and restart the API after changing levels,
dependencies, toolchains, or checks.

## Run the controller in Docker

After building both images above, a controller using the default local Docker
socket can be started with:

```sh
docker run --rm -p 3000:3000 --memory 1g --cpus 1 --pids-limit 256 \
  --group-add "$(stat -c '%g' /var/run/docker.sock)" \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  rektoff-practice
```

Only the trusted controller gets the Docker socket. Worker containers never get
that socket, host directories, API state, or runtime credentials. For rootless
Docker, substitute its socket path as the mount source and use that socket's
group ID. Worker limits are separate from the controller's limits, so budget
host capacity for the controller plus one 4 GiB worker.

## Submission isolation

Each submission uses a fresh build container, followed by a separate fresh
grading container. Source and compiled SBF bytes travel over stdin/stdout; the
API never writes them into a host build directory or runs a compiler or grader
on the host. The worker image contains only trusted toolchains, dependency
caches, starter programs, and the grader; application files, solution sources,
and check source files are removed from its visible filesystem.

Workers run as UID/GID 1000 with no network, no Linux capabilities, no new
privileges, Docker's seccomp policy, and private PID, IPC, network, mount, and
cgroup namespaces. The root filesystem is read-only. Build storage uses a
2 GiB work tmpfs and a 64 MiB temporary tmpfs, both charged against the worker's
4 GiB memory limit. Swap is disabled. Each worker is limited to 2 CPU cores,
128 processes/threads, and 256 open file descriptors per process.

Build and grading execution have 60-second and 20-second controller deadlines;
Docker control operations have separate 15-second deadlines. The image also
has a 90-second PID-1 lifetime limit. The controller force-removes the entire
container after success, failure, output overflow, or timeout, and requests
cleanup when a worker task is cancelled. A cleanup failure that leaves a worker
present disables further submissions. Docker's container logs are disabled;
artifact output is capped at 8 MiB, grading output at 1 MiB, and stderr at
256 KiB. Compiler errors shown to players are further limited to 8 KiB.

Dependencies are prebuilt at the exact paths used by jobs. Each build copies a
trusted cache into private storage, so submissions do not share writable caches.
This shifts dependency warm-up to image creation and avoids reusing a previous
player's program or build state.

This is Linux container isolation, not a guarantee against kernel/runtime
vulnerabilities. Keep the host and Docker patched; a rootless daemon reduces
host privileges, and a compatible stronger runtime can add another boundary.
The worker images must remain trusted and contain no secrets. Destroy stopped
workers left by a controller crash with the scoped command below once no jobs
are running:

```sh
docker container prune --force --filter label=rektoff.submission-worker=true
```

## Verify

```sh
cd backend
cargo test -p api --locked
cargo test -p api --locked real_worker_build_grade_and_isolation -- --ignored --nocapture
```

The Docker integration test builds and grades the reference fixes and buggy
starters, checks that a host canary cannot be read through `include_str!`,
exercises diagnostic-output overflow and timeout cleanup, and verifies no test
worker remains. The startup probe checks the actual network, capability,
seccomp, filesystem, and cgroup restrictions inside the worker.
