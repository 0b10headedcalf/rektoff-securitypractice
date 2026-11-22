#!/bin/sh
# Runs ONLY inside the disposable worker. stdout is the artifact or JSON report.
set -eu
umask 077
action="${1:-}"
level="${2:-}"
case "$level" in
  ''|*[!a-zA-Z0-9_-]*) echo 'invalid level' >&2; exit 2 ;;
esac

case "$action" in
  probe)
    # Check the restrictions inside the actual worker, not just CLI flags.
    test "$(id -u)" = 1000
    test ! -e /var/run/docker.sock
    test ! -e /app/bin/api
    test ! -e /opt/programs/level-01-missing-signer/solution
    grep -q '^CapEff:[[:space:]]*0000000000000000$' /proc/self/status
    grep -q '^NoNewPrivs:[[:space:]]*1$' /proc/self/status
    grep -q '^Seccomp:[[:space:]]*2$' /proc/self/status
    # Only loopback is present in an isolated --network=none namespace.
    test "$(ls /sys/class/net)" = lo
    test "$(wc -l < /proc/net/route)" -eq 1
    test "$(cat /sys/fs/cgroup/memory.max)" = 4294967296
    test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0
    test "$(cat /sys/fs/cgroup/pids.max)" = 128
    test "$(cat /sys/fs/cgroup/cpu.max)" = '200000 100000'
    if touch /home/app/write-test 2>/dev/null; then exit 1; fi
    printf 'sandbox-ready\n'
    ;;
  build)
    test -d "/opt/programs/$level"
    mkdir -p /work/cargo
    # Cargo needs writable lock/cache metadata, but dependencies stay read-only.
    ln -s /usr/local/cargo/registry /work/cargo/registry
    if test -d /usr/local/cargo/git; then
      ln -s /usr/local/cargo/git /work/cargo/git
    fi
    if test -f /usr/local/cargo/.global-cache; then
      cp /usr/local/cargo/.global-cache /work/cargo/.global-cache
    fi
    cp -a "/opt/programs/$level" /work/program
    cd /work/program
    rm -f target/deploy/*.so
    cat > src/lib.rs
    cargo-build-sbf --jobs 2 --offline --skip-tools-install --no-rustup-override -- --locked >&2
    # Only one artifact is accepted. Never reuse a previous submission's output.
    set -- target/deploy/*.so
    test "$#" -eq 1 && test -f "$1" && test ! -L "$1"
    cat "$1"
    ;;
  grade)
    cat > /work/program.so
    exec /opt/grader "$level" /work/program.so
    ;;
  *) echo 'invalid action' >&2; exit 2 ;;
esac
