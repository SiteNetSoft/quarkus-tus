#!/usr/bin/env bash
# Run the Gradle build/tests inside a Podman container with hard CPU and RAM limits,
# so a runaway test run cannot take the host down.
#
# Usage:  scripts/sandbox-build.sh [gradle args...]      (default: build)
#   CPUS=4 MEM=8g scripts/sandbox-build.sh :integration-tests:integrationTest
#
# Reuses ~/.gradle and ~/.m2 caches. No MinIO, so the S3 suites skip (same as a plain local run).
set -euo pipefail

CPUS="${CPUS:-4}"
MEM="${MEM:-8g}"
PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${IMAGE:-localhost/quarkus-tus-sandbox:jdk25}"

# Build the sandbox image once (JDK + curl); rebuild with REBUILD=1.
if [ -n "${REBUILD:-}" ] || ! podman image exists "$IMAGE"; then
  podman build -q -t "$IMAGE" -f "$PROJECT/scripts/sandbox.Containerfile" "$PROJECT/scripts"
fi

mkdir -p "$HOME/.gradle" "$HOME/.m2"

exec podman run --rm ${TTY:--it} \
  --cpus "$CPUS" \
  --memory "$MEM" --memory-swap "$MEM" \
  --pids-limit 2048 \
  --userns=keep-id \
  -v "$PROJECT:/work:Z" -w /work \
  -v "$HOME/.gradle:/home/$(id -un)/.gradle:Z" \
  -v "$HOME/.m2:/home/$(id -un)/.m2:Z" \
  -e HOME="/home/$(id -un)" \
  -e GRADLE_OPTS="-Dorg.gradle.daemon=false" \
  -e JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=50" \
  "$IMAGE" \
  ./gradlew --no-daemon --console=plain "${@:-build}"
