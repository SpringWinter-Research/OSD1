#!/bin/sh
# Record dependency and image advisories. A finding does not fail this script.
# A scanner that cannot run does.
set -eu
cd "$(dirname "$0")/.."

. "$HOME/.cargo/env"
if ! command -v cargo-audit >/dev/null 2>&1; then
  cargo install cargo-audit --locked
fi

echo "=== cargo audit ==="
set +e
cargo audit
audit_rc=$?
set -e
echo "cargo_audit_exit $audit_rc"
if [ "$audit_rc" -ne 0 ] && [ "$audit_rc" -ne 1 ]; then
  echo "cargo audit failed to run" >&2
  exit "$audit_rc"
fi

image="${1:-osd1-osd1:latest}"
echo "=== trivy image $image ==="
set +e
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy:latest \
  image --exit-code 0 --severity UNKNOWN,LOW,MEDIUM,HIGH,CRITICAL "$image"
trivy_rc=$?
set -e
echo "trivy_exit $trivy_rc"
if [ "$trivy_rc" -ne 0 ]; then
  echo "trivy failed to run" >&2
  exit "$trivy_rc"
fi
echo "scan recorded"
