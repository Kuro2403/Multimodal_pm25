#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_NAME="${AIR_QUALITY_TEST_IMAGE:-airquality-app:test}"
FORCE_BUILD=false
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Run pytest in the project Docker image (default behavior).

Usage:
  ./scripts/run_tests.sh [--build] [--image <name>] [-- <pytest args>...]

Examples:
  ./scripts/run_tests.sh
  ./scripts/run_tests.sh -- -q
  ./scripts/run_tests.sh --build -- -k smoke

Options:
  --build           Force rebuild of the Docker image before running tests.
  --image <name>    Docker image tag to use/build (default: airquality-app:test).
  -h, --help        Show this help message.
EOF
}

if [[ "${1:-}" == "--inside" ]]; then
    shift
    cd "$BASE_DIR"
    export PYTHONPATH="$BASE_DIR/src:${PYTHONPATH:-}"
    exec python -m pytest "$@"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)
            FORCE_BUILD=true
            shift
            ;;
        --image)
            if [[ $# -lt 2 ]]; then
                echo "Error: --image requires a value." >&2
                exit 1
            fi
            IMAGE_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            PYTEST_ARGS+=("$@")
            break
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: Docker is required to run tests." >&2
    exit 1
fi

if [[ "$FORCE_BUILD" == "true" ]] || ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "Building test image: $IMAGE_NAME"
    docker build -t "$IMAGE_NAME" "$BASE_DIR"
fi

docker run --rm \
    --entrypoint /app/scripts/run_tests.sh \
    -e AIR_QUALITY_TEST_IN_DOCKER=1 \
    -v "$BASE_DIR:/app" \
    -w /app \
    "$IMAGE_NAME" \
    --inside "${PYTEST_ARGS[@]}"
