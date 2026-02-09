#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="oqbridge"
TAG="latest"
PUSH=false
VERSION="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && git describe --tags --always --dirty 2>/dev/null || echo "dev")"
PLATFORM="linux/amd64"

# Auto-detect container runtime: prefer docker, fall back to podman
if command -v docker &>/dev/null; then
    RUNTIME="docker"
elif command -v podman &>/dev/null; then
    RUNTIME="podman"
else
    echo "Error: neither docker nor podman found in PATH" >&2
    exit 1
fi

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Build the oqbridge container image using docker or podman.

Options:
  -t IMAGE:TAG    Image name and tag (default: oqbridge:latest)
  --runtime R     Container runtime: docker or podman (default: auto-detect)
  --push          Push image to registry after build
  --platform P    Target platform(s), e.g. linux/amd64,linux/arm64
  -h, --help      Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -t)
            IFS=':' read -r IMAGE TAG <<< "$2"
            TAG="${TAG:-latest}"
            shift 2
            ;;
        --runtime)
            RUNTIME="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

FULL_IMAGE="${IMAGE}:${TAG}"
VERSION_IMAGE="${IMAGE}:${VERSION}"
echo "Building ${FULL_IMAGE} + ${VERSION_IMAGE} using ${RUNTIME} ..."

BUILD_ARGS=(
    -f "${SCRIPT_DIR}/Dockerfile"
    -t "${FULL_IMAGE}"
    -t "${VERSION_IMAGE}"
)

if [[ -n "${PLATFORM}" ]]; then
    BUILD_ARGS+=(--platform "${PLATFORM}")
fi

if [[ "${PUSH}" == true ]]; then
    BUILD_ARGS+=(--push)
fi

${RUNTIME} build "${BUILD_ARGS[@]}" "${PROJECT_ROOT}"

if [[ "${PUSH}" == false ]]; then
    echo "Image built: ${FULL_IMAGE}"
    echo "To push: $(basename "$0") -t ${FULL_IMAGE} --push"
fi
