#!/usr/bin/env bash

set -Eeuo pipefail

# A Python virtual environment is not portable between macOS and Linux.
# Build it in an image matching the Yandex Data Proc 2.0 nodes instead.
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REQUIREMENTS_FILE="${PROJECT_DIR}/requirements-prod.txt"
readonly OUTPUT_DIR="${PROJECT_DIR}/venvs"
readonly ARCHIVE_PATH="${OUTPUT_DIR}/venv.tar.gz"
readonly BUILD_IMAGE="${DATAPROC_VENV_BUILD_IMAGE:-ubuntu:20.04}"
readonly BUILD_PLATFORM="${DATAPROC_VENV_BUILD_PLATFORM:-linux/amd64}"

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: Docker is required to build a Linux-compatible Data Proc environment." >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Error: the Docker daemon is not running. Start Docker Desktop and retry." >&2
    exit 1
fi

if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then
    echo "Error: requirements file not found: ${REQUIREMENTS_FILE}" >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

echo "Building the Data Proc Python environment with ${BUILD_IMAGE} (${BUILD_PLATFORM})..."

docker run --rm \
    --platform "${BUILD_PLATFORM}" \
    --env "HOST_UID=$(id -u)" \
    --env "HOST_GID=$(id -g)" \
    --volume "${REQUIREMENTS_FILE}:/build/requirements-prod.txt:ro" \
    --volume "${OUTPUT_DIR}:/output" \
    "${BUILD_IMAGE}" \
    bash -Eeuo pipefail -c '
        export DEBIAN_FRONTEND=noninteractive

        apt-get update
        apt-get install --yes --no-install-recommends \
            ca-certificates \
            python3.8 \
            python3.8-venv

        python3.8 -m venv /tmp/dataproc-venv
        /tmp/dataproc-venv/bin/python -m pip install --upgrade \
            pip \
            wheel \
            venv-pack
        /tmp/dataproc-venv/bin/python -m pip install \
            --requirement /build/requirements-prod.txt

        /tmp/dataproc-venv/bin/venv-pack \
            --prefix /tmp/dataproc-venv \
            --output /output/venv.tar.gz \
            --force

        chown "${HOST_UID}:${HOST_GID}" /output/venv.tar.gz
    '

if ! tar -tzf "${ARCHIVE_PATH}" >/dev/null; then
    echo "Error: the generated archive is invalid: ${ARCHIVE_PATH}" >&2
    exit 1
fi

# venv-pack uses the target machine's Python interpreter. Data Proc 2.0 is
# Ubuntu 20.04 and provides Python 3.8 at /usr/bin/python3.8.
if ! tar -tvzf "${ARCHIVE_PATH}" \
    | grep -F "bin/python3.8 -> /usr/bin/python3.8" >/dev/null; then
    echo "Error: packed Python does not point to /usr/bin/python3.8." >&2
    tar -tvzf "${ARCHIVE_PATH}" | grep "bin/python" >&2 || true
    exit 1
fi

echo "Created Linux-compatible archive: ${ARCHIVE_PATH}"
