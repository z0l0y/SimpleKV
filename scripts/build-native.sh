#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="${1:-$PROJECT_ROOT/data}"

PROJECT_ROOT_FOR_MVN="$PROJECT_ROOT"
if command -v cygpath >/dev/null 2>&1; then
    PROJECT_ROOT_FOR_MVN="$(cygpath -m "$PROJECT_ROOT")"
fi

find_graalvm_home() {
    has_native_image() {
        [ -x "$1/bin/native-image" ] || [ -f "$1/bin/native-image.cmd" ] || [ -f "$1/bin/native-image.exe" ]
    }

    is_graalvm_25() {
        printf '%s' "$1" | grep -qiE 'graalvm.*25|25\.'
    }

    if [ -n "${GRAALVM_HOME:-}" ] && has_native_image "$GRAALVM_HOME" && is_graalvm_25 "$GRAALVM_HOME"; then
        printf '%s' "$GRAALVM_HOME"
        return 0
    fi

    if [ -n "${JAVA_HOME:-}" ] && has_native_image "$JAVA_HOME" && is_graalvm_25 "$JAVA_HOME"; then
        printf '%s' "$JAVA_HOME"
        return 0
    fi

    for base in /d/Develop/Env/Java /cygdrive/d/Develop/Env/Java; do
        for candidate in "$base"/*graalvm*25* "$base"/*GraalVM*25* "$base"/*graalvm*jdk-25*; do
            if [ -d "$candidate" ] && has_native_image "$candidate" && is_graalvm_25 "$candidate"; then
                printf '%s' "$candidate"
                return 0
            fi
        done
    done

    return 1
}

ensure_native_image_java25() {
    local native_image_bin="$1/bin/native-image"
    [ -f "$native_image_bin.cmd" ] && native_image_bin="$native_image_bin.cmd"
    [ -f "$native_image_bin.exe" ] && native_image_bin="$native_image_bin.exe"

    local version_output
    version_output="$($native_image_bin --version 2>&1 || true)"
    if ! printf '%s' "$version_output" | grep -qiE 'java\s*version\s*25|jdk\s*25|graalvm\s*25|\b25\.'; then
        echo "[simplekv] native-image version check failed." >&2
        echo "[simplekv] Detected output:" >&2
        echo "$version_output" >&2
        echo "[simplekv] Please use GraalVM Native Image with Java 25 support." >&2
        exit 1
    fi
}

GRAALVM_HOME_CANDIDATE="$(find_graalvm_home || true)"
if [ -n "$GRAALVM_HOME_CANDIDATE" ]; then
    if command -v cygpath >/dev/null 2>&1; then
        GRAALVM_HOME_CANDIDATE="$(cygpath -m "$GRAALVM_HOME_CANDIDATE")"
    fi
    export GRAALVM_HOME="$GRAALVM_HOME_CANDIDATE"
    export JAVA_HOME="$GRAALVM_HOME_CANDIDATE"
else
    echo "[simplekv] GraalVM JDK not found. Set GRAALVM_HOME to a GraalVM 25 installation." >&2
    exit 1
fi

ensure_native_image_java25 "$GRAALVM_HOME"

echo "[simplekv] building native image..."
mvn -f "$PROJECT_ROOT_FOR_MVN/pom.xml" -pl simplekv-app -am -Pnative clean package

CLI_EXE_PATH="$SCRIPT_DIR/../simplekv-app/target/simplekv-cli.exe"
SERVER_EXE_PATH="$SCRIPT_DIR/../simplekv-app/target/simplekv-server.exe"

if [ -f "$CLI_EXE_PATH" ]; then
    echo "[simplekv] native executable ready: $CLI_EXE_PATH"
    echo "[simplekv] example: $CLI_EXE_PATH -Dsimplekv.storage.data-dir=$DATA_DIR put demo-key demo-value"
else
    echo "[simplekv] CLI executable not found yet (check build logs)."
fi

if [ -f "$SERVER_EXE_PATH" ]; then
    echo "[simplekv] native server executable ready: $SERVER_EXE_PATH"
    echo "[simplekv] example: $SERVER_EXE_PATH --simplekv.storage.data-dir=$DATA_DIR"
else
    echo "[simplekv] Server executable not found yet (check build logs)."
fi
