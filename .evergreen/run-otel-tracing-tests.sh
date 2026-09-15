#!/bin/bash

set -o errexit

# Runs the OTel trace-context propagation prose tests (DRIVERS-3454) against a deployment
# provisioned by drivers-evergreen-tools orchestration with OTEL=1 (DRIVERS-3605).
#
# Supported/used environment variables:
#   MONGODB_URI     Connection string for the OTel-enabled deployment (from mo-expansion)
#   OTEL_TRACE_DIR  Directory the server exports OTLP JSON span batches into (from mo-expansion)
#   JAVA_VERSION    JDK to run the tests with

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/setup-env.bash"

if [ -z "$OTEL_TRACE_DIR" ]; then
  echo "OTEL_TRACE_DIR is not set: orchestration was not started with OTEL=1" >&2
  exit 1
fi

echo "Running OTel trace-context propagation prose tests"

./gradlew -version

# Toolchain auto-detection does not scan /opt/java, so point Gradle at the JDKs explicitly. On the macOS
# images the JDK content lives at the root of /opt/java/jdkNN, but a leftover macOS bundle skeleton with an
# EMPTY Contents/Home sits beside it; Gradle prefers Contents/Home whenever it exists and then rejects the
# installation ("does not contain a java executable") without falling back to the root. Neutralize that by
# removing the empty Contents/Home, or failing that, by symlinking the JDK into a clean bundle-free dir.
sanitize_jdk() {
  local jdk_dir=$1
  if [ -x "$jdk_dir/bin/java" ] && [ -d "$jdk_dir/Contents/Home" ] && [ ! -x "$jdk_dir/Contents/Home/bin/java" ]; then
    if ! rmdir "$jdk_dir/Contents/Home" 2>/dev/null; then
      local clean="$HOME/gradle-jdks/$(basename "$jdk_dir")"
      mkdir -p "$clean"
      local entry
      for entry in bin conf include jmods legal lib release; do
        [ -e "$jdk_dir/$entry" ] && ln -sfn "$jdk_dir/$entry" "$clean/$entry"
      done
      jdk_dir=$clean
    fi
  fi
  echo "$jdk_dir"
}
TOOLCHAIN_PATHS="$(sanitize_jdk "$JDK17"),$(sanitize_jdk "$JDK21")"
echo "Gradle toolchain paths: ${TOOLCHAIN_PATHS}"

./gradlew --stacktrace --info \
    -Porg.gradle.java.installations.paths="${TOOLCHAIN_PATHS}" \
    -PjavaVersion="${JAVA_VERSION:-21}" \
    -Dorg.mongodb.test.uri="${MONGODB_URI}" \
    -Dorg.mongodb.test.otel.trace.dir="${OTEL_TRACE_DIR}" \
    driver-sync:test --tests ServerSpanLinkageProseTest
