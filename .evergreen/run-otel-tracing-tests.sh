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

# Toolchain auto-detection does not scan /opt/java (notably on macOS hosts), so point Gradle at the JDKs
# explicitly. The macOS images lay the JDKs out with an incomplete Contents/Home bundle dir that confuses
# Gradle's probe, so resolve each JDK's real home from the JVM itself rather than trusting the directory.
resolve_java_home() {
  "$1/bin/java" -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java\.home = //p'
}
ls -la /opt/java/ "$JDK17" "$JDK17/Contents" 2>&1 || true
TOOLCHAIN_PATHS="$(resolve_java_home "$JDK17"),$(resolve_java_home "$JDK21")"
echo "Gradle toolchain paths: ${TOOLCHAIN_PATHS}"

./gradlew --stacktrace --info \
    -Porg.gradle.java.installations.paths="${TOOLCHAIN_PATHS}" \
    -PjavaVersion="${JAVA_VERSION:-21}" \
    -Dorg.mongodb.test.uri="${MONGODB_URI}" \
    -Dorg.mongodb.test.otel.trace.dir="${OTEL_TRACE_DIR}" \
    driver-sync:test --tests ServerSpanLinkageProseTest
