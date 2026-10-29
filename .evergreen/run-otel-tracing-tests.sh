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
./gradlew --stacktrace --info \
    -PjavaVersion="${JAVA_VERSION:-21}" \
    -Dorg.mongodb.test.uri="${MONGODB_URI}" \
    -Dorg.mongodb.test.otel.trace.dir="${OTEL_TRACE_DIR}" \
    driver-sync:test --tests ServerSpanLinkageProseTest
