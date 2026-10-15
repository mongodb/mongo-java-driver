#!/bin/bash

# Runs the OpenTelemetry overhead benchmark (DRIVERS-3620) under every tracing
# configuration, interleaved and rotated, and writes results.json in the shape
# that "send-dashboard-data" uploads to the performance dashboard.
#
# Env: OTEL_BENCH_REPS (default 3), OTEL_BENCH_MIN_TIME (default 60), OTEL_BENCH_ITERATIONS (default 100)

set -o xtrace
set -o errexit

rm -rf driver-performance-test-data
git clone https://github.com/mongodb-labs/driver-performance-test-data.git
cd driver-performance-test-data
tar xf extended_bson.tgz
tar xf single_and_multi_document.tgz
cd ..

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

export TEST_PATH="${PROJECT_DIRECTORY}/driver-performance-test-data"
export OUTPUT_DIR="${PROJECT_DIRECTORY}/otel-bench-results"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

start_time=$(date +%s)
python3 -I driver-benchmarks/otel-bench.py \
    --data "${TEST_PATH}" \
    --out "${OUTPUT_DIR}" \
    --reps "${OTEL_BENCH_REPS:-3}" \
    --min-time "${OTEL_BENCH_MIN_TIME:-60}" \
    --iterations "${OTEL_BENCH_ITERATIONS:-100}" \
    --configs off,api-only,sdk-ratio,sdk-always \
    --dashboard "${OUTPUT_FILE}"
end_time=$(date +%s)
echo "otel benchmark elapsed: $((end_time-start_time))s"
cat "${OUTPUT_DIR}/REPORT.md"
