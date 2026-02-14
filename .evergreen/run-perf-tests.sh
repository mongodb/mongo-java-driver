#!/bin/bash

set -o xtrace
set -o errexit

BENCHMARKING_DATA="${PROJECT_DIRECTORY}/testing/resources/specifications/source/benchmarking/data"

cd "${BENCHMARKING_DATA}"
tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz
cd "${PROJECT_DIRECTORY}"

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

export TEST_PATH="${BENCHMARKING_DATA}/"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

if [ "${PROVIDER}" = "Netty" ]; then
    TASK="driver-benchmarks:runNetty"
elif [ "${PROVIDER}" = "Pojo" ]; then
    TASK="driver-benchmarks:runPojo"
else
    TASK="driver-benchmarks:run"
fi

start_time=$(date +%s)
./gradlew -Dorg.mongodb.benchmarks.data=${TEST_PATH} -Dorg.mongodb.benchmarks.output=${OUTPUT_FILE} ${TASK}
end_time=$(date +%s)
elapsed_secs=$((end_time-start_time))

