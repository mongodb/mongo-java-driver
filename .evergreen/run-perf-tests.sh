#!/bin/bash

set -o xtrace
set -o errexit

rm -rf driver-performance-test-data
git clone https://github.com/mongodb-labs/driver-performance-test-data.git
cd driver-performance-test-data
tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz
cd ..

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

export TEST_PATH="${PROJECT_DIRECTORY}/driver-performance-test-data/"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

if [ "${PROVIDER}" = "Netty" ]; then
    TASK="driver-benchmarks:runNetty"
else
    TASK="driver-benchmarks:run"
fi

start_time=$(date +%s)
./gradlew -Dorg.mongodb.benchmarks.data=${TEST_PATH} -Dorg.mongodb.benchmarks.output=${OUTPUT_FILE} ${TASK}
end_time=$(date +%s)
elapsed_secs=$((end_time-start_time))

