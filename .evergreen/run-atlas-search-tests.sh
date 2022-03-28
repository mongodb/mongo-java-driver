#!/bin/bash

set -o errexit

# Supported/used environment variables:
# MONGODB_URI Set the connection to an Atlas cluster

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Running Atlas Search tests"
./gradlew -version
#./gradlew --stacktrace --info \
#    -Dorg.mongodb.test.atlas.search=true \
#    -Dorg.mongodb.test.uri=${MONGODB_URI} \
#    driver-core:test --tests AggregatesSearchIntegrationTest
