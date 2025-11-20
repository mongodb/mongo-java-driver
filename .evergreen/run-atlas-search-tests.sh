#!/bin/bash

set -o errexit

# Supported/used environment variables:
# MONGODB_URI Set the connection to an Atlas cluster

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Running Atlas Search tests"

./gradlew -version
./gradlew --stacktrace --info \
    -Dorg.mongodb.test.atlas.search=true \
    -Dorg.mongodb.test.uri=${ATLAS_SEARCH_URI} \
    driver-core:test --tests AggregatesSearchIntegrationTest \
    --tests AggregatesBinaryVectorSearchIntegrationTest \
    --tests AggregatesSearchTest \
