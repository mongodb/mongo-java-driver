#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_ATLAS_SEARCH_URI                 Set the connection to an Atlas cluster

############################################
#            Main Program                  #
############################################

echo "Running Atlas search tests"

export JAVA_HOME="/opt/java/jdk11"

./gradlew -version

./gradlew --stacktrace --info -Dorg.mongodb.test.search.uri=${MONGODB_ATLAS_SEARCH_URI} driver-core:test --tests SearchOperatorsIntegrationTest
