#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JAVA_VERSION               Set the version of java to be used
#       SERVERLESS_URI             The URI, without credentials
#       SERVERLESS_ATLAS_USER
#       SERVERLESS_ATLAS_PASSWORD
# Support arguments:
#       Pass as many MongoDB URIS as arguments to this script as required

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Running serverless tests with Java ${JAVA_VERSION}"

# Assume "mongodb+srv" protocol
MONGODB_URI="mongodb+srv://${SERVERLESS_ATLAS_USER}:${SERVERLESS_ATLAS_PASSWORD}@${SERVERLESS_URI:14}"

./gradlew -version

./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.uri=${MONGODB_URI} \
   -Dorg.mongodb.test.serverless=true --stacktrace --info --continue driver-sync:test
