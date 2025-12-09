#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"
# Support arguments:
#       Pass as many MongoDB URIS as arguments to this script as required

############################################
#            Main Program                  #
############################################
JAVA_VERSION=8
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Running connectivity tests with Java ${JAVA_VERSION}"

MONGODB_URIS="${ATLAS_FREE}|${ATLAS_REPL}|${ATLAS_SHRD}|${ATLAS_TLS11}|${ATLAS_TLS12}|${ATLAS_SRV_FREE}|${ATLAS_SRV_REPL}|${ATLAS_SRV_SHRD}|${ATLAS_SRV_TLS11}|${ATLAS_SRV_TLS12}"

./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.connectivity.uris="${MONGODB_URIS}" --info --continue \
 driver-sync:test --tests ConnectivityTest \
 driver-legacy:test --tests ConnectivityTest \
 driver-reactive-streams:test --tests ConnectivityTest
