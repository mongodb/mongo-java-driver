#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Running Atlas Data Lake driver tests"

# show test output
set -x

DATA_LAKE_URI="mongodb://mhuser:pencil@localhost"

echo "Running Atlas Data Lake tests with Java ${JAVA_VERSION}"
./gradlew -version
./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.data.lake=true -Dorg.mongodb.test.uri=${DATA_LAKE_URI} \
  --info driver-sync:test --tests *AtlasDataLake*Test
