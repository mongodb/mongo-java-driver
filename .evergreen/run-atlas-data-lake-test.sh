#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

JDK=${JDK:-jdk11}
readonly JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER="-Dorg.mongodb.driver.connection.debugger=LOG_AND_THROW"

############################################
#            Main Program                  #
############################################

echo "Running Atlas Data Lake driver tests"

export JAVA_HOME="/opt/java/${JDK}"

# show test output
set -x

DATA_LAKE_URI="mongodb://mhuser:pencil@localhost"

echo "Running Atlas Data Lake tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=${JAVA_HOME} -Dorg.mongodb.test.data.lake=true -Dorg.mongodb.test.uri=${DATA_LAKE_URI} \
  ${JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER} \
  --info driver-sync:test --tests AtlasDataLake*Test
