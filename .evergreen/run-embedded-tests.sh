#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk6", "jdk7", "jdk8", "jdk9"

JDK=${JDK:-jdk}

############################################
#            Main Program                  #
############################################

# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk11"

export EMBEDDED_PATH="${PROJECT_DIRECTORY}/tmp/mongo-embedded-java"
mkdir -p ${EMBEDDED_PATH}

echo "Running tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=/opt/java/${JDK} --stacktrace --info :driver-embedded:test -Dorg.mongodb.test.embedded.path=${EMBEDDED_PATH}
