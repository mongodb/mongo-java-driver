#!/bin/bash

set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including username/password to use to connect to the server via PLAIN authentication mechanism
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Running MMAPv1 Storage Test"

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version
for PACKAGE in driver-sync driver-reactive-streams ; do
    ./gradlew -PjavaVersion=${JAVA_VERSION} --stacktrace --info \
              -Dorg.mongodb.test.uri=${MONGODB_URI} \
              ${PACKAGE}:test --tests RetryableWritesProseTest
done
