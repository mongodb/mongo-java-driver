#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including username/password to use to connect to the server via PLAIN authentication mechanism
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8"

JDK=${JDK:-jdk}
JAVA_HOME="/opt/java/${JDK}"

############################################
#            Main Program                  #
############################################

echo "Running PLAIN authentication tests"

echo "Compiling java driver with jdk8"
# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk8"
./gradlew -version
./gradlew --info classes testClasses

echo "Running tests with ${JDK}"
JAVA_HOME="/opt/java/${JDK}"
./gradlew -version
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info -Dtest.single=PlainAuthenticationSpecification -x classes -x testClasses --rerun-tasks driver-core:test
