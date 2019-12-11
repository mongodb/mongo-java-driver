#!/bin/bash

set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including username/password to use to connect to the server via PLAIN authentication mechanism
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"

JDK=${JDK:-jdk}

############################################
#            Main Program                  #
############################################

echo "Running MMAPv1 Storage Test"

echo "Compiling java driver with jdk11"

# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk11"

echo "Running tests with ${JDK}"
./gradlew -version
for PACKAGE in driver-sync driver-core ; do
    ./gradlew -PjdkHome=/opt/java/${JDK} --stacktrace --info \
              -Dorg.mongodb.test.uri=${MONGODB_URI} \
              ${PACKAGE}:test --tests RetryableWritesProseTest
done
