#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JAVA_HOME="/opt/java/jdk11"

############################################
#            Main Program                  #
############################################


echo "Running Reactive Streams TCK tests with ${JDK}"


./gradlew -version
./gradlew --stacktrace --info driver-reactive-streams:tckTest
                