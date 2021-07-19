#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JAVA_HOME="/opt/java/jdk11"
readonly JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER="-Dorg.mongodb.driver.connection.debugger=LOG_AND_THROW"

############################################
#            Main Program                  #
############################################


echo "Running Reactive Streams TCK tests with ${JDK}"


./gradlew -version
./gradlew ${JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER} --stacktrace --info driver-reactive-streams:tckTest
