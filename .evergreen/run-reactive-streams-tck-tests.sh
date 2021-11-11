#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
source "${BASH_SOURCE%/*}/javaConfig.bash"

echo "Running Reactive Streams TCK tests with Java ${JAVA_VERSION}"

./gradlew -version
./gradlew --stacktrace --info driver-reactive-streams:tckTest
