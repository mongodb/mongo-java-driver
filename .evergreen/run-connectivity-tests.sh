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
source "${BASH_SOURCE%/*}/javaConfig.bash"

echo "Running connectivity tests with Java ${JAVA_VERSION}"

./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.connectivity.uris="${MONGODB_URIS}" --info --continue \
 driver-sync:test --tests ConnectivityTest \
 driver-legacy:test --tests ConnectivityTest \
 driver-reactive-streams:test --tests ConnectivityTest
