#!/bin/bash

# Disabling errexit so that all gradle command will run.
# Then we exit with non-zero if any of them exited with non-zero
set +o errexit

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"
# Support arguments:
#       Pass as many MongoDB URIS as arguments to this script as required

JDK=${JDK:-jdk8}

############################################
#            Main Program                  #
############################################

echo "Running connectivity tests with ${JDK}"

export JAVA_HOME="/opt/java/jdk11"

./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.connectivity.uris=${MONGODB_URIS} --info \
 driver-sync:test --tests ConnectivityTest \
 driver-legacy:test --tests ConnectivityTest \
 driver-reactive-streams:test --tests ConnectivityTest
