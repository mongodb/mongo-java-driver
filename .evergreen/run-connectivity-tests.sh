#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"
# Support arguments:
#       Pass as many MongoDB URIS as arguments to this script as required

JDK=${JDK:-jdk}

############################################
#            Main Program                  #
############################################

echo "Running connectivity tests with ${JDK}"

export JAVA_HOME="/opt/java/jdk11"

./gradlew -version

for MONGODB_URI in $@; do
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info --rerun-tasks driver-sync:test --tests ConnectivityTest
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info --rerun-tasks driver-core:test --tests ConnectivityTest
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info --rerun-tasks driver-legacy:test --tests ConnectivityTest
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info --rerun-tasks driver-reactive-streams:test --tests ConnectivityTest
done
