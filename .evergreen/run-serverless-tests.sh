#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                       Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#       MONGODB_URI               The URI, without credentials
#       SERVERLESS_ATLAS_USER
#       SERVERLESS_ATLAS_PASSWORD
# Support arguments:
#       Pass as many MongoDB URIS as arguments to this script as required

JDK=${JDK:-jdk8}

############################################
#            Main Program                  #
############################################

echo "Running serverless tests with ${JDK}"

export JAVA_HOME="/opt/java/jdk11"

MONGODB_URI_SINGLE_HOST=${MONGODB_URI%%,*}
SINGLE_HOST=${MONGODB_URI_SINGLE_HOST:10}

MONGODB_URI_FINAL="mongodb://${SERVERLESS_ATLAS_USER}:${SERVERLESS_ATLAS_PASSWORD}@${SINGLE_HOST}/?tls=true"

echo ${MONGODB_URI_FINAL}

./gradlew -version

./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI_FINAL} -Dorg.mongodb.test.serverless=true \
   --stacktrace --info --continue driver-sync:test
