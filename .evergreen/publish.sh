#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
RELEASE=${RELEASE:false}

export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signingKey="${SIGNING_KEY}"
export ORG_GRADLE_PROJECT_signingPassword=${SIGNING_PASSWORD}

echo "Publishing snapshot with jdk11"
export JAVA_HOME="/opt/java/jdk11"

if [ "$RELEASE" == "true" ]; then
  TASK="publishArchives"
else
  TASK="publishSnapshots"
fi

SYSTEM_PROPERTIES="-Dorg.gradle.internal.publish.checksums.insecure=true -Dorg.gradle.internal.http.connectionTimeout=120000 -Dorg.gradle.internal.http.socketTimeout=120000"

./gradlew -version
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info  ${TASK}
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info :bson-scala:${TASK} :driver-scala:${TASK} -PdefaultScalaVersions=2.11.12,2.12.10
