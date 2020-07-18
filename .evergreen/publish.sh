#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
RELEASE=${RELEASE:false}

echo ${RING_FILE_GPG_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/secring.gpg

trap "rm ${PROJECT_DIRECTORY}/secring.gpg; exit" EXIT HUP

git status

export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signing_keyId=${SIGNING_KEY_ID}
export ORG_GRADLE_PROJECT_signing_password=${SIGNING_PASSWORD}
export ORG_GRADLE_PROJECT_signing_secretKeyRingFile=${PROJECT_DIRECTORY}/secring.gpg

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
