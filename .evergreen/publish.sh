#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

echo ${RING_FILE_GPG_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/secring.gpg

echo "Publishing snapshot with jdk8"

export JAVA_HOME="/opt/java/jdk8"

export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signing_keyId=${SIGNING_KEY_ID}
export ORG_GRADLE_PROJECT_signing_password=${SIGNING_PASSWORD}
export ORG_GRADLE_PROJECT_signing_secretKeyRingFile=${PROJECT_DIRECTORY}/secring.gpg

./gradlew -version
./gradlew uploadSnapshotArchives
