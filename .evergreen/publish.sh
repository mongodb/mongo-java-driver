#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

echo ${RING_FILE_GPG_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/secring.gpg

echo "Publishing snapshot with jdk8"

export JAVA_HOME="/opt/java/jdk8"
./gradlew -version
./gradlew -PnexusUsername=${NEXUS_USER} -PnexusPassword=${NEXUS_PWD} -Psigning.secretKeyRingFile=${PROJECT_DIRECTORY}/secring.gpg -Psigning.password=${SIGNING_PWD} -Psigning.keyId=${SIGNING_KEY_ID} uploadSnapshotArchives --info
