#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

echo ${RING_FILE_GPG_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/secring.gpg

trap "rm ${PROJECT_DIRECTORY}/secring.gpg; exit" EXIT HUP



export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signing_keyId=${SIGNING_KEY_ID}
export ORG_GRADLE_PROJECT_signing_password=${SIGNING_PASSWORD}
export ORG_GRADLE_PROJECT_signing_secretKeyRingFile=${PROJECT_DIRECTORY}/secring.gpg

SDK_ROOT=$PWD/android_sdk
if [ ! -e  $SDK_ROOT ]; then
    echo "Installing ANDROID SDK"
    mkdir $SDK_ROOT
    (
        cd $SDK_ROOT
        export JAVA_HOME="/opt/java/jdk8"
        SDK_PACKAGE=sdk-tools-linux-4333796.zip
        curl -O https://dl.google.com/android/repository/$SDK_PACKAGE
        unzip $SDK_PACKAGE
        yes | ./tools/bin/sdkmanager --channel=0 \
            "platforms;android-28"  \
            "build-tools;28.0.0"
    )
fi
export ANDROID_HOME=${SDK_ROOT}

echo "Publishing snapshot with jdk9"
export JAVA_HOME="/opt/java/jdk9"

./gradlew -version
./gradlew publishSnapshots
