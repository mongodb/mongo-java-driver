#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including an optional username/password to use to connect to the server via MONGODB-AWS
#                               authentication mechanism
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
MONGODB_URI="$1"

echo "Running MONGODB-AWS ECS authentication tests"

if ! which java ; then
    echo "Installing java..."
    apt install openjdk-11-jdk -y
fi

if ! which git ; then
    echo "installing git..."
    apt install git -y
fi

cd src
./gradlew -version

echo "Running tests..."
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --debug --info driver-core:test --tests AwsAuthenticationSpecification
cd -
