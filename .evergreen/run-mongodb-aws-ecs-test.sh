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

apt update

if ! which java ; then
    echo "Installing java..."
    # Ubuntu 18.04 ca-certificates-java and opendjdk-17 bug work around
    dpkg --purge --force-depends ca-certificates-java
    apt install ca-certificates-java -y
    apt install openjdk-17-jdk -y
fi

if ! which git ; then
    echo "installing git..."
    apt install git -y
fi

cd src

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

./gradlew -version

echo "Running tests..."
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} -Dorg.mongodb.test.aws.credential.provider=awsSdkV2 --stacktrace --debug --info \
           driver-core:test --tests AwsAuthenticationSpecification
first=$?
echo $first

./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} -Dorg.mongodb.test.aws.credential.provider=awsSdkV1 --stacktrace --debug --info \
           driver-core:test --tests AwsAuthenticationSpecification
second=$?
echo $second

./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} -Dorg.mongodb.test.aws.credential.provider=builtIn --stacktrace --debug --info \
           driver-core:test --tests AwsAuthenticationSpecification
third=$?
echo $third

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
elif [ $third -ne 0 ]; then
   exit $third
else
   exit 0
fi


cd -
