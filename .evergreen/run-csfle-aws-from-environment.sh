#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI                 Set the suggested connection MONGODB_URI (including credentials and topology info)
#       AWS_ACCESS_KEY_ID           The AWS access key identifier for client-side encryption
#       AWS_SECRET_ACCESS_KEY       The AWS secret access key for client-side encryption

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"
echo "Running CSFLE AWS from environment tests"

./gradlew -version

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

./gradlew --stacktrace --info -Dorg.mongodb.test.uri=${MONGODB_URI} \
    --no-build-cache driver-sync:cleanTest driver-sync:test --tests ClientSideEncryptionAwsCredentialFromEnvironmentTest
first=$?
echo $first

./gradlew --stacktrace --info  -Dorg.mongodb.test.uri=${MONGODB_URI} \
    --no-build-cache driver-reactive-streams:cleanTest driver-reactive-streams:test --tests ClientSideEncryptionAwsCredentialFromEnvironmentTest
second=$?
echo $second

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
else
   exit 0
fi
