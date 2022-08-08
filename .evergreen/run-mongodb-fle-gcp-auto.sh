#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including an optional username/password to use to connect to the server via MONGODB-AWS
#                               authentication mechanism
#       SUCCESS                 Whether the authentication is expected to succeed or fail.  One of "true" or "false"
############################################
#            Main Program                  #
############################################

echo "Running GCP Credential Acquisition Test"

if ! which java ; then
    echo "Installing java..."
    sudo apt install openjdk-17-jdk -y
fi

./gradlew -Dorg.mongodb.test.uri="${MONGODB_URI}" -Dorg.mongodb.test.gcp.success="${SUCCESS}" --stacktrace --debug --info driver-sync:test \
  --tests ClientSideEncryptionOnDemandGcpCredentialsTest
first=$?
echo $first

./gradlew -Dorg.mongodb.test.uri="${MONGODB_URI}" -Dorg.mongodb.test.gcp.success="${SUCCESS}" --stacktrace --debug --info driver-reactive-streams:test \
  --tests ClientSideEncryptionOnDemandGcpCredentialsTest
second=$?
echo $second

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
else
   exit 0
fi
