#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI                 Set the URI, including an optional username/password to use to connect to the server
#       PROVIDER                    Which KMS provider to test (either "gcp" or "azure")
#       AZUREKMS_KEY_VAULT_ENDPOINT The Azure key vault endpoint for Azure integration tests
#       AZUREKMS_KEY_NAME           The Azure key name endpoint for Azure integration tests

############################################
#            Main Program                  #
############################################

echo "Running ${PROVIDER}} Credential Acquisition Test"

if ! which java ; then
    echo "Installing java..."
    sudo apt install openjdk-17-jdk -y
fi

export PROVIDER=${PROVIDER}

echo "Running gradle version"
./gradlew -version

echo "Running gradle classes compile for driver-sync and driver-reactive-streams"
./gradlew --parallel --build-cache --stacktrace --info  \
  driver-sync:classes driver-reactive-streams:classes

echo "Running driver-sync tests"
./gradlew -Dorg.mongodb.test.uri="${MONGODB_URI}" \
 -Dorg.mongodb.test.fle.on.demand.credential.test.success.enabled=true \
 --build-cache--stacktrace --info  driver-sync:test --tests ClientSideEncryptionOnDemandCredentialsTest
first=$?
echo $first

echo "Running driver-reactive-streams tests"
./gradlew -Dorg.mongodb.test.uri="${MONGODB_URI}" \
 -Dorg.mongodb.test.fle.on.demand.credential.test.success.enabled=true \
 --build-cache --stacktrace --info  driver-reactive-streams:test --tests ClientSideEncryptionOnDemandCredentialsTest
second=$?
echo $second

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
else
   exit 0
fi
