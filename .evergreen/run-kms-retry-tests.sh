#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI                 Set the suggested connection MONGODB_URI (including credentials and topology info)

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"
echo "Running KMS Retry tests"

cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt

export GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pssl.trustStoreType=jks -Pssl.trustStore=`pwd`/mongo-truststore -Pssl.trustStorePassword=changeit"

./gradlew -version

# Disable errexit so both suites run and their exit codes can be captured below.
set +o errexit

./gradlew --stacktrace --info ${GRADLE_EXTRA_VARS} -Dorg.mongodb.test.uri=${MONGODB_URI} \
    -Dorg.mongodb.test.kms.retry.ca.path="${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem" \
    driver-sync:cleanTest driver-sync:test --tests ClientSideEncryptionKmsRetryProseTest
first=$?
echo $first

./gradlew --stacktrace --info ${GRADLE_EXTRA_VARS} -Dorg.mongodb.test.uri=${MONGODB_URI} \
    -Dorg.mongodb.test.kms.retry.ca.path="${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem" \
    driver-reactive-streams:cleanTest driver-reactive-streams:test --tests ClientSideEncryptionKmsRetryProseTest
second=$?
echo $second

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
else
   exit 0
fi
