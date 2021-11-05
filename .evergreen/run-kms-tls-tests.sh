#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI                 Set the suggested connection MONGODB_URI (including credentials and topology info)
#       KMS_TLS_ERROR_TYPE          Either "expired" or "invalidHostname"

############################################
#            Main Program                  #
############################################
source "${BASH_SOURCE%/*}/javaConfig.bash"
echo "Running KMS TLS tests"

cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt

export GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pssl.trustStoreType=jks -Pssl.trustStore=`pwd`/mongo-truststore -Pssl.trustStorePassword=changeit"

./gradlew -version

./gradlew --stacktrace --info ${GRADLE_EXTRA_VARS} -Dorg.mongodb.test.uri=${MONGODB_URI} \
    -Dorg.mongodb.test.kms.tls.error.type=${KMS_TLS_ERROR_TYPE} \
    --no-build-cache driver-sync:cleanTest driver-sync:test --tests ClientSideEncryptionKmsTlsTest
first=$?
echo $first

./gradlew --stacktrace --info ${GRADLE_EXTRA_VARS} -Dorg.mongodb.test.uri=${MONGODB_URI} \
    -Dorg.mongodb.test.kms.tls.error.type=${KMS_TLS_ERROR_TYPE} \
    --no-build-cache driver-reactive-streams:cleanTest driver-reactive-streams:test --tests ClientSideEncryptionKmsTlsTest
second=$?
echo $second

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
else
   exit 0
fi
