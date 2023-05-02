#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI                 Set the suggested connection MONGODB_URI (including credentials and topology info)
#       JAVA_VERSION                Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#       AWS_ACCESS_KEY_ID           The AWS access key identifier for client-side encryption
#       AWS_SECRET_ACCESS_KEY       The AWS secret access key for client-side encryption
#       AWS_TEMP_ACCESS_KEY_ID      The temporary AWS access key identifier for client-side encryption
#       AWS_TEMP_SECRET_ACCESS_KEY  The temporary AWS secret access key for client-side encryption
#       AWS_TEMP_SESSION_TOKEN      The temporary AWS session token for client-side encryption
#       AZURE_TENANT_ID             The Azure tenant identifier for client-side encryption
#       AZURE_CLIENT_ID             The Azure client identifier for client-side encryption
#       AZURE_CLIENT_SECRET         The Azure client secret for client-side encryption
#       GCP_EMAIL                   The GCP email for client-side encryption
#       GCP_PRIVATE_KEY             The GCP private key for client-side encryption
#       AZUREKMS_KEY_VAULT_ENDPOINT The Azure key vault endpoint for integration tests
#       AZUREKMS_KEY_NAME           The Azure key name endpoint for integration tests

MONGODB_URI=${MONGODB_URI:-}

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

############################################
#            Functions                     #
############################################

provision_ssl () {
  # We generate the keystore and truststore on every run with the certs in the drivers-tools repo
  if [ ! -f client.pkc ]; then
    openssl pkcs12 -CAfile ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -export -in ${DRIVERS_TOOLS}/.evergreen/x509gen/client.pem -out client.pkc -password pass:bithere
  fi

  cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
  ${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt

  # We add extra gradle arguments for SSL
  export GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pssl.keyStoreType=pkcs12 -Pssl.keyStore=`pwd`/client.pkc -Pssl.keyStorePassword=bithere -Pssl.trustStoreType=jks -Pssl.trustStore=`pwd`/mongo-truststore -Pssl.trustStorePassword=changeit"
}

############################################
#            Main Program                  #
############################################

# Set up keystore/truststore regardless, as they are required for testing KMIP
provision_ssl

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version

# By not specifying the path to the `crypt_shared` via the `org.mongodb.test.crypt.shared.lib.path` Java system property,
# we force the driver to start `mongocryptd` instead of loading and using `crypt_shared`.
./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.uri=${MONGODB_URI} \
      -Dorg.mongodb.test.fle.on.demand.credential.test.failure.enabled="true" \
      -Dorg.mongodb.test.fle.on.demand.credential.test.azure.keyVaultEndpoint="${AZUREKMS_KEY_VAULT_ENDPOINT}" \
      -Dorg.mongodb.test.fle.on.demand.credential.test.azure.keyName="${AZUREKMS_KEY_NAME}" \
      -Dorg.mongodb.test.awsAccessKeyId=${AWS_ACCESS_KEY_ID} -Dorg.mongodb.test.awsSecretAccessKey=${AWS_SECRET_ACCESS_KEY} \
      -Dorg.mongodb.test.tmpAwsAccessKeyId=${AWS_TEMP_ACCESS_KEY_ID} -Dorg.mongodb.test.tmpAwsSecretAccessKey=${AWS_TEMP_SECRET_ACCESS_KEY} -Dorg.mongodb.test.tmpAwsSessionToken=${AWS_TEMP_SESSION_TOKEN} \
      -Dorg.mongodb.test.azureTenantId=${AZURE_TENANT_ID} -Dorg.mongodb.test.azureClientId=${AZURE_CLIENT_ID} -Dorg.mongodb.test.azureClientSecret=${AZURE_CLIENT_SECRET} \
      -Dorg.mongodb.test.gcpEmail=${GCP_EMAIL} -Dorg.mongodb.test.gcpPrivateKey=${GCP_PRIVATE_KEY} \
      ${GRADLE_EXTRA_VARS} \
      --stacktrace --info --continue \
      driver-legacy:test \
          --tests "*.ClientEncryption*" --tests "*.ClientSideEncryption*" \
      driver-sync:test \
          --tests "*.ClientEncryption*" --tests "*.ClientSideEncryption*" \
      driver-reactive-streams:test \
          --tests "*.ClientEncryption*" --tests "*.ClientSideEncryption*" \
      driver-scala:integrationTest \
          --tests "*.ClientEncryption*" --tests "*.ClientSideEncryption*"
