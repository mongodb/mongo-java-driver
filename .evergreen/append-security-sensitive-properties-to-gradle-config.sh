#!/bin/bash

# Append to gradle.properties so that they are not echoed below on the gradlew command line
cat <<EOF >> ./gradle.properties
systemProp.org.mongodb.test.fle.on.demand.credential.test.failure.enabled=true
systemProp.org.mongodb.test.fle.on.demand.credential.test.azure.keyVaultEndpoint=${AZUREKMS_KEY_VAULT_ENDPOINT}
systemProp.org.mongodb.test.fle.on.demand.credential.test.azure.keyName=${AZUREKMS_KEY_NAME}
systemProp.org.mongodb.test.awsAccessKeyId=${AWS_ACCESS_KEY_ID}
systemProp.org.mongodb.test.awsSecretAccessKey=${AWS_SECRET_ACCESS_KEY}
systemProp.org.mongodb.test.tmpAwsAccessKeyId=${AWS_TEMP_ACCESS_KEY_ID}
systemProp.org.mongodb.test.tmpAwsSecretAccessKey=${AWS_TEMP_SECRET_ACCESS_KEY}
systemProp.org.mongodb.test.tmpAwsSessionToken=${AWS_TEMP_SESSION_TOKEN}
systemProp.org.mongodb.test.azureTenantId=${AZURE_TENANT_ID}
systemProp.org.mongodb.test.azureClientId=${AZURE_CLIENT_ID}
systemProp.org.mongodb.test.azureClientSecret=${AZURE_CLIENT_SECRET}
systemProp.org.mongodb.test.gcpEmail=${GCP_EMAIL}
systemProp.org.mongodb.test.gcpPrivateKey=${GCP_PRIVATE_KEY}
systemProp.org.mongodb.test.crypt.shared.lib.path=${CRYPT_SHARED_LIB_PATH}
EOF
