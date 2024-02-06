#!/bin/bash

export org.mongodb.test.fle.on.demand.credential.test.azure.keyVaultEndpoint=${AZUREKMS_KEY_VAULT_ENDPOINT}
export org.mongodb.test.fle.on.demand.credential.test.azure.keyName=${AZUREKMS_KEY_NAME}
export org.mongodb.test.awsAccessKeyId=${AWS_ACCESS_KEY_ID}
export org.mongodb.test.awsSecretAccessKey=${AWS_SECRET_ACCESS_KEY}
export org.mongodb.test.tmpAwsAccessKeyId=${AWS_TEMP_ACCESS_KEY_ID}
export org.mongodb.test.tmpAwsSecretAccessKey=${AWS_TEMP_SECRET_ACCESS_KEY}
export org.mongodb.test.tmpAwsSessionToken=${AWS_TEMP_SESSION_TOKEN}
export org.mongodb.test.azureTenantId=${AZURE_TENANT_ID}
export org.mongodb.test.azureClientId=${AZURE_CLIENT_ID}
export org.mongodb.test.azureClientSecret=${AZURE_CLIENT_SECRET}
export org.mongodb.test.gcpEmail=${GCP_EMAIL}
export org.mongodb.test.gcpPrivateKey=${GCP_PRIVATE_KEY}
export org.mongodb.test.crypt.shared.lib.path=${CRYPT_SHARED_LIB_PATH}
