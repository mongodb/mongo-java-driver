#!/bin/bash

export org_mongodb_test_fle_on_demand_credential_test_azure_keyVaultEndpoint=${AZUREKMS_KEY_VAULT_ENDPOINT}
export org_mongodb_test_fle_on_demand_credential_test_azure_keyName=${AZUREKMS_KEY_NAME}
export org_mongodb_test_fle_on_demand_credential_provider="${PROVIDER}"
export org_mongodb_test_awsAccessKeyId=${AWS_ACCESS_KEY_ID}
export org_mongodb_test_awsSecretAccessKey=${AWS_SECRET_ACCESS_KEY}
export org_mongodb_test_tmpAwsAccessKeyId=${AWS_TEMP_ACCESS_KEY_ID}
export org_mongodb_test_tmpAwsSecretAccessKey=${AWS_TEMP_SECRET_ACCESS_KEY}
export org_mongodb_test_tmpAwsSessionToken=${AWS_TEMP_SESSION_TOKEN}
export org_mongodb_test_azureTenantId=${AZURE_TENANT_ID}
export org_mongodb_test_azureClientId=${AZURE_CLIENT_ID}
export org_mongodb_test_azureClientSecret=${AZURE_CLIENT_SECRET}
export org_mongodb_test_gcpEmail=${GCP_EMAIL}
export org_mongodb_test_gcpPrivateKey=${GCP_PRIVATE_KEY}

export org_mongodb_test_crypt_shared_lib_path=${CRYPT_SHARED_LIB_PATH}
