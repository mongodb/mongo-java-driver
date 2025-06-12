# Java configurations for evergreen

export JDK8="/opt/java/jdk8"
export JDK11="/opt/java/jdk11"
export JDK17="/opt/java/jdk17"
export JDK21="/opt/java/jdk21"
# note that `JDK21_GRAALVM` is used in `run-graalvm-native-image-app.sh`
# by dynamically constructing the variable name
export JDK21_GRAALVM="/opt/java/jdk21-graalce"

if [ -d "$JDK17" ]; then
  export JAVA_HOME=$JDK17
fi

export JAVA_VERSION=${JAVA_VERSION:-17}

echo "Java Configs:"
echo "Java Home: ${JAVA_HOME}"
echo "Java test version: ${JAVA_VERSION}"

# Rename environment variables for AWS, Azure, and GCP
if [ -f secrets-export.sh ]; then
  echo "Renaming secrets env variables"
  . secrets-export.sh

  export AWS_ACCESS_KEY_ID=$FLE_AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY=$FLE_AWS_SECRET_ACCESS_KEY
  export AWS_DEFAULT_REGION=$FLE_AWS_DEFAULT_REGION

  export AWS_ACCESS_KEY_ID_AWS_KMS_NAMED=$FLE_AWS_KEY2
  export AWS_SECRET_ACCESS_KEY_AWS_KMS_NAMED=$FLE_AWS_SECRET2

  export AWS_TEMP_ACCESS_KEY_ID=$CSFLE_AWS_TEMP_ACCESS_KEY_ID
  export AWS_TEMP_SECRET_ACCESS_KEY=$CSFLE_AWS_TEMP_SECRET_ACCESS_KEY
  export AWS_TEMP_SESSION_TOKEN=$CSFLE_AWS_TEMP_SESSION_TOKEN

  export AZURE_CLIENT_ID=$FLE_AZURE_CLIENTID
  export AZURE_TENANT_ID=$FLE_AZURE_TENANTID
  export AZURE_CLIENT_SECRET=$FLE_AZURE_CLIENTSECRET

  export GCP_EMAIL=$FLE_GCP_EMAIL
  export GCP_PRIVATE_KEY=$FLE_GCP_PRIVATEKEY

  # Unset AWS_SESSION_TOKEN if it is empty
  if [ -z "$AWS_SESSION_TOKEN" ];then
    unset AWS_SESSION_TOKEN
  fi

else
  echo "No secrets env variables found to rename"
fi
