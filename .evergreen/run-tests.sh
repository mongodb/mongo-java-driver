#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#   AUTH                                 Set to enable authentication. Values are: "auth" / "noauth" (default)
#   SSL                                  Set to enable SSL. Values are "ssl" / "nossl" (default)
#   NETTY_SSL_PROVIDER                   The Netty TLS/SSL protocol provider. Ignored unless SSL is "ssl" and STREAM_TYPE is "netty". Values are "JDK", "OPENSSL", null (a.k.a. "" or '') (default).
#   MONGODB_URI                          Set the suggested connection MONGODB_URI (including credentials and topology info)
#   TOPOLOGY                             Allows you to modify variables and the MONGODB_URI based on test topology
#                                        Supported values: "server", "replica_set", "sharded_cluster"
#   COMPRESSOR                           Set to enable compression. Values are "snappy" and "zlib" (default is no compression)
#   STREAM_TYPE                          Set the stream type.  Values are "nio2" or "netty".  Defaults to "nio2".
#   JDK                                  Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#   SLOW_TESTS_ONLY                      Set to true to only run the slow tests
#   AWS_ACCESS_KEY_ID                    The AWS access key identifier for client-side encryption
#   AWS_SECRET_ACCESS_KEY                The AWS secret access key for client-side encryption
#   AWS_ACCESS_KEY_ID_AWS_KMS_NAMED      The AWS access key identifier for client-side encryption's named KMS provider.
#   AWS_SECRET_ACCESS_KEY_AWS_KMS_NAMED  The AWS secret access key for client-side encryption's named KMS provider.
#   AWS_TEMP_ACCESS_KEY_ID               The temporary AWS access key identifier for client-side encryption
#   AWS_TEMP_SECRET_ACCESS_KEY           The temporary AWS secret access key for client-side encryption
#   AWS_TEMP_SESSION_TOKEN               The temporary AWS session token for client-side encryption
#   AZURE_TENANT_ID                      The Azure tenant identifier for client-side encryption
#   AZURE_CLIENT_ID                      The Azure client identifier for client-side encryption
#   AZURE_CLIENT_SECRET                  The Azure client secret for client-side encryption
#   GCP_EMAIL                            The GCP email for client-side encryption
#   GCP_PRIVATE_KEY                      The GCP private key for client-side encryption
#   AZUREKMS_KEY_VAULT_ENDPOINT          The Azure key vault endpoint for integration tests
#   AZUREKMS_KEY_NAME                    The Azure key name endpoint for integration tests

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
TOPOLOGY=${TOPOLOGY:-server}
COMPRESSOR=${COMPRESSOR:-}
STREAM_TYPE=${STREAM_TYPE:-nio2}
TESTS=${TESTS:-test}
SLOW_TESTS_ONLY=${SLOW_TESTS_ONLY:-false}

export ASYNC_TYPE="-Dorg.mongodb.test.async.type=${STREAM_TYPE}"

if [ "${SSL}" = "ssl" ] && [ "${STREAM_TYPE}" = "netty" ] && [ "${NETTY_SSL_PROVIDER}" != "" ]; then
  readonly JAVA_SYSPROP_NETTY_SSL_PROVIDER="-Dorg.mongodb.test.netty.ssl.provider=${NETTY_SSL_PROVIDER}"
fi

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

provision_multi_mongos_uri_for_ssl () {
  # Arguments for auth + SSL
  if [ "$AUTH" != "noauth" ] || [ "$TOPOLOGY" == "replica_set" ]; then
    export MONGODB_URI="${MONGODB_URI}&ssl=true&sslInvalidHostNameAllowed=true"
    if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
        export MULTI_MONGOS_URI="${MULTI_MONGOS_URI}&ssl=true&sslInvalidHostNameAllowed=true"
    fi
  else
    export MONGODB_URI="${MONGODB_URI}/?ssl=true&sslInvalidHostNameAllowed=true"
    if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
        export MULTI_MONGOS_URI="${MULTI_MONGOS_URI}/?ssl=true&sslInvalidHostNameAllowed=true"
    fi
  fi
}

############################################
#            Main Program                  #
############################################

# Provision the correct connection string and set up SSL if needed
if [ "$TOPOLOGY" == "sharded_cluster" ]; then
    if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
        if [ "$AUTH" = "auth" ]; then
            export MULTI_MONGOS_URI="mongodb://bob:pwd123@localhost:27017,localhost:27018/?authSource=admin"
        else
            export MULTI_MONGOS_URI="${MONGODB_URI}"
        fi
    fi

     if [ "$AUTH" = "auth" ]; then
       export MONGODB_URI="mongodb://bob:pwd123@localhost:27017/?authSource=admin"
     else
       export MONGODB_URI="mongodb://localhost:27017"
     fi
fi

if [ "$COMPRESSOR" != "" ]; then
     if [[ "$MONGODB_URI" == *"?"* ]]; then
       export MONGODB_URI="${MONGODB_URI}&compressors=${COMPRESSOR}"
     else
       export MONGODB_URI="${MONGODB_URI}/?compressors=${COMPRESSOR}"
     fi

     if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
         if [[ "$MULTI_MONGOS_URI" == *"?"* ]]; then
             export MULTI_MONGOS_URI="${MULTI_MONGOS_URI}&compressors=${COMPRESSOR}"
         else
             export MULTI_MONGOS_URI="${MULTI_MONGOS_URI}/?compressors=${COMPRESSOR}"
         fi
     fi
fi

# Set up keystore/truststore regardless, as they are required for testing KMIP
provision_ssl

if [ "$SSL" != "nossl" ]; then
   provision_multi_mongos_uri_for_ssl
fi

if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
    export MULTI_MONGOS_URI_SYSTEM_PROPERTY="-Dorg.mongodb.test.multi.mongos.uri=${MULTI_MONGOS_URI}"
fi

# For now it's sufficient to hard-code the API version to "1", since it's the only API version
if [ ! -z "$REQUIRE_API_VERSION" ]; then
  export API_VERSION="-Dorg.mongodb.test.api.version=1"
fi

echo "Running $AUTH tests over $SSL for $TOPOLOGY and connecting to $MONGODB_URI"

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version

./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.uri=${MONGODB_URI} \
          ${MULTI_MONGOS_URI_SYSTEM_PROPERTY} ${API_VERSION} ${GRADLE_EXTRA_VARS} ${ASYNC_TYPE} \
          ${JAVA_SYSPROP_NETTY_SSL_PROVIDER} \
          -Dorg.mongodb.test.fle.on.demand.credential.test.failure.enabled=true \
          --stacktrace --info --continue ${TESTS}
