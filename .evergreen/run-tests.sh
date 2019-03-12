#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       SSL                     Set to enable SSL. Values are "ssl" / "nossl" (default)
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       TOPOLOGY                Allows you to modify variables and the MONGODB_URI based on test topology
#                               Supported values: "server", "replica_set", "sharded_cluster"
#       COMPRESSOR              Set to enable compression. Values are "snappy" and "zlib" (default is no compression)
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"
#       SLOW_TESTS_ONLY         Set to true to only run the slow tests
#       AWS_ACCESS_KEY_ID       The AWS access key identifier for client-side encryption
#       AWS_SECRET_ACCESS_KEY   The AWS secret access key for client-side encryption

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
JDK=${JDK:-jdk}
TOPOLOGY=${TOPOLOGY:-server}
COMPRESSOR=${COMPRESSOR:-}
SLOW_TESTS_ONLY=${SLOW_TESTS_ONLY:-false}

# JDK6 needs async.type=netty
if [ "$JDK" == "jdk6" ]; then
  export ASYNC_TYPE="-Dorg.mongodb.async.type=netty"
else
  export ASYNC_TYPE="-Dorg.mongodb.async.type=nio2"
fi

# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk9"


############################################
#            Functions                     #
############################################

provision_ssl () {
  echo "SSL !"

  # We generate the keystore and truststore on every run with the certs in the drivers-tools repo
  if [ ! -f client.pkc ]; then
    openssl pkcs12 -CAfile ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -export -in ${DRIVERS_TOOLS}/.evergreen/x509gen/client.pem -out client.pkc -password pass:bithere
  fi

  cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
  ${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt

  # We add extra gradle arguments for SSL
  export GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pssl.keyStoreType=pkcs12 -Pssl.keyStore=`pwd`/client.pkc -Pssl.keyStorePassword=bithere -Pssl.trustStoreType=jks -Pssl.trustStore=`pwd`/mongo-truststore -Pssl.trustStorePassword=changeit"
  # Arguments for auth + SSL
  if [ "$AUTH" != "noauth" ] || [ "$TOPOLOGY" == "replica_set" ]; then
    export MONGODB_URI="${MONGODB_URI}&ssl=true&sslInvalidHostNameAllowed=true"
    if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
        export TRANSACTION_URI="${TRANSACTION_URI}&ssl=true&sslInvalidHostNameAllowed=true"
    fi
  else
    export MONGODB_URI="${MONGODB_URI}/?ssl=true&sslInvalidHostNameAllowed=true"
    if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
        export TRANSACTION_URI="${TRANSACTION_URI}/?ssl=true&sslInvalidHostNameAllowed=true"
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
            export TRANSACTION_URI="mongodb://bob:pwd123@localhost:27017,localhost:27018/?authSource=admin"
        else
            export TRANSACTION_URI="${MONGODB_URI}"
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
         if [[ "$TRANSACTION_URI" == *"?"* ]]; then
             export TRANSACTION_URI="${TRANSACTION_URI}&compressors=${COMPRESSOR}"
         else
             export TRANSACTION_URI="${TRANSACTION_URI}/?compressors=${COMPRESSOR}"
         fi
     fi
fi

if [ "$SSL" != "nossl" ]; then
   provision_ssl
fi

if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
    export TRANSACTION_URI="-Dorg.mongodb.test.transaction.uri=${TRANSACTION_URI}"
fi

echo "Running $AUTH tests over $SSL for $TOPOLOGY and connecting to $MONGODB_URI"

echo "Running tests with ${JDK}"
./gradlew -version

if [ "$SLOW_TESTS_ONLY" == "true" ]; then
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} \
              ${TRANSACTION_URI} ${GRADLE_EXTRA_VARS} ${ASYNC_TYPE} --stacktrace --info testSlowOnly
else
    ./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} \
              -Dorg.mongodb.test.awsAccessKeyId=${AWS_ACCESS_KEY_ID} -Dorg.mongodb.test.awsSecretAccessKey=${AWS_SECRET_ACCESS_KEY} \
              ${TRANSACTION_URI} ${GRADLE_EXTRA_VARS} ${ASYNC_TYPE} --stacktrace --info test
fi
