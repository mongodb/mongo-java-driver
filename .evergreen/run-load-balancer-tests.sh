#!/bin/bash

set -o xtrace  # Write all commands first to stderr
set -o errexit # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                        Set to enable authentication. Values are: "auth" / "noauth" (default)
#       SSL                         Set to enable SSL. Values are "ssl" / "nossl" (default)
#       JDK                         Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#       SINGLE_MONGOS_LB_URI        Set the URI pointing to a load balancer configured with a single mongos server
#       MULTI_MONGOS_LB_URI         Set the URI pointing to a load balancer configured with multiple mongos servers

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}

############################################
#            Main Program                  #
############################################
source "${BASH_SOURCE%/*}/javaConfig.bash"

if [ "$SSL" != "nossl" ]; then
  # We generate the keystore and truststore on every run with the certs in the drivers-tools repo
  if [ ! -f client.pkc ]; then
    openssl pkcs12 -CAfile ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -export -in ${DRIVERS_TOOLS}/.evergreen/x509gen/client.pem -out client.pkc -password pass:bithere
  fi

  cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
  ${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt

  # We add extra gradle arguments for SSL
  GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pssl.keyStoreType=pkcs12 -Pssl.keyStore=$(pwd)/client.pkc -Pssl.keyStorePassword=bithere -Pssl.trustStoreType=jks -Pssl.trustStore=$(pwd)/mongo-truststore -Pssl.trustStorePassword=changeit"
  SINGLE_MONGOS_LB_URI="${SINGLE_MONGOS_LB_URI}&ssl=true&sslInvalidHostNameAllowed=true"
  MULTI_MONGOS_LB_URI="${MULTI_MONGOS_LB_URI}&ssl=true&sslInvalidHostNameAllowed=true"
fi

echo "Running $AUTH tests over $SSL and connecting to $SINGLE_MONGOS_LB_URI"

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version

# Disabling errexit so that both gradle command will run.
# Then we exit with non-zero if either of them exited with non-zero

set +o errexit

./gradlew -PjavaVersion=${JAVA_VERSION} \
  -Dorg.mongodb.test.uri=${SINGLE_MONGOS_LB_URI} \
  -Dorg.mongodb.test.multi.mongos.uri=${MULTI_MONGOS_LB_URI} \
  ${GRADLE_EXTRA_VARS} --stacktrace --info --continue driver-sync:test \
  --tests LoadBalancerTest \
  --tests RetryableReadsTest \
  --tests RetryableWritesTest \
  --tests VersionedApiTest \
  --tests ChangeStreamsTest \
  --tests UnifiedCrudTest \
  --tests UnifiedTransactionsTest \
  --tests InitialDnsSeedlistDiscoveryTest
first=$?
echo $first

./gradlew -PjavaVersion=${JAVA_VERSION} \
  -Dorg.mongodb.test.uri=${SINGLE_MONGOS_LB_URI} \
  -Dorg.mongodb.test.multi.mongos.uri=${MULTI_MONGOS_LB_URI} \
  ${GRADLE_EXTRA_VARS} --stacktrace --info --continue driver-reactive-stream:test \
  --tests LoadBalancerTest \
  --tests RetryableReadsTest \
  --tests RetryableWritesTest \
  --tests VersionedApiTest \
  --tests ChangeStreamsTest \
  --tests UnifiedCrudTest \
  --tests UnifiedTransactionsTest \
  --tests InitialDnsSeedlistDiscoveryTest
second=$?
echo $second

./gradlew -PjavaVersion=${JAVA_VERSION} \
  -Dorg.mongodb.test.uri=${SINGLE_MONGOS_LB_URI} \
  -Dorg.mongodb.test.multi.mongos.uri=${MULTI_MONGOS_LB_URI} \
  ${GRADLE_EXTRA_VARS} --stacktrace --info --continue driver-core:test \
  --tests QueryBatchCursorFunctionalSpecification
third=$?
echo $third

if [ $first -ne 0 ]; then
   exit $first
elif [ $second -ne 0 ]; then
   exit $second
elif [ $third -ne 0 ]; then
   exit $third
else
   exit 0
fi
