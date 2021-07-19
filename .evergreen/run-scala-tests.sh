#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
JDK=${JDK:-jdk11}
TOPOLOGY=${TOPOLOGY:-standalone}
SAFE_FOR_MULTI_MONGOS=${SAFE_FOR_MULTI_MONGOS:-}
readonly JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER="-Dorg.mongodb.driver.connection.debugger=LOG_AND_THROW"

export JAVA_HOME="/opt/java/${JDK}"

############################################
#            Main Program                  #
############################################

if [ "$SSL" != "nossl" ]; then
  echo -e "\nSSL support not configured for Scala tests"
  exit 1
fi

if [ "$AUTH" != "noauth" ]; then
  echo -e "\nAuth support not configured for Scala tests"
  exit 1
fi

if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
    export TRANSACTION_URI="-Dorg.mongodb.test.transaction.uri=${MONGODB_URI}"
fi

echo "Running scala tests with Scala $SCALA"

./gradlew -version
./gradlew -PscalaVersion=$SCALA \
  ${JAVA_SYSPROP_MONGODB_DRIVER_DEBUGGER} \
  --stacktrace --info :bson-scala:test :driver-scala:test :driver-scala:integrationTest -Dorg.mongodb.test.uri=${MONGODB_URI} ${TRANSACTION_URI}
