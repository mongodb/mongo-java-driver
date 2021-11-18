#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
TOPOLOGY=${TOPOLOGY:-standalone}
SAFE_FOR_MULTI_MONGOS=${SAFE_FOR_MULTI_MONGOS:-}

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"


if [ "$SSL" != "nossl" ]; then
  echo -e "\nSSL support not configured for Scala tests"
  exit 1
fi

if [ "$AUTH" != "noauth" ]; then
  echo -e "\nAuth support not configured for Scala tests"
  exit 1
fi

if [ "$SAFE_FOR_MULTI_MONGOS" == "true" ]; then
    export MULTI_MONGOS_URI_SYSTEM_PROPERTY="-Dorg.mongodb.test.multi.mongos.uri=${MONGODB_URI}"
fi

echo "Running scala tests with Scala $SCALA"

./gradlew -version
./gradlew -PscalaVersion=$SCALA --stacktrace --info :bson-scala:test :driver-scala:test :driver-scala:integrationTest -Dorg.mongodb.test.uri=${MONGODB_URI} ${MULTI_MONGOS_URI_SYSTEM_PROPERTY}
