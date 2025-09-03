#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
TOPOLOGY=${TOPOLOGY:-standalone}

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"


if [ "$SSL" != "nossl" ]; then
  echo -e "\nSSL support not configured for Scala tests"
  exit 1
fi

if [ "$AUTH" != "noauth" ]; then
  echo -e "\nAuth support not configured for Scala tests"
  exit 1
fi

export MULTI_MONGOS_URI_SYSTEM_PROPERTY="-Dorg.mongodb.test.multi.mongos.uri=${MONGODB_URI}"

echo "Running scala tests with Scala $SCALA"

./gradlew -version
./gradlew -PjavaVersion=${JAVA_VERSION} -PscalaVersion=$SCALA --stacktrace --info scalaCheck \
    -Dorg.mongodb.test.uri=${MONGODB_URI} ${MULTI_MONGOS_URI_SYSTEM_PROPERTY}
