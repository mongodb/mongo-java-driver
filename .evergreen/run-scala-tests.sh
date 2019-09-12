#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
JDK=${JDK:-jdk11}

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

echo "Running scala tests with Scala $SCALA"

./gradlew -version
./gradlew --stacktrace --info clean :bson-scala:test :driver-scala:test :driver-scala:integrationTest -Dorg.mongodb.test.uri=${MONGODB_URI} -PscalaVersion=$SCALA
