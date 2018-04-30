#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       TOPOLOGY                Allows you to modify variables and the MONGODB_URI based on test topology
#                               Supported values: "server", "replica_set", "sharded_cluster"
#       COMPRESSOR              Set to enable compression. Values are "snappy" and "zlib" (default is no compression)
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9"

AUTH=${AUTH:-noauth}
MONGODB_URI=${MONGODB_URI:-}
JDK=${JDK:-jdk}
TOPOLOGY=${TOPOLOGY:-server}
COMPRESSOR=${COMPRESSOR:-}

############################################
#            Main Program                  #
############################################

SOCKET_REGEX='(.*)localhost:([0-9]+)?(.*)'
while [[ $MONGODB_URI =~ $SOCKET_REGEX ]]; do
	MONGODB_URI="${BASH_REMATCH[1]}%2Ftmp%2Fmongodb-${BASH_REMATCH[2]}.sock${BASH_REMATCH[3]}"
done

# Provision the correct connection string and set up SSL if needed
if [ "$TOPOLOGY" == "sharded_cluster" ]; then

     if [ "$AUTH" = "auth" ]; then
       export MONGODB_URI="mongodb://bob:pwd123@%2Ftmp%2Fmongodb-27017.sock/?authSource=admin"
     else
       export MONGODB_URI="mongodb://%2Ftmp%2Fmongodb-27017.sock/"
     fi
fi

if [ "$COMPRESSOR" != "" ]; then
     if [[ "$MONGODB_URI" == *"?"* ]]; then
       export MONGODB_URI="${MONGODB_URI}&compressors=${COMPRESSOR}"
     else
       export MONGODB_URI="${MONGODB_URI}/?compressors=${COMPRESSOR}"
     fi
fi

echo "Running $AUTH tests over for $TOPOLOGY and connecting to $MONGODB_URI"

# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk9"

echo "Running tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} ${GRADLE_EXTRA_VARS} --stacktrace --info :driver-legacy:test :driver-sync:test
