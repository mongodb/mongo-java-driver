#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

JDK=${JDK:-jdk11}

############################################
#            Main Program                  #
############################################

echo "Running MONGODB-AWS authentication tests"

export JAVA_HOME="/opt/java/jdk11"

# ensure no secrets are printed in log files
set +x

# load the script
shopt -s expand_aliases # needed for `urlencode` alias
[ -s "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh" ] && source "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh"

MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
MONGODB_URI="${MONGODB_URI}/aws?authMechanism=MONGODB-AWS"
if [[ -n ${SESSION_TOKEN} ]]; then
    MONGODB_URI="${MONGODB_URI}&authMechanismProperties=AWS_SESSION_TOKEN:${SESSION_TOKEN}"
fi

# show test output
set -x

echo "Running tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=/opt/java/${JDK} -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --debug --info driver-core:test --tests AwsAuthenticationSpecification
