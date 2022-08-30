#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  JDK                               Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                                    "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"
#  USE_BUILT_IN_AWS_CREDENTIAL_PROVIDER  "true" or "false"
############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Running MONGODB-AWS authentication tests"


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

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version

# As this script may be executed multiple times in a single task, with different values for MONGODB_URI, it's necessary
# to run cleanTest to ensure that the test actually executes each run
./gradlew -PjavaVersion="${JAVA_VERSION}" -Dorg.mongodb.test.uri="${MONGODB_URI}" \
-Dorg.mongodb.test.use.built.in.aws.credential.provider="${USE_BUILT_IN_AWS_CREDENTIAL_PROVIDER}" \
--stacktrace --debug --info --no-build-cache driver-core:cleanTest driver-core:test --tests AwsAuthenticationSpecification
