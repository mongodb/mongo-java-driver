#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  JDK                               Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                                    "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"
#  AWS_CREDENTIAL_PROVIDER           "builtIn", 'awsSdkV1', 'awsSdkV2'
############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Running MONGODB-AWS authentication tests"

# Handle credentials and environment setup.
. $DRIVERS_TOOLS/.evergreen/auth_aws/aws_setup.sh $1

# show test output
set -x

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version

# As this script may be executed multiple times in a single task, with different values for MONGODB_URI, it's necessary
# to run cleanTest to ensure that the test actually executes each run
./gradlew -PjavaVersion="${JAVA_VERSION}" -Dorg.mongodb.test.uri="${MONGODB_URI}" \
-Dorg.mongodb.test.aws.credential.provider="${AWS_CREDENTIAL_PROVIDER}" \
--stacktrace --debug --info --no-build-cache driver-core:cleanTest driver-core:test --tests AwsAuthenticationSpecification
