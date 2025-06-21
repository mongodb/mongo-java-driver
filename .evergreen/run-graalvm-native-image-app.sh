#!/bin/bash

# Supported/used environment variables:
# MONGODB_URI The connection string to use, including credentials and topology info.
# JAVA_VERSION The Java SE version for Gradle toolchain.

set -o errexit

readonly RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "MONGODB_URI: ${MONGODB_URI}"
echo "JAVA_HOME: ${JAVA_HOME}"
readonly JDK_GRAALVM_VAR_NAME="JDK${JAVA_VERSION}_GRAALVM"
readonly JDK_GRAALVM="${!JDK_GRAALVM_VAR_NAME}"
echo "The JDK distribution for running Gradle is"
echo "$("${JAVA_HOME}"/bin/java --version)"
echo "The Java SE version for the Gradle toolchain is ${JAVA_VERSION}"
echo "The GraalVM JDK distribution expected to be found at \`${JDK_GRAALVM}\` by the Gradle toolchain functionality is"
echo "$("${JDK_GRAALVM}"/bin/java --version)"
echo "The Gradle version is"
./gradlew --version

echo "Building and running the GraalVM native image app"
./gradlew -PincludeGraalvm -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.uri=${MONGODB_URI} :graalvm-native-image-app:nativeRun
