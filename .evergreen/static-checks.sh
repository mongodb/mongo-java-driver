#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Compiling JVM drivers"

./gradlew -version
./gradlew -PxmlReports.enabled=true --info -x test -x integrationTest -x spotlessApply clean check scalaCheck jar testClasses docs
