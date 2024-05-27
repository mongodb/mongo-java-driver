#!/bin/bash

set -o errexit

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/javaConfig.bash"

echo "Creating SSLDC reports"
./gradlew -version
./gradlew -PssdlcReport.enabled=true --continue -x test -x integrationTest -x spotlessApply clean check scalaCheck kotlinCheck testClasses || true
echo "SpotBugs created the following SARIF files"
find . -path "*/spotbugs/*.sarif"
