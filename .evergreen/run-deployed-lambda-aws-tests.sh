#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"

# compiled outside of lambda workflow. Note "SkipBuild: True" in template.yaml
./gradlew -version
./gradlew -PxmlReports.enabled=true --info -x test -x integrationTest -x spotlessApply jar shadowJar

. ${DRIVERS_TOOLS}/.evergreen/aws_lambda/run-deployed-lambda-aws-tests.sh
