#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

# compiled outside of lambda workflow. Note "SkipBuild: True" in template.yaml
./gradlew -version
./gradlew --info driver-lambda:shadowJar

. ${DRIVERS_TOOLS}/.evergreen/aws_lambda/run-deployed-lambda-aws-tests.sh
