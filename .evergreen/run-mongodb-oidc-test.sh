#!/bin/bash

set +x          # Disable debug trace
set -eu

echo "Running MONGODB-OIDC authentication tests"
echo "OIDC_ENV $OIDC_ENV"

if [ $OIDC_ENV == "test" ]; then
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh
    # java will not need to be installed, but we need to config
    RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
    source "${RELATIVE_DIR_PATH}/setup-env.bash"
elif [ $OIDC_ENV == "azure" ]; then
    source ./env.sh
elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh
elif [ $OIDC_ENV == "k8s" ]; then
    # Make sure K8S_VARIANT is set.
    if [ -z "$K8S_VARIANT" ]; then
        echo "Must specify K8S_VARIANT"
        popd
        exit 1
    fi

    # fix for git permissions issue:
    git config --global --add safe.directory /tmp/test
else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi


if ! which java ; then
    echo "Installing java..."
    sudo apt install openjdk-17-jdk -y
    echo "Installed java."
fi

which java
export OIDC_TESTS_ENABLED=true

# use admin credentials for tests
TO_REPLACE="mongodb://"
REPLACEMENT="mongodb://$OIDC_ADMIN_USER:$OIDC_ADMIN_PWD@"
ADMIN_URI=${MONGODB_URI/$TO_REPLACE/$REPLACEMENT}

./gradlew -Dorg.mongodb.test.uri="$ADMIN_URI" \
  --stacktrace --debug --info --no-build-cache driver-core:cleanTest \
  driver-sync:test --tests OidcAuthenticationProseTests --tests UnifiedAuthTest \
  driver-reactive-streams:test --tests OidcAuthenticationAsyncProseTests \
