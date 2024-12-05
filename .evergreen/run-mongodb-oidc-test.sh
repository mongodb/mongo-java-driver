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
    source "${RELATIVE_DIR_PATH}/javaConfig.bash"
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

    if [ -z "${VARIANT}" ]; then
        echo "VARIANT is not set"
        exit 1
    elif [ $VARIANT == "eks" ]; then
        path="${AWS_WEB_IDENTITY_TOKEN_FILE}"
    elif [ $VARIANT == "aks" ]; then
        path="${AZURE_FEDERATED_TOKEN_FILE}"
    elif [ $VARIANT == "gke" ]; then
        path="/var/run/secrets/kubernetes.io/serviceaccount/token"
    else
        echo "Unrecognized k8s VARIANT: $VARIANT"
        exit 1
    fi

    # Print the file
    if [ -f "$path" ]; then
        file_size=$(stat -c%s "$path")
        echo "VARIANT: $VARIANT"
        echo "Token file path: $path"
        echo "Token file size: $file_size bytes"
    else
        echo "Error: Token file not found at $path" >&2
        exit 1
    fi

    if [ $VARIANT == "gke" ]; then
        echo "Skipping gke test to avoid error code 137 when running gradle"
        exit 0
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
