#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Supported/used environment variables:
#   JDK                                  Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#   ATLAS_X509_DEV                       Set the connection string for the Atlas X509 development cluster.
#   ATLAS_X509_DEV_CERT_BASE64           Set the base64 encoded contents of a PEM file containing the client certificate (signed by the mongodb dev CA) and client private key for the X509 authentication on development cluster.
#   ATLAS_X509_DEV_CERT_NOUSER_BASE64    Set the base64 encoded contents of a PEM file containing the client certificate (signed by the mongodb dev CA) and client private key for the X509 authentication on development cluster with the subject name that does not exist on the server/cluster.

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

MONGODB_URI=${ATLAS_X509_DEV:-}
echo "$MONGODB_URI"
ATLAS_X509_DEV_CERT_BASE64=${ATLAS_X509_DEV_CERT_BASE64:-}
ATLAS_X509_DEV_CERT_NOUSER_BASE64=${ATLAS_X509_DEV_CERT_NOUSER_BASE64:-}

############################################
#            Functions                     #
############################################

provision_keystores () {
  # Base64 decode contents of a PEM holder for client certificate (signed by the mongodb dev CA) and private key
  echo "${ATLAS_X509_DEV_CERT_BASE64}" | base64 --decode > ca_and_pk.pem
  echo "${ATLAS_X509_DEV_CERT_NOUSER_BASE64}" | base64 --decode > ca_and_pk_no_user.pem

  # Build the pkcs12 (keystore). We include the leaf-only certificate (with public key) and private key in the keystore,
  # assuming the signed certificate is already trusted by the Atlas as issuer is MongoDB dev CA.
 echo "Creating PKCS12 keystore from ca_and_pk.pem"
 openssl pkcs12 -export \
   -in ca_and_pk.pem \
   -out existing_user.p12 \
   -password pass:test

 echo "Creating PKCS12 keystore from ca_and_pk_no_user.pem"
 openssl pkcs12 -export \
   -in ca_and_pk_no_user.pem \
   -out non_existing_user.p12 \
   -password pass:test
}

############################################
#            Main Program                  #
############################################
echo "Running X509 Authentication tests with Java ${JAVA_VERSION}"

# Set up keystores for x509 authentication.
provision_keystores

./gradlew -PjavaVersion=${JAVA_VERSION} -Dorg.mongodb.test.uri=${MONGODB_URI} --info --continue \
 -Dorg.mongodb.test.x509.auth=true \
 -Dorg.mongodb.test.x509.auth.keystore.location="$(pwd)" \
 driver-sync:test --tests X509AuthenticationTest \
 driver-reactive-streams:test --tests X509AuthenticationTest