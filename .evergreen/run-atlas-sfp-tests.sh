#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Runs the Atlas Secure Frontend Processor (SFP) prose tests.
# See https://github.com/mongodb/specifications/blob/master/source/atlas-sfp-testing/atlas-sfp-testing.md
#
# Supported/used environment variables (populated from the `drivers/sfp` secrets vault via secrets-export.sh):
#   JAVA_VERSION            Set the version of java to be used.
#   SFP_ATLAS_URI           Connection string for the SFP-proxied cluster, without credentials.
#   SFP_ATLAS_USER          Username for SCRAM-SHA-256 authentication.
#   SFP_ATLAS_PASSWORD      Password for SCRAM-SHA-256 authentication.
#   SFP_ATLAS_X509_URI      Connection string for X.509 authentication through the SFP.
#   SFP_ATLAS_X509_BASE64   Base64 encoded contents of a PEM file containing the X.509 client certificate and private key.

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

SFP_ATLAS_URI=${SFP_ATLAS_URI:?"SFP_ATLAS_URI is required"}
SFP_ATLAS_USER=${SFP_ATLAS_USER:?"SFP_ATLAS_USER is required"}
SFP_ATLAS_PASSWORD=${SFP_ATLAS_PASSWORD:?"SFP_ATLAS_PASSWORD is required"}
SFP_ATLAS_X509_URI=${SFP_ATLAS_X509_URI:?"SFP_ATLAS_X509_URI is required"}
SFP_ATLAS_X509_BASE64=${SFP_ATLAS_X509_BASE64:?"SFP_ATLAS_X509_BASE64 is required"}

############################################
#            Functions                     #
############################################

provision_keystore () {
  # Base64 decode the PEM holding the client certificate and private key, then build a PKCS12 keystore from it.
  echo "${SFP_ATLAS_X509_BASE64}" | base64 --decode > sfp_x509.pem

  echo "Creating PKCS12 keystore from sfp_x509.pem"
  openssl pkcs12 -export \
    -in sfp_x509.pem \
    -out sfp_x509.p12 \
    -password pass:test
}

############################################
#            Main Program                  #
############################################
echo "Running Atlas SFP tests with Java ${JAVA_VERSION}"

provision_keystore

./gradlew -PjavaVersion=${JAVA_VERSION} --info --continue \
 -Dorg.mongodb.test.sfp.uri="${SFP_ATLAS_URI}" \
 -Dorg.mongodb.test.sfp.user="${SFP_ATLAS_USER}" \
 -Dorg.mongodb.test.sfp.password="${SFP_ATLAS_PASSWORD}" \
 -Dorg.mongodb.test.sfp.x509.uri="${SFP_ATLAS_X509_URI}" \
 -Dorg.mongodb.test.sfp.x509.keystore.location="$(pwd)" \
 driver-sync:test --tests AtlasSfpProseTest \
 driver-reactive-streams:test --tests AtlasSfpProseTest
