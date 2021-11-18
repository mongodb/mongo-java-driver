#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including username/password to use to connect to the server via PLAIN authentication mechanism
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"
#       KDC                     The KDC
#       REALM                   The realm
#       KEYTAB_BASE64           The BASE64-encoded keytab
#       PROJECT_DIRECTORY       The project directory
#       LOGIN_CONTEXT_NAME      The login context name to use to look up the GSSAPI Subject

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/javaConfig.bash"
echo "Running GSSAPI authentication tests with login context name '${LOGIN_CONTEXT_NAME}'"

echo ${KEYTAB_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

trap "rm ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab; exit" EXIT HUP

cat << EOF > .evergreen/java.login.drivers.config
${LOGIN_CONTEXT_NAME} {
    com.sun.security.auth.module.Krb5LoginModule required
          doNotPrompt=true useKeyTab=true keyTab="${PROJECT_DIRECTORY}/.evergreen/drivers.keytab" principal=drivers;
};
EOF

echo "Running tests with Java ${JAVA_VERSION}"
./gradlew -version
./gradlew -PjavaVersion=${JAVA_VERSION} --stacktrace --info \
-Dorg.mongodb.test.uri=${MONGODB_URI} -Pgssapi.enabled=true -Pgssapi.login.context.name=${LOGIN_CONTEXT_NAME} \
-Psun.security.krb5.debug=true -Pauth.login.config=file://${PROJECT_DIRECTORY}/.evergreen/java.login.drivers.config \
-Pkrb5.kdc=${KDC} -Pkrb5.realm=${REALM} -Psun.security.krb5.debug=true \
driver-core:test --tests GSSAPIAuthenticationSpecification --tests GSSAPIAuthenticatorSpecification --tests KerberosSubjectProviderTest
