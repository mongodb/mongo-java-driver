#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

echo "Compiling java driver with jdk8"

# We always compile with the latest version of java
export JAVA_HOME="/opt/java/jdk8"
./gradlew -version
./gradlew -PxmlReports.enabled=true --info -x test clean check jar testClasses javadoc
