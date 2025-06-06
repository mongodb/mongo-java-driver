#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Enable caching"
echo "org.gradle.caching=true" >> gradle.properties
echo "kotlin.caching.enabled=true" >> gradle.properties

echo "Compiling JVM drivers"
./gradlew -version
./gradlew classes --parallel

# Copy the Gradle dependency cache to the gradle read only dependency cache directory.
if [ -n "$GRADLE_RO_DEP_CACHE" ];then
  echo "Copying Gradle dependency cache to $GRADLE_RO_DEP_CACHE"
  mkdir -p $GRADLE_RO_DEP_CACHE

  # https://docs.gradle.org/current/userguide/dependency_caching.html#sec:cache-copy
  # Gradle suggests removing the "*.lock" files and the `gc.properties` file for saving/restoring cache
  cp -r $HOME/.gradle/caches/modules-2 "$GRADLE_RO_DEP_CACHE"
  find "$GRADLE_RO_DEP_CACHE" -name "*.lock" -type f | xargs rm -f
  find "$GRADLE_RO_DEP_CACHE" -name "gc.properties" -type f | xargs rm -f

  echo "Copied Gradle dependency cache to $GRADLE_RO_DEP_CACHE"
fi
