#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Creates the root archive file which includes all git submodules.
export GIT_ARCHIVE_FILE=${1:-"/tmp/mongo-java-driver.tar"}

echo "Creating archive: $GIT_ARCHIVE_FILE"
if [ -f "$GIT_ARCHIVE_FILE" ]; then
  echo "Removing existing archive file: $GIT_ARCHIVE_FILE"
  rm $GIT_ARCHIVE_FILE
fi

# create root archive
git archive --output $GIT_ARCHIVE_FILE HEAD

echo "Appending submodule archives"
git submodule status --recursive | awk '{ print $2 }' | xargs tar -rf $GIT_ARCHIVE_FILE

echo "Appending .git directory to the root archive"
tar -rf $GIT_ARCHIVE_FILE .git

echo "Created root archive $GIT_ARCHIVE_FILE"
