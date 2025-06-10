#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Returns the path to the root archive file which includes all git submodules.

echo "Creating root archive"
export GIT_ARCHIVE_FILE="/tmp/mongo-java-driver.tar"

# create root archive
git archive --output $GIT_ARCHIVE_FILE HEAD

echo "Appending submodule archives"
git submodule status --recursive | awk '{ print $2 }' | xargs tar -rf $GIT_ARCHIVE_FILE

echo "Appending .git directory to the root archive"
tar -rf $GIT_ARCHIVE_FILE .git

echo "$GIT_ARCHIVE_FILE"
