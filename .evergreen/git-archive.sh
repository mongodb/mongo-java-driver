#!/bin/bash

# Exit the script with error if any of the commands fail
set -o errexit

# Returns the path to the root archive file which includes all git submodules.

echo "Creating root archive"
export GIT_ARCHIVE_FILE="/tmp/mongo-java-driver"

# create root archive
git archive --output "$GIT_ARCHIVE_FILE.tar" HEAD

echo "Appending submodule archives"
# for each of git submodules append to the root archive
git submodule foreach --recursive 'git archive --prefix=$path/ --output "$GIT_ARCHIVE_FILE-sub-$sha1.tar" $sha1'

if [[ $(ls $GIT_ARCHIVE_FILE-sub*.tar | wc -l) != 0  ]]; then
  echo "Combining all submodule archives into one tar"
  tar -cf $GIT_ARCHIVE_FILE.tar $GIT_ARCHIVE_FILE-sub*.tar

  echo "Removing submodule archives"
  rm -rf $GIT_ARCHIVE_FILE-sub*.tar
fi

echo "Appending .git directory to the root archive"
tar -rf $GIT_ARCHIVE_FILE.tar .git

echo "$GIT_ARCHIVE_FILE.tar"
