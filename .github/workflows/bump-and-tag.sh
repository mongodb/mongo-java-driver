#!/usr/bin/env bash
set -e

# This script assumes that release X.Y.Z will always be created from X.Y.Z-SNAPSHOT"
echo "Replace snapshot version with release version ${RELEASE_VERSION} in build.gradle"
sed --in-place "s/version = '.*-SNAPSHOT'/version = '${RELEASE_VERSION}'/g" build.gradle

echo "Create package commit for ${RELEASE_VERSION}"
git commit -m "Version: bump ${RELEASE_VERSION}" build.gradle

echo "Create release tag for ${RELEASE_VERSION}"
git tag -a -m "${RELEASE_VERSION}" r${RELEASE_VERSION}

echo "Bump to snapshot version for ${NEXT_VERSION}"
sed --in-place "s/version = '${RELEASE_VERSION}'/version = '${NEXT_VERSION}-SNAPSHOT'/g" build.gradle

echo "Create commit for version bump to ${NEXT_VERSION}"
git commit -m "Version: bump ${NEXT_VERSION}-SNAPSHOT" build.gradle
