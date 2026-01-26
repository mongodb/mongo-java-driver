#!/usr/bin/env bash
set -e

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <current version> <release version> <next version>" >&2
  exit 1
fi

CURRENT_VERSION=$1
RELEASE_VERSION=$2
NEXT_VERSION=$3

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

echo "Bump version in gradle.properties to ${RELEASE_VERSION}"
${SCRIPT_DIR}/bump-version.sh "${RELEASE_VERSION_WITHOUT_SUFFIX}-SNAPSHOT" "${RELEASE_VERSION}"

echo "Create release tag for ${RELEASE_VERSION}"
git tag -a -m "${RELEASE_VERSION}" r${RELEASE_VERSION}

echo "Bump to snapshot version for ${NEXT_VERSION}"
${SCRIPT_DIR}/bump-version.sh "${RELEASE_VERSION}" "${NEXT_VERSION}-SNAPSHOT"
