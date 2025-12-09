#!/usr/bin/env bash
set -e

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <old version> <new version>" >&2
  exit 1
fi

FROM_VERSION=$1
TO_VERSION=$2

sed --in-place "s/version=${FROM_VERSION}/version=${TO_VERSION}/g" gradle.properties
git commit -m "Version: bump ${TO_VERSION}" gradle.properties
