#!/bin/bash

# exit when any command fails
set -e

echo "Building and running unit tests for ambry-store"
./gradlew -s --scan :ambry-store:test codeCoverageReport

echo "Uploading unit test coverage for ambry-store to codecov"
bash <(curl -s https://codecov.io/bash)
