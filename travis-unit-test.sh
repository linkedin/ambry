#!/bin/bash

# exit when any command fails
set -e

echo "Building and running unit tests excluding ambry-store"
./gradlew -s --scan -x :ambry-store:test build codeCoverageReport

echo "Uploading unit test coverage excluding ambry-store to codecov"
bash <(curl -s https://codecov.io/bash)
