#!/bin/bash

# exit when any command fails
set -e

echo "Building and running unit tests"
./gradlew -s --scan build codeCoverageReport

echo "Uploading unit test coverage to codecov"
bash <(curl -s https://codecov.io/bash)
