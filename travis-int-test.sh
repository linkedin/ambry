#!/bin/bash

# exit when any command fails
set -e

echo "Running integration tests"
./gradlew -s --scan intTest codeCoverageReport

echo "Uploading integration test coverage to codecov"
bash <(curl -s https://codecov.io/bash)
