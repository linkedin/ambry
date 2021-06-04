#!/bin/bash

# exit when any command fails
set -e

echo "Building artifacts, and creating pom files"
./gradlew --scan assemble publishToMavenLocal

echo "Testing publication by uploading in dry run mode"
./gradlew -i --scan artifactoryPublishAll -Partifactory.dryRun

echo "Releasing (tagging, uploading to JFrog Artifactory)"
./gradlew -i --scan ciPerformRelease

