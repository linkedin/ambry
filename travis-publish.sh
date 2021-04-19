#!/bin/bash

# exit when any command fails
set -e

echo "Building artifacts, and creating pom files"
./gradlew --scan assemble publishToMavenLocal

echo "Testing publication by uploading in dry run mode"
# TODO remove bintray here
./gradlew -i --scan bintrayUploadAll artifactoryPublishAll -Pbintray.dryRun -Partifactory.dryRun

echo "Pull request: [$TRAVIS_PULL_REQUEST], Travis branch: [$TRAVIS_BRANCH]"
# release only from master when no pull request build
if [ "$TRAVIS_BRANCH" = "master" ] && [ "$TRAVIS_PULL_REQUEST" = "false" ]
then
    echo "Releasing (tagging, uploading to Bintray)"
    ./gradlew -i --scan ciPerformRelease
fi
