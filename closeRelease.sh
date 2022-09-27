#!/bin/bash

if [[ $1 == "[Gradle Release Plugin] - pre tag commit"* ]]
then
  echo "Closing release $1"
  ./gradlew closeAndReleaseRepository
else
  echo "Snapshot, doing nothing $1"
fi
