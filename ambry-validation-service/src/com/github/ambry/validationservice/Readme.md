##Validation Service Run Book
Validation Service is used to validate and execute tests in a distributed fashion across Ambry
server nodes of the cluster

This run book goes over the steps required for a dev environment set up and testing

## Set up
To execute this tool, execute "./gradlew allJar" in the root directory.
This will generate ambry.jar in "target" folder under root directory and the same will be used to assist us in using
these tools.

## 1. ZooKeeper
Start Zookeeper
```bash
java -cp ambry.jar com.github.ambry.validationservice.ZookeeperService
```

## 2. Add Cluster
Adds a new cluster to Zookeeper
```bash
java -cp ambry.jar com.github.ambry.validationservice.ValidationServiceHelper --typeOfOperation add_cluster
```

## 3. Helix Controller for Ambry
Start a helix controller for Ambry
```bash
java -cp ambry.jar com.github.ambry.validationservice.ValidationController
```

## 4. Instantiate Participants
Add 3 participants to the cluster
```bash
java -cp ambry.jar com.github.ambry.validationservice.ValidationService --servicePort 6999
java -cp ambry.jar com.github.ambry.validationservice.ValidationService --servicePort 7000
java -cp ambry.jar com.github.ambry.validationservice.ValidationService --servicePort 7001
```

## 5. Trigger Generic Jobs
Trigger generic jobs against the cluster participants
```bash
java -cp ambry.jar com.github.ambry.validationservice.ValidationServiceHelper --typeOfOperation trigger_generic_job
```


## 6. Trigger Targeted Jobs
Trigger targeted jobs against the cluster participants(targeted against specific instance groups)
```bash
java -cp ambry.jar com.github.ambry.validationservice.ValidationServiceHelper --typeOfOperation trigger_targeted_job
```
