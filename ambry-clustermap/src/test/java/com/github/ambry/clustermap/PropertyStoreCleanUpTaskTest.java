/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskResult;
import org.junit.Test;

import static org.junit.Assert.*;


public class PropertyStoreCleanUpTaskTest {
  private final MetricRegistry metricRegistry;
  private static final String CLUSTER_NAME = "testCluster";
  private static final String HOST_NAME = "localhost";
  private static final int  PORT = 12345;
  private static final int ZK_PORT = 1234;
  private static final String DC = "DC1";
  private static final String ZK_HOST = "localhost";

  public PropertyStoreCleanUpTaskTest()  {
    metricRegistry = new MetricRegistry();
  }

  /**
   * Test the case when instance is down and not present in ideal state or external view:-
   * Replicas should be removed from property store if they are offline
   */
  @Test
  public void testPropertyStoreCleanUpTaskInstanceDownAndNotPresentInIdealState() throws IOException {

    TestUtils.ZkInfo zkInfo =
        new TestUtils.ZkInfo(TestUtils.getTempDir(PropertyStoreCleanUpTask.COMMAND), DC, (byte) 1, ZK_PORT + '1', true);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(getProperties()));
    HelixManager helixManager = new MockHelixManager(ClusterMapUtils.getInstanceName(HOST_NAME, PORT),
        InstanceType.PARTICIPANT, ZK_HOST + ":" + zkInfo.getPort(), CLUSTER_NAME, new MockHelixAdmin(),
        null, null, null, false);
    DataNodeConfigSource dataNodeConfigSource = new PropertyStoreToDataNodeConfigAdapter(
        ZK_HOST + ":" + zkInfo.getPort(), clusterMapConfig);

    testCluster1(helixManager, dataNodeConfigSource);

    // Run the task
    PropertyStoreCleanUpTask propertyStoreCleanUpTask =
        new PropertyStoreCleanUpTask(helixManager, dataNodeConfigSource,clusterMapConfig, metricRegistry);
    TaskResult taskResult = propertyStoreCleanUpTask.run();

    //Task should be completed successfully
    assertEquals(TaskResult.Status.COMPLETED, taskResult.getStatus());

    //Replica should be removed from property store for down host -> localhost_2
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().isEmpty());
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().isEmpty());

    //Replica should be present in property store for up hosts -> localhost_1, localhost_3
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));
  }

  /**
   * Test the case when instance is down but present in ideal state or external view
   * Replicas should not be removed from property store
   */
  @Test
  public void testPropertyStoreCleanUpTaskInstanceDownButPresentInIdealState() throws IOException {
    TestUtils.ZkInfo zkInfo =
        new TestUtils.ZkInfo(TestUtils.getTempDir(PropertyStoreCleanUpTask.COMMAND), DC, (byte) 1, ZK_PORT + '2', true);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(getProperties()));
    HelixManager helixManager = new MockHelixManager(ClusterMapUtils.getInstanceName(HOST_NAME, PORT),
        InstanceType.PARTICIPANT, ZK_HOST + ":" + zkInfo.getPort(), CLUSTER_NAME, new MockHelixAdmin(),
        null, null, null, false);
    DataNodeConfigSource dataNodeConfigSource = new PropertyStoreToDataNodeConfigAdapter(
        ZK_HOST + ":" + zkInfo.getPort(), clusterMapConfig);

    testCluster2(helixManager, dataNodeConfigSource);

    // Run the task
    PropertyStoreCleanUpTask propertyStoreCleanUpTask =
        new PropertyStoreCleanUpTask(helixManager, dataNodeConfigSource, clusterMapConfig, metricRegistry);
    TaskResult taskResult = propertyStoreCleanUpTask.run();

    //Task should be completed successfully
    assertEquals(TaskResult.Status.COMPLETED, taskResult.getStatus());

    //Replica should be present in property store for down host -> localhost_2
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    //Replica should be present in property store for up hosts -> localhost_1, localhost_3
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));
  }

  /**
   * Test the case when no instance is down and all instances are present in ideal state or external view
   * Replicas should not be removed from property store
   */
  @Test
  public void testPropertyStoreCleanUpTaskAllInstancesUp() throws IOException {
    TestUtils.ZkInfo zkInfo =
        new TestUtils.ZkInfo(TestUtils.getTempDir(PropertyStoreCleanUpTask.COMMAND), DC, (byte) 1, ZK_PORT + '3', true);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(getProperties()));
    HelixManager helixManager = new MockHelixManager(ClusterMapUtils.getInstanceName(HOST_NAME, PORT),
        InstanceType.PARTICIPANT, ZK_HOST + ":" + zkInfo.getPort(), CLUSTER_NAME, new MockHelixAdmin(),
        null, null, null, false);
    DataNodeConfigSource dataNodeConfigSource = new PropertyStoreToDataNodeConfigAdapter(
        ZK_HOST + ":" + zkInfo.getPort(), clusterMapConfig);

    testCluster3(helixManager, dataNodeConfigSource);

    // Run the task
    PropertyStoreCleanUpTask propertyStoreCleanUpTask =
        new PropertyStoreCleanUpTask(helixManager, dataNodeConfigSource, clusterMapConfig, metricRegistry);
    TaskResult taskResult = propertyStoreCleanUpTask.run();

    //Task should be completed successfully
    assertEquals(TaskResult.Status.COMPLETED, taskResult.getStatus());

    //Replica should be present in property store for all hosts -> localhost_1, localhost_2, localhost_3
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));
  }

  /**
   * Test case when DELETE_DATA_FROM_DATANODE_CONFIG is false
   * Replicas should not be removed from property store
   */
  @Test
  public void testPropertyStoreCleanUpTaskDeleteDataFromDataNodeConfigFalse() throws IOException {
    Properties props = getProperties();
    props.setProperty(ClusterMapConfig.DELETE_DATA_FROM_DATANODE_CONFIG_IN_PROPERTY_STORE_CLEAN_UP_TASK, "false");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    TestUtils.ZkInfo zkInfo =
        new TestUtils.ZkInfo(TestUtils.getTempDir(PropertyStoreCleanUpTask.COMMAND), DC, (byte) 1, ZK_PORT + '4', true);
    HelixManager helixManager = new MockHelixManager(ClusterMapUtils.getInstanceName(HOST_NAME, PORT),
        InstanceType.PARTICIPANT, ZK_HOST + ":" + zkInfo.getPort(), CLUSTER_NAME, new MockHelixAdmin(),
        null, null, null, false);
    DataNodeConfigSource dataNodeConfigSource = new PropertyStoreToDataNodeConfigAdapter(
        ZK_HOST + ":" + zkInfo.getPort(), clusterMapConfig);

    testCluster1(helixManager, dataNodeConfigSource);

    // Run the task
    PropertyStoreCleanUpTask propertyStoreCleanUpTask =
        new PropertyStoreCleanUpTask(helixManager, dataNodeConfigSource, clusterMapConfig, metricRegistry);
    TaskResult taskResult = propertyStoreCleanUpTask.run();

    //Task should be completed successfully
    assertEquals(TaskResult.Status.COMPLETED, taskResult.getStatus());

    //Replica should be present in property store for all hosts -> localhost_1, localhost_2, localhost_3
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_1", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_2", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));

    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk1").getReplicaConfigs().containsKey("partition1"));
    assertTrue(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName("localhost_3", PORT))
        .getDiskConfigs().get("disk2").getReplicaConfigs().containsKey("partition2"));
  }

  /**
   * Case when instance is down and not present in ideal state or external view
   * @param helixManager
   * @param dataNodeConfigSource
   */
  private void testCluster1(HelixManager helixManager, DataNodeConfigSource dataNodeConfigSource) {
    MockHelixAdmin admin =  (MockHelixAdmin)helixManager.getClusterManagmentTool();
    // Add 3 instances to cluster
    addInstancesToCluster(helixManager, ClusterMapUtils.getInstanceName("localhost_1", PORT),
        ClusterMapUtils.getInstanceName("localhost_2", PORT), ClusterMapUtils.getInstanceName("localhost_3", PORT));

    // Bring down instance2
    admin.bringInstanceDown(ClusterMapUtils.getInstanceName("localhost_2", PORT));

    /* Add a resourceName to cluster with partition on localhost_1 and localhost_3
     localhost_2 is down, hence assuming it will be removed by fullAuto from ideal state and external view
     so we will not include it in preference list here
     */
    IdealState idealState = new IdealState("resource1");
    idealState.setPreferenceList("partition1", Arrays.asList(ClusterMapUtils
        .getInstanceName("localhost_1", PORT), ClusterMapUtils.getInstanceName("localhost_3", PORT)));
    admin.addResource(CLUSTER_NAME, "resource1", idealState);

    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk2", "partition2");


  }

  /**
   * Case when instance is down but present in ideal state or external view
   * @param helixManager
   * @param dataNodeConfigSource
   * @return
   */
  private void testCluster2(HelixManager helixManager, DataNodeConfigSource dataNodeConfigSource){
    MockHelixAdmin admin =  (MockHelixAdmin)helixManager.getClusterManagmentTool();
    // Add 3 instances to cluster
    addInstancesToCluster(helixManager, ClusterMapUtils.getInstanceName("localhost_1", PORT),
        ClusterMapUtils.getInstanceName("localhost_2", PORT), ClusterMapUtils.getInstanceName("localhost_3", PORT));

    // Bring down instance2
    admin.bringInstanceDown(ClusterMapUtils.getInstanceName("localhost_2", PORT));

    // Add a resourceName to cluster with partition on localhost_1 and localhost_2 and localhost_3
    IdealState idealState = new IdealState("resource1");
    idealState.setPreferenceList("partition1", Arrays.asList(ClusterMapUtils
        .getInstanceName("localhost_1", PORT), ClusterMapUtils.getInstanceName("localhost_2", PORT),
        ClusterMapUtils.getInstanceName("localhost_3", PORT)));
    admin.addResource(CLUSTER_NAME, "resource1", idealState);

    // Add replicas to property store
    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk2", "partition2");
  }

  /**
   * Case when no instance is down and all instances are present in ideal state or external view
   * @param helixManager
   * @param dataNodeConfigSource
   * @return
   */
  private void testCluster3(HelixManager helixManager, DataNodeConfigSource dataNodeConfigSource){
    MockHelixAdmin admin =  (MockHelixAdmin)helixManager.getClusterManagmentTool();
    // Add 3 instances to cluster
    addInstancesToCluster(helixManager, ClusterMapUtils.getInstanceName("localhost_1", PORT),
        ClusterMapUtils.getInstanceName("localhost_2", PORT), ClusterMapUtils.getInstanceName("localhost_3", PORT));

    // Add a resourceName to cluster with partition on localhost_1 and localhost_2 and localhost_3
    IdealState idealState = new IdealState("resource1");
    idealState.setPreferenceList("partition1", Arrays.asList(ClusterMapUtils
        .getInstanceName("localhost_1", PORT), ClusterMapUtils.getInstanceName("localhost_2", PORT),
        ClusterMapUtils.getInstanceName("localhost_3", PORT)));
    admin.addResource(CLUSTER_NAME, "resource1", idealState);

    // Add replicas to property store
    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_1", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_2", "disk2", "partition2");

    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk1", "partition1");
    addReplicasToDisk(dataNodeConfigSource, "localhost_3", "disk2", "partition2");

  }

  private DataNodeConfig getDataNodeConfig(DataNodeConfigSource dataNodeConfigSource, String host) {
    if(dataNodeConfigSource.get(ClusterMapUtils.getInstanceName(host, PORT)) != null) {
      return dataNodeConfigSource.get(ClusterMapUtils.getInstanceName(host, PORT));
    }
    String instanceName = ClusterMapUtils.getInstanceName(host, PORT);
    return new DataNodeConfig(instanceName, host, PORT, DC, PORT + 1, PORT + 2, "rack", ClusterMapUtils.DEFAULT_XID);
  }

  private void setDataNodeConfig(DataNodeConfig dataNodeConfig, String disk, String partition, DataNodeConfigSource dataNodeConfigSource) {
    DataNodeConfig.DiskConfig diskConfig = new DataNodeConfig.DiskConfig(HardwareState.AVAILABLE, 1000);
    diskConfig.getReplicaConfigs().put(partition, new DataNodeConfig.ReplicaConfig(100, "maxAllReplicas"));
    dataNodeConfig.getDiskConfigs().put(disk, diskConfig);
    dataNodeConfigSource.set(dataNodeConfig);
  }

  private void addReplicasToDisk(DataNodeConfigSource dataNodeConfigSource, String host, String disk, String partition) {
    // Add replicas to property store
    DataNodeConfig dataNodeConfig = getDataNodeConfig(dataNodeConfigSource, host);
    setDataNodeConfig(dataNodeConfig, disk, partition, dataNodeConfigSource);
  }

  private Properties getProperties() {
    Properties props = new Properties();
    props.setProperty(ClusterMapConfig.DELETE_DATA_FROM_DATANODE_CONFIG_IN_PROPERTY_STORE_CLEAN_UP_TASK, "true");
    props.setProperty(ClusterMapConfig.ENABLE_PROPERTY_STORE_CLEAN_UP_TASK, "true");
    props.setProperty(ClusterMapConfig.CLUSTERMAP_DATA_NODE_CONFIG_SOURCE_TYPE,
        DataNodeConfigSourceType.PROPERTY_STORE.name());
    props.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, CLUSTER_NAME);
    props.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, DC);
    props.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, HOST_NAME);
    return props;
  }

  private void addInstancesToCluster(HelixManager helixManager, String... instances) {
    MockHelixAdmin admin =  (MockHelixAdmin)helixManager.getClusterManagmentTool();
    for (String instance : instances) {
      admin.addInstance(CLUSTER_NAME, new InstanceConfig(instance));
    }
  }

}
