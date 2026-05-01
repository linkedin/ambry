/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests {@link HelixClusterManager}'s FULL_AUTO ("FAR") code paths. These tests previously lived in
 * {@link HelixClusterManagerTest} but each ran across all 11 parameter combinations and were
 * skipped via {@code assumeTrue(... && fullAutoCompatible)} on 9 of 11 — wasting constructor
 * setup on combinations that produced no useful coverage. They are extracted here as a
 * single non-parameterized class with a FAR-compatible {@link MockHelixCluster} (i.e. bootstrapped
 * with {@code maxInstancesInOneResourceForFullAuto = 10000}).
 */
public class HelixClusterManagerFullAutoTest {
  private static final String CLUSTER_NAME_PREFIX = "Ambry-";
  private static final String CLUSTER_NAME_STATIC = "HelixClusterManagerFullAutoTestCluster";
  private static final String[] HELIX_DCS = new String[]{"DC0", "DC1"};
  private static final HashMap<String, com.github.ambry.utils.TestUtils.ZkInfo> dcsToZkInfo = new HashMap<>();
  private static final List<Integer> zookeeperServerPorts = new ArrayList<>();

  // The 6 FAR tests all gate on !useComposite and at most also !useAggregatedView. listenCrossColo
  // and !overrideEnabled are the standard defaults the parent class used for these tests.
  private static final boolean USE_COMPOSITE = false;
  private static final boolean USE_AGGREGATED_VIEW = false;
  private static final boolean OVERRIDE_ENABLED = false;
  private static final boolean LISTEN_CROSS_COLO = true;

  private final TestHardwareLayout testHardwareLayout;
  private final ClusterMapConfig clusterMapConfig;
  private final MockHelixCluster helixCluster;
  private final DataNode currentNode;
  private final String hostname;
  private final int portNum;
  private final String selfInstanceName;
  private final String localDc;
  private final String remoteDc;
  private final JSONObject zkJson;
  private HelixClusterManager clusterManager;
  private MetricRegistry metricRegistry;

  public HelixClusterManagerFullAutoTest() throws Exception {
    MockitoAnnotations.initMocks(this);
    localDc = HELIX_DCS[0];
    remoteDc = HELIX_DCS[1];
    Random random = new Random();
    File tempDir = Files.createTempDirectory("helixClusterManagerFullAuto-" + random.nextInt(1000)).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();
    String hardwareLayoutPath = tempDirPath + File.separator + "hardwareLayoutTest.json";
    String partitionLayoutPath = tempDirPath + File.separator + "partitionLayoutTest.json";
    String zkLayoutPath = tempDirPath + File.separator + "zkLayoutPath.json";
    zkJson = constructZkLayoutJSON(dcsToZkInfo.values());
    testHardwareLayout = constructInitialHardwareLayoutJSON(CLUSTER_NAME_STATIC);
    TestPartitionLayout testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, 3, localDc);

    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    // FAR-compatible cluster: maxInstancesInOneResourceForFullAuto = 10000 (vs -1 for FAR-incompatible).
    helixCluster = new MockHelixCluster(CLUSTER_NAME_PREFIX, hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        localDc, USE_AGGREGATED_VIEW, 100, 10000);

    currentNode = testHardwareLayout.getRandomDataNodeFromDc(localDc);
    hostname = currentNode.getHostname();
    portNum = currentNode.getPort();
    selfInstanceName = getInstanceName(hostname, portNum);
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(getProperties()));
    metricRegistry = new MetricRegistry();
    clusterManager = new HelixClusterManager(clusterMapConfig, selfInstanceName,
        new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, null, null, USE_AGGREGATED_VIEW), metricRegistry);
  }

  private Properties getProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", hostname);
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME_PREFIX + CLUSTER_NAME_STATIC);
    props.setProperty("clustermap.aggregated.view.cluster.name", CLUSTER_NAME_PREFIX + CLUSTER_NAME_STATIC);
    props.setProperty("clustermap.use.aggregated.view", Boolean.toString(USE_AGGREGATED_VIEW));
    props.setProperty("clustermap.datacenter.name", localDc);
    props.setProperty("clustermap.port", Integer.toString(portNum));
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.current.xid", Long.toString(64));
    props.setProperty("clustermap.enable.partition.override", Boolean.toString(OVERRIDE_ENABLED));
    props.setProperty("clustermap.listen.cross.colo", Boolean.toString(LISTEN_CROSS_COLO));
    return props;
  }

  @BeforeClass
  public static void setupZookeeperServers() throws Exception {
    Random random = new Random();
    File tempDir = Files.createTempDirectory("helixClusterManagerFullAuto-" + random.nextInt(1000)).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();
    int port = 2200;
    byte dcId = (byte) 0;
    for (String dcName : HELIX_DCS) {
      dcsToZkInfo.put(dcName, new com.github.ambry.utils.TestUtils.ZkInfo(tempDirPath, dcName, dcId++, port, true));
      zookeeperServerPorts.add(port);
      port++;
    }
  }

  @AfterClass
  public static void shutdownZookeeperServers() {
    for (com.github.ambry.utils.TestUtils.ZkInfo zkInfo : dcsToZkInfo.values()) {
      zkInfo.shutdown();
    }
  }

  @After
  public void after() {
    if (clusterManager != null) {
      clusterManager.close();
    }
    // Mirrors HelixClusterManagerTest's cleanup: wipe both the MockHelixCluster's ZK paths and the
    // namespaced one used by the cluster manager config, so listener state doesn't leak across tests.
    for (int port : zookeeperServerPorts) {
      String addr = "localhost:" + port;
      cleanupClusterPath(addr, helixCluster.getClusterName());
      cleanupClusterPath(addr, CLUSTER_NAME_PREFIX + CLUSTER_NAME_STATIC);
    }
  }

  private static void cleanupClusterPath(String zkAddr, String clusterName) {
    try {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore(zkAddr, "/" + clusterName, null);
      try {
        propertyStore.remove("/", AccessOption.PERSISTENT);
      } finally {
        propertyStore.stop();
      }
    } catch (Exception e) {
      // path likely doesn't exist for this cluster; ignore so the next cleanup still runs
    }
  }

  /**
   * Mirror of HelixClusterManagerTest's private verifyInitialClusterChanges helper. Verifies that
   * the cluster manager has caught up to the initial state of the underlying Helix cluster.
   */
  private void verifyInitialClusterChanges(HelixClusterManager clusterManager, MockHelixCluster helixCluster,
      String[] dcs) {
    Map<String, Set<AmbryDataNode>> dataNodesByDc = clusterManager.getDcToDataNodesMap();
    for (String dc : dcs) {
      List<InstanceConfig> instanceConfigsInCluster = helixCluster.getInstanceConfigsFromDcs(new String[]{dc});
      assertEquals("Mismatch in number of instances", instanceConfigsInCluster.size(), dataNodesByDc.get(dc).size());
      Set<String> hostsFromClusterManager =
          dataNodesByDc.get(dc).stream().map(AmbryDataNode::getHostname).collect(Collectors.toSet());
      Set<String> hostsFromHelix =
          instanceConfigsInCluster.stream().map(InstanceConfig::getHostName).collect(Collectors.toSet());
      assertEquals("Mismatch in hosts set", hostsFromHelix, hostsFromClusterManager);
      Map<String, Map<String, Set<String>>> partitionToResourceByDc = clusterManager.getPartitionToResourceMapByDC();
      Map<String, Set<String>> partitionToResourceMap = partitionToResourceByDc.get(dc);
      MockHelixAdmin helixAdmin = helixCluster.getHelixAdminFromDc(dc);
      List<IdealState> idealStates = helixAdmin.getIdealStates();
      for (IdealState idealState : idealStates) {
        String resourceName = idealState.getResourceName();
        Set<String> partitionSet = idealState.getPartitionSet();
        for (String partitionStr : partitionSet) {
          assertTrue("Mismatch in resource name", partitionToResourceMap.get(partitionStr).contains(resourceName));
        }
      }
    }
  }

  /**
   * Test getting new replica in full auto.
   */
  @Test
  public void getNewReplicaInFullAutoBasicTest() throws Exception {
    metricRegistry = new MetricRegistry();

    // Set the resource and all data nodes in local dc to FULL_AUTO
    List<String> resourceNames = helixCluster.getResources(localDc);
    String resourceName = resourceNames.get(0);
    String tag = "TAG_100000";
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setInstanceGroupTag(tag);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : testHardwareLayout.getAllDataNodesFromDc(localDc)) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(tag);
    }

    // Create Helix cluster manager
    Properties props = getProperties();
    long defaultReplicaCapacity = 50L * 1024 * 1024 * 1024;
    props.setProperty("clustermap.default.replica.capacity.in.bytes", String.valueOf(defaultReplicaCapacity));
    HelixClusterManagerTest.MockHelixManagerFactory helixFactory =
        new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, null, null, USE_AGGREGATED_VIEW);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixClusterManager helixClusterManager =
        new HelixClusterManager(clusterMapConfig, selfInstanceName, helixFactory, metricRegistry);

    // Case 1. We are creating a bootstrap replica for an exiting partition
    PartitionId partitionOfNewReplica = helixClusterManager.getAllPartitionIds(null).get(0);
    long expectedCapacity = partitionOfNewReplica.getReplicaIds().get(0).getCapacityInBytes();
    AmbryDataNode ambryDataNode = helixClusterManager.getDataNodeId(currentNode.getHostname(), currentNode.getPort());
    ReplicaId bootstrapReplica =
        helixClusterManager.getBootstrapReplica(partitionOfNewReplica.toPathString(), ambryDataNode);
    assertNotNull("New replica should be created successfully", bootstrapReplica);
    assertEquals("Bootstrap replica of existing partition should has peers' capacity", expectedCapacity,
        bootstrapReplica.getCapacityInBytes());
    assertEquals("There should be exactly one entry in bootstrap replica map", 1,
        helixClusterManager.getBootstrapReplicaMap().size());

    // Case 2. We are creating a bootstrap replica for a new partition
    String newPartitionId = String.valueOf(idealState.getNumPartitions() + 1001);
    bootstrapReplica = helixClusterManager.getBootstrapReplica(newPartitionId, ambryDataNode);
    assertNotNull(bootstrapReplica);
    assertEquals(defaultReplicaCapacity, bootstrapReplica.getCapacityInBytes());
    assertEquals("There should be 2 entries in bootstrap replica map", 2,
        helixClusterManager.getBootstrapReplicaMap().size());

    helixClusterManager.close();
  }

  /**
   * Test correct disk is used while getting new replica in full auto.
   */
  @Test
  public void getNewReplicaInFullAutoDiskUsageTest() throws Exception {
    metricRegistry = new MetricRegistry();
    // Set the node to FullAuto
    List<String> resourceNames = helixCluster.getResources(localDc);
    String resourceName = resourceNames.get(0);
    String tag = "TAG_100000";
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setInstanceGroupTag(tag);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : testHardwareLayout.getAllDataNodesFromDc(localDc)) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(tag);
    }

    // Create Helix cluster manager
    HelixClusterManager helixClusterManager = new HelixClusterManager(clusterMapConfig, selfInstanceName,
        new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, null, null, USE_AGGREGATED_VIEW), metricRegistry);
    // Select a partition for getting the bootstrap replica
    PartitionId partition = helixClusterManager.getAllPartitionIds(null).get(0);
    // Get list of disks present on the node
    HelixClusterManager.HelixClusterManagerQueryHelper clusterManagerCallback =
        helixClusterManager.getManagerQueryHelper();
    AmbryDataNode ambryDataNode = helixClusterManager.getDataNodeId(currentNode.getHostname(), currentNode.getPort());
    List<AmbryDisk> disks = new ArrayList<>(clusterManagerCallback.getDisks(ambryDataNode));
    // Get replica capacity of new replica
    long replicaCapacity = partition.getReplicaIds().get(0).getCapacityInBytes();

    // 1. Verify all disks are used in round-robin fashion
    for (AmbryDisk disk : disks) {
      // Set each disk capacity equal to twice the replica capacity
      disk.setDiskAvailableSpace(2 * replicaCapacity);
    }
    Set<AmbryDisk> potentialDisks = new HashSet<>(disks);
    for (int i = 0; i < disks.size(); i++) {
      ReplicaId bootstrapReplica = helixClusterManager.getBootstrapReplica(partition.toPathString(), ambryDataNode);
      assertTrue("Correct disk is not used", potentialDisks.contains((AmbryDisk) bootstrapReplica.getDiskId()));
      potentialDisks.remove((AmbryDisk) bootstrapReplica.getDiskId());
    }

    // 2. Verify bootstrapping fails when disk space is empty
    for (AmbryDisk disk : disks) {
      disk.setDiskAvailableSpace(replicaCapacity - 100);
    }
    assertNull("Bootstrapping replica should be fail since no disk space is available",
        helixClusterManager.getBootstrapReplica(partition.toPathString(), ambryDataNode));

    // 3. Verify disk with the most space is used
    AmbryDisk expectedDisk = null;
    for (int i = 0; i < disks.size(); i++) {
      AmbryDisk disk = disks.get(i);
      if (i == 0) {
        expectedDisk = disk;
        disk.setDiskAvailableSpace(2 * replicaCapacity);
      } else {
        disk.setDiskAvailableSpace(replicaCapacity);
      }
    }
    helixClusterManager.clearBootstrapDiskSelectionMap();
    ReplicaId bootstrapReplica = helixClusterManager.getBootstrapReplica(partition.toPathString(), ambryDataNode);
    assertEquals("Mismatch in disk used", expectedDisk, bootstrapReplica.getDiskId());

    helixClusterManager.close();
  }

  /**
   * Test correct disk is used while getting new replica in full auto under concurrent bootstrap requests.
   */
  @Test
  public void getNewReplicaInFullAutoRaceConditionTest() throws Exception {
    metricRegistry = new MetricRegistry();

    // 1. Set up for test
    // 1.1 Set the node to FullAuto
    List<String> resourceNames = helixCluster.getResources(localDc);
    String resourceName = resourceNames.get(0);
    String tag = "TAG_100000";
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setInstanceGroupTag(tag);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : testHardwareLayout.getAllDataNodesFromDc(localDc)) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(tag);
    }
    // 1.2 Create Helix cluster manager
    HelixClusterManager helixClusterManager = new HelixClusterManager(clusterMapConfig, selfInstanceName,
        new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, null, null, USE_AGGREGATED_VIEW), metricRegistry);
    // 1.3 Get list of disks present on the node
    HelixClusterManager.HelixClusterManagerQueryHelper clusterManagerCallback =
        helixClusterManager.getManagerQueryHelper();
    AmbryDataNode ambryDataNode = helixClusterManager.getDataNodeId(currentNode.getHostname(), currentNode.getPort());
    List<AmbryDisk> disks = new ArrayList<>(clusterManagerCallback.getDisks(ambryDataNode));
    // 1.4 Select partition for getting the bootstrap replicas
    PartitionId partition = helixClusterManager.getAllPartitionIds(null).get(0);
    long replicaCapacity = partition.getReplicaIds().get(0).getCapacityInBytes();
    // 1.5 Let only 2 disks be available
    for (int i = 0; i < disks.size(); i++) {
      AmbryDisk disk = disks.get(i);
      if (i == 0 || i == 1) {
        disk.setDiskAvailableSpace(replicaCapacity);
      } else {
        disk.setDiskAvailableSpace(0);
      }
    }

    // 2. Bootstrap 2 replicas in separate threads
    final ReplicaId[] bootstrapReplica1 = new ReplicaId[1];
    final ReplicaId[] bootstrapReplica2 = new ReplicaId[1];
    Thread t1 = new Thread(
        () -> bootstrapReplica1[0] = helixClusterManager.getBootstrapReplica(partition.toPathString(), ambryDataNode));
    Thread t2 = new Thread(
        () -> bootstrapReplica2[0] = helixClusterManager.getBootstrapReplica(partition.toPathString(), ambryDataNode));
    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // 3. Verify that both bootstrapping of both replicas are successful
    assertNotNull("Bootstrapping of replica must be successful", bootstrapReplica1[0]);
    assertNotNull("Bootstrapping of replica must be successful", bootstrapReplica2[0]);
    // 4. Verify disks for both replicas are different
    assertNotEquals("Both disks must be different", bootstrapReplica1[0].getDiskId(), bootstrapReplica2[0].getDiskId());

    helixClusterManager.close();
  }

  /**
   * Test {@link ClusterMap#isDataNodeInFullAutoMode}.
   * @throws Exception
   */
  @Test
  public void testHostFullAuto() throws Exception {
    HelixClusterManager helixClusterManager = (HelixClusterManager) clusterManager;
    verifyInitialClusterChanges(helixClusterManager, helixCluster, HELIX_DCS);

    // By default, all the hosts are not in FULL_AUTO mode.
    List<DataNode> allLocalDcDataNodes = testHardwareLayout.getAllDataNodesFromDc(localDc);
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertFalse("By default, local node should not on FULL_AUTO",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
    List<DataNode> allRemoteDcDataNodes = testHardwareLayout.getAllDataNodesFromDc(remoteDc);
    for (DataNode dataNode : allRemoteDcDataNodes) {
      try {
        helixClusterManager.isDataNodeInFullAutoMode(dataNode);
        fail("By default, remote node should throw an exception");
      } catch (IllegalArgumentException e) {
      }
    }

    // Should have only one Resource when bootup
    List<String> resourceNames = helixCluster.getResources(localDc);
    assertEquals("Should only have one resource when bootup", 1, resourceNames.size());
    String resourceName = resourceNames.get(0);
    String totalInstanceMetricName =
        MetricRegistry.name(HelixClusterManager.class, "Resource_" + resourceName + "_TotalInstanceCount");
    // Make sure that we don't have any FULL AUTO related metrics
    MetricRegistry registry = helixClusterManager.getMetricRegistry();
    assertFalse(registry.getGauges().keySet().contains(totalInstanceMetricName));

    // Update the IdealState to CUSTOMIZED mode, this would keep all local data node as they were.
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    // Update idealState here would impact the IdealState in the helixCluster since they reference to the same object
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    helixCluster.refreshIdealState();
    allLocalDcDataNodes = testHardwareLayout.getAllDataNodesFromDc(localDc);
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertFalse("Customized mode, local node should not on FULL_AUTO",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
    assertFalse(registry.getGauges().keySet().contains(totalInstanceMetricName));

    // Now update resource to have a tag
    final String instanceGroupTag = "TAG_1000000";
    idealState.setInstanceGroupTag(instanceGroupTag);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertFalse("Should not be in FULL_AUTO since IdealState is not on FULL_AUTO and instances don't have tags",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
    assertFalse(registry.getGauges().keySet().contains(totalInstanceMetricName));

    // Now update data node to have the same tag
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(instanceGroupTag);
      assertFalse("Should not be in FULL_AUTO since IdealState is not on FULL_AUTO",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
    assertFalse(registry.getGauges().keySet().contains(totalInstanceMetricName));

    // Now update resource to FULL_AUTO
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertTrue("Should be in FULL_AUTO mode", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
    assertTrue(registry.getGauges().keySet().contains(totalInstanceMetricName));

    // Now update data node to have another tag
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(instanceGroupTag + "_another_one");
      assertFalse("Instance has multiple tags", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }

    // Now update data node to remove the extra tag
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.removeTag(instanceGroupTag + "_another_one");
      assertTrue("Should be on FULL_AUTO", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }

    // Now update data node to remove the tag and add a new tag
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.removeTag(instanceGroupTag);
      instanceConfig.addTag("random_tag");
      assertFalse("Instance has random tag", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }

    // Now update data node to has the same tag but switch resource back to semi auto
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    helixCluster.refreshIdealState();
    assertFalse(registry.getGauges().keySet().contains(totalInstanceMetricName));
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.removeTag("random_tag");
      instanceConfig.addTag(instanceGroupTag);
      assertFalse("Resource is in SEMI AUTO", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }
  }

  /**
   * Test {@link ClusterMap#isDataNodeInFullAutoMode} when falling back to semi-auto.
   * @throws Exception
   */
  @Test
  public void testRollBackFromFullAuto() throws Exception {
    HelixClusterManager helixClusterManager = (HelixClusterManager) clusterManager;
    verifyInitialClusterChanges(helixClusterManager, helixCluster, HELIX_DCS);

    List<DataNode> allLocalDcDataNodes = testHardwareLayout.getAllDataNodesFromDc(localDc);
    String resourceName = helixCluster.getResources(localDc).get(0);
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    final String instanceGroupTag = "TAG_1000000";
    idealState.setInstanceGroupTag(instanceGroupTag);
    helixCluster.refreshIdealState();

    // 1. Test data node in full auto
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(instanceGroupTag);
      assertTrue("Should be on FULL_AUTO", helixClusterManager.isDataNodeInFullAutoMode(dataNode, true));
    }

    // 2. Test data node rollback for a resource present in /AdminConfigs/FullAutoMigration
    String addr = "localhost:" + dcsToZkInfo.get(localDc).getPort();
    HelixPropertyStore<ZNRecord> propertyStore =
        CommonUtils.createHelixPropertyStore(addr, "/" + helixCluster.getClusterName() + "/" + PROPERTYSTORE_STR, null);
    ZNRecord zNRecord = new ZNRecord(FULL_AUTO_MIGRATION_STR);
    zNRecord.setListField(RESOURCES_STR, Collections.singletonList(resourceName));
    propertyStore.set(FULL_AUTO_MIGRATION_ZNODE_PATH, zNRecord, AccessOption.PERSISTENT);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertTrue("Resource should considered as FULL AUTO while it is rolling back to help with local disk selection",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode, true));
      assertFalse("Resource should considered NOT IN FULL AUTO when not checking the property store",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode, false));
    }
  }

  /**
   * Test {@link ClusterMap#isDataNodeInFullAutoMode} when falling back to semi-auto for a resource
   * not present in the FULL_AUTO_MIGRATION znode.
   * @throws Exception
   */
  @Test
  public void testRollBackFromFullAutoForDifferentResource() throws Exception {
    HelixClusterManager helixClusterManager = (HelixClusterManager) clusterManager;
    verifyInitialClusterChanges(helixClusterManager, helixCluster, HELIX_DCS);

    List<DataNode> allLocalDcDataNodes = testHardwareLayout.getAllDataNodesFromDc(localDc);
    String resourceName = helixCluster.getResources(localDc).get(0);
    IdealState idealState = helixCluster.getResourceIdealState(resourceName, localDc);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    final String instanceGroupTag = "TAG_1000000";
    idealState.setInstanceGroupTag(instanceGroupTag);
    helixCluster.refreshIdealState();

    // 1. Test data node in full auto
    for (DataNode dataNode : allLocalDcDataNodes) {
      String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
      InstanceConfig instanceConfig =
          helixCluster.getHelixAdminFromDc(localDc).getInstanceConfig(helixCluster.getClusterName(), instanceName);
      instanceConfig.addTag(instanceGroupTag);
      assertTrue("Should be on FULL_AUTO", helixClusterManager.isDataNodeInFullAutoMode(dataNode));
    }

    // 2. Test data node rollback for a resource which is not present in /AdminConfigs/FullAutoMigration
    String addr = "localhost:" + dcsToZkInfo.get(localDc).getPort();
    HelixPropertyStore<ZNRecord> propertyStore =
        CommonUtils.createHelixPropertyStore(addr, "/" + helixCluster.getClusterName() + "/" + PROPERTYSTORE_STR, null);
    ZNRecord zNRecord = new ZNRecord(FULL_AUTO_MIGRATION_STR);
    zNRecord.setListField(RESOURCES_STR, Collections.singletonList("random_resource"));
    propertyStore.set(FULL_AUTO_MIGRATION_ZNODE_PATH, zNRecord, AccessOption.PERSISTENT);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    helixCluster.refreshIdealState();
    for (DataNode dataNode : allLocalDcDataNodes) {
      assertFalse("Resource should considered as SEMI AUTO",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode, true));
      assertFalse("Resource should considered as SEMI AUTO",
          helixClusterManager.isDataNodeInFullAutoMode(dataNode, false));
    }
  }
}
