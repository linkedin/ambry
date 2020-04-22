/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link HelixParticipant}
 */
@RunWith(Parameterized.class)
public class HelixParticipantTest {
  private final MockHelixManagerFactory helixManagerFactory;
  private final Properties props;
  private final String clusterName = "HelixParticipantTestCluster";
  private final JSONObject zkJson;
  private final String stateModelDef;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{ClusterMapConfig.DEFAULT_STATE_MODEL_DEF}, {ClusterMapConfig.AMBRY_STATE_MODEL_DEF}});
  }

  public HelixParticipantTest(String stateModelDef) throws Exception {
    List<ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new ZkInfo(null, "DC0", (byte) 0, 2199, false));
    zkJson = constructZkLayoutJSON(zkInfoList);
    props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "2200");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.state.model.definition", stateModelDef);
    this.stateModelDef = stateModelDef;
    helixManagerFactory = new MockHelixManagerFactory();
  }

  /**
   * Tests setReplicaSealedState method for {@link HelixParticipant}
   * @throws IOException
   */
  @Test
  public void testGetAndSetReplicaSealedState() throws IOException {
    //setup HelixParticipant and dependencies
    String partitionIdStr = "somePartitionId";
    String partitionIdStr2 = "someOtherPartitionId";
    ReplicaId replicaId = createMockAmbryReplica(partitionIdStr);
    ReplicaId replicaId2 = createMockAmbryReplica(partitionIdStr2);
    String hostname = "localhost";
    int port = 2200;
    String instanceName = ClusterMapUtils.getInstanceName(hostname, port);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant helixParticipant =
        new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
            getDefaultZkConnectStr(clusterMapConfig), true);
    helixParticipant.participate(Collections.emptyList());
    HelixManager helixManager = helixManagerFactory.getZKHelixManager(null, null, null, null);
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    InstanceConfig instanceConfig = new InstanceConfig("someInstanceId");
    helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);

    //Make sure the current sealedReplicas list is empty
    List<String> sealedReplicas = helixParticipant.getSealedReplicas();
    assertEquals("sealedReplicas should be empty", Collections.emptyList(), sealedReplicas);

    String listName = "sealedReplicas";

    //Check that invoking setReplicaSealedState with a non-AmbryReplica ReplicaId throws an IllegalArgumentException
    ReplicaId notAmbryReplica = createMockNotAmbryReplica(partitionIdStr);
    try {
      helixParticipant.setReplicaSealedState(notAmbryReplica, true);
      fail("Expected an IllegalArgumentException here");
    } catch (IllegalArgumentException e) {
      //Expected exception
    }

    //Check that invoking setReplicaSealedState adds the partition to the list of sealed replicas
    helixParticipant.setReplicaSealedState(replicaId, true);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, listName);
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Seal another replicaId
    helixParticipant.setReplicaSealedState(replicaId2, true);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 2, listName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Check that sealed replica list doesn't take duplicates (and that dups are detected by partitionId comparison, not
    //replicaId object comparison
    ReplicaId dup = createMockAmbryReplica(partitionIdStr);
    helixParticipant.setReplicaSealedState(dup, true);
    helixParticipant.setReplicaSealedState(replicaId2, true);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 2, listName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Check that invoking setReplicaSealedState with isSealed == false removes partition from list of sealed replicas
    helixParticipant.setReplicaSealedState(replicaId, false);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, listName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertFalse(sealedReplicas.contains(partitionIdStr));

    //Removing a replicaId that's already been removed doesn't hurt anything
    helixParticipant.setReplicaSealedState(replicaId, false);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, listName);

    //Removing all replicas yields expected behavior (and removal works by partitionId, not replicaId itself)
    dup = createMockAmbryReplica(partitionIdStr2);
    helixParticipant.setReplicaSealedState(dup, false);
    sealedReplicas = helixParticipant.getSealedReplicas();
    listIsExpectedSize(sealedReplicas, 0, listName);
  }

  /**
   * Tests setReplicaStoppedState method for {@link HelixParticipant}
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void testGetAndSetReplicaStoppedState() throws IOException, JSONException {
    //setup HelixParticipant, HelixParticipantDummy and dependencies
    Properties propsDummy = new Properties(props);
    propsDummy.setProperty("clustermap.host.name", "dummyHost");
    propsDummy.setProperty("clustermap.port", "2200");
    propsDummy.setProperty("clustermap.cluster.name", clusterName);
    propsDummy.setProperty("clustermap.datacenter.name", "DC0");
    propsDummy.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    String partitionId1 = "partitionId1";
    String partitionId2 = "partitionId2";
    String partitionId3 = "partitionId3";
    ReplicaId replicaId1 = createMockAmbryReplica(partitionId1);
    ReplicaId replicaId2 = createMockAmbryReplica(partitionId2);
    ReplicaId replicaId3 = createMockAmbryReplica(partitionId3);
    String hostname = "localhost";
    int port = 2200;
    String instanceName = ClusterMapUtils.getInstanceName(hostname, port);
    String instanceNameDummy = ClusterMapUtils.getInstanceName("dummyHost", 2200);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    ClusterMapConfig clusterMapConfigDummy = new ClusterMapConfig(new VerifiableProperties(propsDummy));
    HelixParticipant helixParticipant =
        new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
            getDefaultZkConnectStr(clusterMapConfig), true);
    HelixParticipant helixParticipantDummy =
        new HelixParticipant(clusterMapConfigDummy, helixManagerFactory, new MetricRegistry(),
            getDefaultZkConnectStr(clusterMapConfigDummy), true);
    HelixParticipant helixParticipantSpy = Mockito.spy(helixParticipant);
    helixParticipant.participate(Collections.emptyList());
    helixParticipantDummy.participate(Collections.emptyList());
    helixParticipantSpy.participate(Collections.emptyList());
    HelixManager helixManager = helixManagerFactory.getZKHelixManager(null, null, null, null);
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    InstanceConfig instanceConfig = new InstanceConfig("testInstanceId");
    helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    helixAdmin.setInstanceConfig(clusterName, instanceNameDummy, null);

    //Make sure the current stoppedReplicas list is non-null and empty
    List<String> stoppedReplicas = helixParticipant.getStoppedReplicas();
    assertEquals("stoppedReplicas list should be empty", Collections.emptyList(), stoppedReplicas);

    String listName = "stoppedReplicas list";

    //Check that invoking setReplicaStoppedState with a non-AmbryReplica ReplicaId throws an IllegalArgumentException
    ReplicaId nonAmbryReplica = createMockNotAmbryReplica(partitionId1);
    try {
      helixParticipant.setReplicaStoppedState(Arrays.asList(nonAmbryReplica), true);
      fail("Expected an IllegalArgumentException here");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    //Check that invoking setReplicaStoppedState with null instanceConfig
    try {
      helixParticipantDummy.setReplicaStoppedState(Arrays.asList(replicaId1), true);
      fail("Expected an IllegalStateException here");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    //Check that invoking setReplicaStoppedState adds the replicaId1, replicaId2 to the list of stopped replicas
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), true);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 2, listName);
    assertTrue(stoppedReplicas.contains(partitionId1));
    assertTrue(stoppedReplicas.contains(partitionId2));

    //Invoke setReplicaStoppedState to add replicaId1, replicaId2 again, ensure no more set operations performed on stopped list
    helixParticipantSpy.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), true);
    verify(helixParticipantSpy, never()).setStoppedReplicas(anyList());

    //Add replicaId1 again as well as replicaId3 to ensure new replicaId is correctly added and no duplicates in the stopped list
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId3), true);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 3, listName);
    assertTrue(stoppedReplicas.contains(partitionId1));
    assertTrue(stoppedReplicas.contains(partitionId2));
    assertTrue(stoppedReplicas.contains(partitionId3));

    //Check that invoking setReplicaStoppedState with markStop == false removes replicaId1, replicaId2 from stopped list
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), false);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 1, listName);
    assertTrue(stoppedReplicas.contains(partitionId3));
    assertFalse(stoppedReplicas.contains(partitionId1));
    assertFalse(stoppedReplicas.contains(partitionId2));

    //Removing replicaIds which have already been removed doesn't hurt anything and will not update InstanceConfig in Helix
    helixParticipantSpy.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), false);
    verify(helixParticipantSpy, never()).setStoppedReplicas(anyList());
    stoppedReplicas = helixParticipantSpy.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 1, listName);
    assertTrue(stoppedReplicas.contains(partitionId3));

    //Removing all replicas (including replica not in the list) yields expected behavior
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId2, replicaId3), false);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 0, listName);
  }

  /**
   * Test bad instantiation and initialization scenarios of the {@link HelixParticipant}
   */
  @Test
  public void testBadCases() {
    // Invalid state model def
    props.setProperty("clustermap.state.model.definition", "InvalidStateModelDef");
    try {
      new ClusterMapConfig(new VerifiableProperties(props));
      fail("should fail due to invalid state model definition");
    } catch (IllegalArgumentException e) {
      //expected and restore previous props
      props.setProperty("clustermap.state.model.definition", stateModelDef);
    }
    // Connect failure.
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManagerFactory.getHelixManager().beBad = true;
    HelixParticipant helixParticipant =
        new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
            getDefaultZkConnectStr(clusterMapConfig), true);
    try {
      helixParticipant.participate(Collections.emptyList());
      fail("Participation should have failed");
    } catch (IOException e) {
      // OK
    }

    // Bad param during instantiation.
    props.setProperty("clustermap.cluster.name", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
          getDefaultZkConnectStr(clusterMapConfig), true);
      fail("Instantiation should have failed");
    } catch (IllegalStateException e) {
      // OK
    }

    props.setProperty("clustermap.cluster.name", "HelixParticipantTestCluster");
    props.setProperty("clustermap.dcs.zk.connect.strings", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixClusterAgentsFactory(clusterMapConfig, new MetricRegistry()).getClusterParticipants();
      fail("Instantiation should have failed");
    } catch (IOException e) {
      // OK
    }
  }

  /**
   * Test instantiating multiple {@link HelixParticipant}(s) in {@link HelixClusterAgentsFactory}
   * @throws Exception
   */
  @Test
  public void testMultiParticipants() throws Exception {
    JSONArray zkInfosJson = new JSONArray();
    // create a new zkJson which contains two zk endpoints in the same data center.
    JSONObject zkInfoJson = new JSONObject();
    zkInfoJson.put(ClusterMapUtils.DATACENTER_STR, "DC0");
    zkInfoJson.put(ClusterMapUtils.DATACENTER_ID_STR, (byte) 0);
    zkInfoJson.put(ClusterMapUtils.ZKCONNECT_STR, "localhost:2199" + ZKCONNECT_STR_DELIMITER + "localhost:2299");
    zkInfosJson.put(zkInfoJson);
    JSONObject jsonObject = new JSONObject().put(ClusterMapUtils.ZKINFO_STR, zkInfosJson);
    props.setProperty("clustermap.dcs.zk.connect.strings", jsonObject.toString(2));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      List<ClusterParticipant> participants =
          new HelixClusterAgentsFactory(clusterMapConfig, helixManagerFactory).getClusterParticipants();
      assertEquals("Number of participants is not expected", 2, participants.size());
    } catch (Exception e) {
      throw e;
    } finally {
      // restore previous setup
      props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    }
  }

  /**
   * Test the good path of instantiation, initialization and termination of the {@link HelixParticipant}
   * @throws Exception
   */
  @Test
  public void testHelixParticipant() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant = new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
        getDefaultZkConnectStr(clusterMapConfig), true);
    participant.participate(Collections.emptyList());
    MockHelixManagerFactory.MockHelixManager helixManager = helixManagerFactory.getHelixManager();
    assertTrue(helixManager.isConnected());
    assertEquals(stateModelDef, helixManager.getStateModelDef());
    assertEquals(AmbryStateModelFactory.class, helixManager.getStateModelFactory().getClass());
    participant.close();
    assertFalse(helixManager.isConnected());
  }

  /**
   * Test both replica info addition and removal cases when updating node info in Helix cluster.
   * @throws Exception
   */
  @Test
  public void testUpdateNodeInfoInCluster() throws Exception {
    // test setup: 3 disks on local node, each disk has 3 replicas
    MockClusterMap clusterMap = new MockClusterMap(false, 1, 3, 3, false, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    ReplicaId existingReplica = localReplicas.get(0);
    // override some props for current test
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(true));
    props.setProperty("clustermap.port", String.valueOf(localNode.getPort()));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant = new HelixParticipant(clusterMapConfig, helixManagerFactory, new MetricRegistry(),
        getDefaultZkConnectStr(clusterMapConfig), true);
    // create InstanceConfig for local node. Also, put existing replica into sealed list
    List<String> sealedList = new ArrayList<>();
    sealedList.add(existingReplica.getPartitionId().toPathString());
    InstanceConfig instanceConfig = generateInstanceConfig(clusterMap, localNode, sealedList);
    HelixAdmin helixAdmin = participant.getHelixAdmin();
    helixAdmin.addCluster(clusterMapConfig.clusterMapClusterName);
    helixAdmin.addInstance(clusterMapConfig.clusterMapClusterName, instanceConfig);
    String instanceName = ClusterMapUtils.getInstanceName(localNode.getHostname(), localNode.getPort());
    // generate exactly same config for comparison
    InstanceConfig initialInstanceConfig = generateInstanceConfig(clusterMap, localNode, sealedList);
    // 1. add existing replica's info to Helix should be no-op
    assertTrue("Adding existing replica's info should succeed",
        participant.updateDataNodeInfoInCluster(existingReplica, true));
    assertEquals("InstanceConfig should stay unchanged", initialInstanceConfig,
        helixAdmin.getInstanceConfig(clusterMapConfig.clusterMapClusterName, instanceName));
    // create two new partitions on the same disk of local node
    PartitionId newPartition1 = clusterMap.createNewPartition(Collections.singletonList(localNode), 0);
    PartitionId newPartition2 = clusterMap.createNewPartition(Collections.singletonList(localNode), 0);
    // 2. add new partition2 (id = 10, replicaFromPartition2) into InstanceConfig
    ReplicaId replicaFromPartition2 = newPartition2.getReplicaIds().get(0);
    assertTrue("Adding new replica info into InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition2, true));
    // verify new added replica (id = 10, replicaFromPartition2) info is present in InstanceConfig
    instanceConfig = helixAdmin.getInstanceConfig(clusterMapConfig.clusterMapClusterName, instanceName);
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition2, true);
    // 3. add new partition1 (id = 9, replicaFromPartition1) into InstanceConfig
    ReplicaId replicaFromPartition1 = newPartition1.getReplicaIds().get(0);
    assertTrue("Adding new replica info into InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, true));
    // verify new added replica (id = 9, replicaFromPartition1) info is present in InstanceConfig
    instanceConfig = helixAdmin.getInstanceConfig(clusterMapConfig.clusterMapClusterName, instanceName);
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition1, true);
    // ensure previous added replica (id = 10, replicaFromPartition2) still exists
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition2, true);
    // 4. remove recently added new replica (id = 9, replicaFromPartition1)
    assertTrue("Removing replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, false));
    instanceConfig = helixAdmin.getInstanceConfig(clusterMapConfig.clusterMapClusterName, instanceName);
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition1, false);
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition2, true);
    // 5. remove same replica again (id = 9, replicaFromPartition1) should be no-op
    assertTrue("Removing non-found replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, false));
    // 6. remove an existing replica should succeed
    assertTrue("Removing replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(existingReplica, false));
    instanceConfig = helixAdmin.getInstanceConfig(clusterMapConfig.clusterMapClusterName, instanceName);
    verifyReplicaInfoInInstanceConfig(instanceConfig, existingReplica, false);
    verifyReplicaInfoInInstanceConfig(instanceConfig, replicaFromPartition2, true);
    // reset props
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(false));
    props.setProperty("clustermap.port", "2200");
  }

  /**
   * Get the default zk connect string in current dc. The default zk connect string is the first one (if there are
   * multiple strings associated with same dc) specified in clustermap config.
   * @param clusterMapConfig the {@link ClusterMapConfig} to parse default zk connect string associated with current dc.
   * @return the zk connection string representing the ZK service endpoint.
   */
  private String getDefaultZkConnectStr(ClusterMapConfig clusterMapConfig) {
    return parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
        clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0);
  }

  /**
   * Generate {@link InstanceConfig} for given data node.
   * @param clusterMap {@link MockClusterMap} to use
   * @param dataNode the data node associated with InstanceConfig.
   * @param sealedReplicas the sealed replicas that should be placed into sealed list. This can be null.
   * @return {@link InstanceConfig} of given data node.
   */
  private InstanceConfig generateInstanceConfig(MockClusterMap clusterMap, MockDataNodeId dataNode,
      List<String> sealedReplicas) {
    String instanceName = ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(dataNode.getHostname());
    instanceConfig.setPort(Integer.toString(dataNode.getPort()));
    instanceConfig.getRecord().setSimpleField(DATACENTER_STR, dataNode.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(RACKID_STR, dataNode.getRackId());
    instanceConfig.getRecord().setSimpleField(SCHEMA_VERSION_STR, Integer.toString(CURRENT_SCHEMA_VERSION));
    Map<String, SortedSet<ReplicaId>> mountPathToReplicas = new HashMap<>();
    for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
      mountPathToReplicas.computeIfAbsent(replicaId.getMountPath(),
          k -> new TreeSet<>(Comparator.comparing(ReplicaId::getPartitionId))).add(replicaId);
    }
    Map<String, Map<String, String>> mountPathToDiskInfos = new HashMap<>();
    for (Map.Entry<String, SortedSet<ReplicaId>> entry : mountPathToReplicas.entrySet()) {
      String mountPath = entry.getKey();
      StringBuilder replicaStrBuilder = new StringBuilder();
      DiskId diskId = null;
      for (ReplicaId replica : entry.getValue()) {
        replicaStrBuilder.append(replica.getPartitionId().toPathString())
            .append(REPLICAS_STR_SEPARATOR)
            .append(replica.getCapacityInBytes())
            .append(REPLICAS_STR_SEPARATOR)
            .append(replica.getPartitionId().getPartitionClass())
            .append(REPLICAS_DELIM_STR);
        diskId = replica.getDiskId();
      }
      Map<String, String> diskInfo = new HashMap<>();
      diskInfo.put(REPLICAS_STR, replicaStrBuilder.toString());
      diskInfo.put(DISK_CAPACITY_STR, String.valueOf(diskId.getRawCapacityInBytes()));
      diskInfo.put(DISK_STATE, AVAILABLE_STR);
      mountPathToDiskInfos.put(mountPath, diskInfo);
    }
    instanceConfig.getRecord().setMapFields(mountPathToDiskInfos);
    instanceConfig.getRecord()
        .setListField(ClusterMapUtils.SEALED_STR, sealedReplicas == null ? new ArrayList<>() : sealedReplicas);
    return instanceConfig;
  }

  /**
   * Verify updated {@link InstanceConfig}. If {@param shouldExist} is true, verify that replica is present in InstanceConfig.
   * If false, InstanceConfig should not contain given replica info.
   * @param instanceConfig the updated {@link InstanceConfig} to verify.
   * @param replicaId the replica whose info should/shouldn't exist in InstanceConfig
   * @param shouldExist whether given replica should exist in InstanceConfig.
   */
  private void verifyReplicaInfoInInstanceConfig(InstanceConfig instanceConfig, ReplicaId replicaId,
      boolean shouldExist) {
    Map<String, Map<String, String>> mountPathToDiskInfos = instanceConfig.getRecord().getMapFields();
    Map<String, String> diskInfo = mountPathToDiskInfos.get(replicaId.getMountPath());
    Set<String> replicasOnDisk = new HashSet<>();
    for (String replicaInfo : diskInfo.get(REPLICAS_STR).split(REPLICAS_DELIM_STR)) {
      replicasOnDisk.add(replicaInfo.split(REPLICAS_STR_SEPARATOR)[0]);
    }
    List<String> sealedList = getSealedReplicas(instanceConfig);
    List<String> stoppedList = getStoppedReplicas(instanceConfig);
    String partitionName = replicaId.getPartitionId().toPathString();
    if (shouldExist) {
      assertTrue("New replica is not found in InstanceConfig", replicasOnDisk.contains(partitionName));
    } else {
      assertFalse("Old replica should not exist in InstanceConfig", replicasOnDisk.contains(partitionName));
      // make sure the replica is not present in sealed/stopped list
      assertFalse("Old replica should not exist in sealed/stopped list",
          sealedList.contains(partitionName) || stoppedList.contains(partitionName));
    }
  }

  private ReplicaId createMockAmbryReplica(String partitionIdString) {
    return createMockReplicaId(partitionIdString, AmbryReplica.class, AmbryPartition.class);
  }

  private ReplicaId createMockNotAmbryReplica(String partitionIdString) {
    return createMockReplicaId(partitionIdString, ReplicaId.class, PartitionId.class);
  }

  private ReplicaId createMockReplicaId(String partitionIdString, Class<? extends ReplicaId> replicaClass,
      Class<? extends PartitionId> partitionClass) {
    ReplicaId replicaId = Mockito.mock(replicaClass);
    PartitionId partitionId = Mockito.mock(partitionClass);
    when(partitionId.toPathString()).thenReturn(partitionIdString);
    when(replicaId.getPartitionId()).thenReturn(partitionId);
    return replicaId;
  }

  private void listIsExpectedSize(List list, int expectedSize, String listName) {
    assertNotNull(listName + " is null", list);
    assertEquals(listName + " doesn't have the expected size " + expectedSize, expectedSize, list.size());
  }
}
