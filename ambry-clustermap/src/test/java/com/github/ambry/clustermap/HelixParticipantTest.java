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
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.lock.LockScope;
import org.apache.helix.lock.helix.HelixLockScope;
import org.apache.helix.lock.helix.ZKDistributedNonblockingLock;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.*;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.HelixAdminOperation.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link HelixParticipant}
 */
@RunWith(Parameterized.class)
public class HelixParticipantTest {
  private final MockHelixManagerFactory helixManagerFactory;
  private final Properties props;
  private final String stateModelDef;
  private final ZkInfo zkInfo;
  private final DataNodeConfigSourceType dataNodeConfigSourceType;
  private final ClusterMapConfig clusterMapConfig;
  private final PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter;
  private final InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter;
  private static final String clusterName = "HelixParticipantTestCluster";
  private static final String dcName = "DC0";
  private static final List<ZkInfo> zkInfoList = new ArrayList<>();
  private static final List<PropertyStoreToDataNodeConfigAdapter> adapters = new ArrayList<>();
  private static final int distributedLockLeaseTimeout = 2000; // 2 seconds
  private static JSONObject zkJson;
  private static TestHardwareLayout testHardwareLayout;
  private static TestPartitionLayout testPartitionLayout;
  private static String hardwareLayoutPath;
  private static String partitionLayoutPath;
  private static String zkLayoutPath;

  @BeforeClass
  public static void initialize() throws IOException {
    String tempDirPath = getTempDir("HelixParticipantTest-");
    zkInfoList.add(new ZkInfo("/tmp/" + tempDirPath, dcName, (byte) 0, 2199, true));
    System.out.println(tempDirPath);
    hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    testHardwareLayout = constructInitialHardwareLayoutJSON(clusterName);
    testPartitionLayout =
        constructInitialPartitionLayoutJSON(testHardwareLayout, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, null);
    zkJson = constructZkLayoutJSON(zkInfoList);
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, DataNodeConfigSourceType.INSTANCE_CONFIG},
            {ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, DataNodeConfigSourceType.PROPERTY_STORE},
            {ClusterMapConfig.AMBRY_STATE_MODEL_DEF, DataNodeConfigSourceType.INSTANCE_CONFIG},
            {ClusterMapConfig.AMBRY_STATE_MODEL_DEF, DataNodeConfigSourceType.PROPERTY_STORE}});
  }

  public HelixParticipantTest(String stateModelDef, DataNodeConfigSourceType dataNodeConfigSourceType)
      throws Exception {
    zkInfo = zkInfoList.get(0);
    props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", String.valueOf(testHardwareLayout.getRandomDataNodeFromDc(dcName).getPort()));
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dcName);
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.state.model.definition", stateModelDef);
    props.setProperty("clustermap.data.node.config.source.type", dataNodeConfigSourceType.name());
    props.setProperty("clustermap.enable.state.model.listener", "true");
    props.setProperty(ClusterMapConfig.DISTRIBUTED_LOCK_LEASE_TIMEOUT_IN_MS,
        String.valueOf(distributedLockLeaseTimeout));
    this.stateModelDef = stateModelDef;
    this.dataNodeConfigSourceType = dataNodeConfigSourceType;
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManagerFactory = new MockHelixManagerFactory();
    // This updates and verifies that the information in Helix is consistent with the one in the static cluster map.

    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "", "DC0",
        DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory(), false, stateModelDef,
        BootstrapCluster, dataNodeConfigSourceType, false, 1000);
    propertyStoreAdapter =
        dataNodeConfigSourceType == DataNodeConfigSourceType.PROPERTY_STORE ? new PropertyStoreToDataNodeConfigAdapter(
            "localhost:" + zkInfo.getPort(), clusterMapConfig) : null;
    if (propertyStoreAdapter != null) {
      adapters.add(propertyStoreAdapter);
    }
    instanceConfigConverter = new InstanceConfigToDataNodeConfigAdapter.Converter(clusterMapConfig);
  }

  @After
  public void clear() {
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    admin.dropCluster(clusterName);
    admin.close();
  }

  @AfterClass
  public static void destroy() {
    adapters.forEach(PropertyStoreToDataNodeConfigAdapter::close);
    for (ZkInfo zkInfo : zkInfoList) {
      zkInfo.shutdown();
    }
  }

  /**
   * Tests setReplicaSealedState method for {@link HelixParticipant}
   * @throws Exception
   */
  @Test
  public void testGetAndSetReplicaSealedState() {
    //setup HelixParticipant and dependencies
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);

    Set<String> localPartitionNames = new HashSet<>();
    dataNodeConfig.getDiskConfigs()
        .values()
        .forEach(diskConfig -> localPartitionNames.addAll(diskConfig.getReplicaConfigs().keySet()));
    String partitionIdStr = localPartitionNames.iterator().next();
    String partitionIdStr2 = localPartitionNames.stream().filter(p -> !p.equals(partitionIdStr)).findFirst().get();
    ReplicaId replicaId = createMockAmbryReplica(partitionIdStr);
    ReplicaId replicaId2 = createMockAmbryReplica(partitionIdStr2);

    //Make sure the current sealedReplicas list is empty
    List<String> sealedReplicas = helixParticipant.getSealedReplicas();
    List<String> partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    assertEquals("sealedReplicas should be empty", Collections.emptyList(), sealedReplicas);
    assertEquals("partiallySealedReplicas should be empty", Collections.emptyList(), partiallySealedReplicas);

    String sealedListName = "sealedReplicas";
    String partiallySealedListName = "partiallySealedReplicas";

    //Check that invoking setReplicaSealedState with a non-AmbryReplica ReplicaId throws an IllegalArgumentException
    ReplicaId notAmbryReplica = createMockNotAmbryReplica(partitionIdStr);
    try {
      helixParticipant.setReplicaSealedState(notAmbryReplica, ReplicaSealStatus.SEALED);
      fail("Expected an IllegalArgumentException here");
    } catch (IllegalArgumentException e) {
      //Expected exception
    }
    try {
      helixParticipant.setReplicaSealedState(notAmbryReplica, ReplicaSealStatus.PARTIALLY_SEALED);
      fail("Expected an IllegalArgumentException here");
    } catch (IllegalArgumentException e) {
      //Expected exception
    }

    //Check that invoking setReplicaSealedState adds the partition to the list of sealed replicas
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Seal another replicaId
    helixParticipant.setReplicaSealedState(replicaId2, ReplicaSealStatus.SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 2, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Check that sealed replica list doesn't take duplicates (and that dups are detected by partitionId comparison, not
    //replicaId object comparison
    ReplicaId dup = createMockAmbryReplica(partitionIdStr);
    helixParticipant.setReplicaSealedState(dup, ReplicaSealStatus.SEALED);
    helixParticipant.setReplicaSealedState(replicaId2, ReplicaSealStatus.SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 2, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertTrue(sealedReplicas.contains(partitionIdStr));

    //Check that invoking setReplicaSealedState with ReplicaSealStatus.NOT_SEALED removes partition from list of sealed replicas
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.NOT_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr2));
    assertFalse(sealedReplicas.contains(partitionIdStr));

    //Unsealing a replicaId that's already been unsealed doesn't hurt anything
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.NOT_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);

    //Unsealing all replicas yields expected behavior (and unseal works by partitionId, not replicaId itself)
    dup = createMockAmbryReplica(partitionIdStr2);
    helixParticipant.setReplicaSealedState(dup, ReplicaSealStatus.NOT_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 0, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);

    //Check that invoking setReplicaSealedState adds the partition to the list of partially sealed replicas
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.PARTIALLY_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 0, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 1, partiallySealedListName);
    assertTrue(partiallySealedReplicas.contains(partitionIdStr));

    //Partially Seal another replicaId
    helixParticipant.setReplicaSealedState(replicaId2, ReplicaSealStatus.PARTIALLY_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 0, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 2, partiallySealedListName);
    assertTrue(partiallySealedReplicas.contains(partitionIdStr2));
    assertTrue(partiallySealedReplicas.contains(partitionIdStr));

    //Seal one partially sealed replicaId
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 1, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr));
    assertTrue(partiallySealedReplicas.contains(partitionIdStr2));

    //Unseal one partially sealed replicaId
    helixParticipant.setReplicaSealedState(replicaId2, ReplicaSealStatus.NOT_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 1, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 0, partiallySealedListName);
    assertTrue(sealedReplicas.contains(partitionIdStr));

    // Partially seal a sealed replicaId
    helixParticipant.setReplicaSealedState(replicaId, ReplicaSealStatus.PARTIALLY_SEALED);
    sealedReplicas = helixParticipant.getSealedReplicas();
    partiallySealedReplicas = helixParticipant.getPartiallySealedReplicas();
    listIsExpectedSize(sealedReplicas, 0, sealedListName);
    listIsExpectedSize(partiallySealedReplicas, 1, partiallySealedListName);
    assertTrue(partiallySealedReplicas.contains(partitionIdStr));

    helixAdmin.close();
    helixParticipant.close();
  }

  /**
   * Tests setReplicaStoppedState method for {@link HelixParticipant}
   * @throws Exception
   */
  @Test
  public void testGetAndSetReplicaStoppedState() throws Exception {
    //setup HelixParticipant, HelixParticipantDummy and dependencies
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);

    //Make sure the current stoppedReplicas list is empty
    List<String> stoppedReplicas = helixParticipant.getStoppedReplicas();
    assertEquals("stoppedReplicas list should be empty", Collections.emptyList(), stoppedReplicas);

    String listName = "stoppedReplicas list";
    Set<String> localPartitionNames = new HashSet<>();
    dataNodeConfig.getDiskConfigs()
        .values()
        .forEach(diskConfig -> localPartitionNames.addAll(diskConfig.getReplicaConfigs().keySet()));
    String[] partitionIds = new String[3];
    for (int i = 0; i < partitionIds.length; ++i) {
      partitionIds[i] = localPartitionNames.iterator().next();
      localPartitionNames.remove(partitionIds[i]);
    }
    ReplicaId replicaId1 = createMockAmbryReplica(partitionIds[0]);
    ReplicaId replicaId2 = createMockAmbryReplica(partitionIds[1]);
    ReplicaId replicaId3 = createMockAmbryReplica(partitionIds[2]);
    //Check that invoking setReplicaStoppedState with a non-AmbryReplica ReplicaId throws an IllegalArgumentException
    ReplicaId nonAmbryReplica = createMockNotAmbryReplica(partitionIds[1]);
    try {
      helixParticipant.setReplicaStoppedState(Collections.singletonList(nonAmbryReplica), true);
      fail("Expected an IllegalArgumentException here");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    //Check that invoking setReplicaStoppedState adds the replicaId1, replicaId2 to the list of stopped replicas
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), true);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 2, listName);
    assertTrue(stoppedReplicas.contains(replicaId1.getPartitionId().toPathString()));
    assertTrue(stoppedReplicas.contains(replicaId2.getPartitionId().toPathString()));

    //Invoke setReplicaStoppedState to add replicaId1, replicaId2 again, should be no-op
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), true);
    listIsExpectedSize(helixParticipant.getStoppedReplicas(), 2, listName);

    //Add replicaId1 again as well as replicaId3 to ensure new replicaId is correctly added and no duplicates in the stopped list
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId3), true);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 3, listName);
    assertTrue(stoppedReplicas.contains(replicaId1.getPartitionId().toPathString()));
    assertTrue(stoppedReplicas.contains(replicaId2.getPartitionId().toPathString()));
    assertTrue(stoppedReplicas.contains(replicaId3.getPartitionId().toPathString()));

    //Check that invoking setReplicaStoppedState with markStop == false removes replicaId1, replicaId2 from stopped list
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), false);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 1, listName);
    assertTrue(stoppedReplicas.contains(replicaId3.getPartitionId().toPathString()));
    assertFalse(stoppedReplicas.contains(replicaId2.getPartitionId().toPathString()));
    assertFalse(stoppedReplicas.contains(replicaId1.getPartitionId().toPathString()));

    //Removing replicaIds which have already been removed doesn't hurt anything and will not update InstanceConfig in Helix
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId1, replicaId2), false);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 1, listName);
    assertTrue(stoppedReplicas.contains(replicaId3.getPartitionId().toPathString()));

    //Removing all replicas (including replica not in the list) yields expected behavior
    helixParticipant.setReplicaStoppedState(Arrays.asList(replicaId2, replicaId3), false);
    stoppedReplicas = helixParticipant.getStoppedReplicas();
    listIsExpectedSize(stoppedReplicas, 0, listName);
    helixAdmin.close();
    helixParticipant.close();
  }

  /**
   * Test bad instantiation and initialization scenarios of the {@link HelixParticipant}
   */
  @Test
  public void testBadCases() {
    // Invalid state model def
    props.setProperty("clustermap.state.model.definition", "InvalidStateModelDef");
    props.setProperty(ClusterMapConfig.ENABLE_PROPERTY_STORE_CLEAN_UP_TASK, Boolean.toString(false));
    try {
      new ClusterMapConfig(new VerifiableProperties(props));
      fail("should fail due to invalid state model definition");
    } catch (IllegalArgumentException e) {
      //expected and restore previous props
      props.setProperty("clustermap.state.model.definition", stateModelDef);
    }
    // Connect failure.
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManagerFactory.getHelixManager(InstanceType.PARTICIPANT).beBad = true;
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, helixManagerFactory,
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    try {
      helixParticipant.participate();
      helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
      fail("Participation should have failed");
    } catch (IOException e) {
      // OK
    }

    // Bad param during instantiation.
    props.setProperty("clustermap.cluster.name", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, helixManagerFactory, new MetricRegistry(),
          getDefaultZkConnectStr(clusterMapConfig), true);
      fail("Instantiation should have failed");
    } catch (IllegalStateException e) {
      // OK
    }

    props.setProperty("clustermap.cluster.name", "HelixParticipantTestCluster");
    props.setProperty("clustermap.dcs.zk.connect.strings", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixClusterAgentsFactoryWithMockClusterMap(clusterMapConfig, new MetricRegistry()).getClusterParticipants();
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
    assumeTrue(dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG);
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
      List<ClusterParticipant> participants = new HelixClusterAgentsFactoryWithMockClusterMap(clusterMapConfig,
          helixManagerFactory).getClusterParticipants();
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
    props.setProperty(ClusterMapConfig.ENABLE_PROPERTY_STORE_CLEAN_UP_TASK, Boolean.toString(false));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, helixManagerFactory,
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    assertTrue(helixManagerFactory.getHelixManager(InstanceType.SPECTATOR).isConnected());
    assertFalse(helixManagerFactory.getHelixManager(InstanceType.PARTICIPANT).isConnected());

    participant.participate();
    participant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    MockHelixManagerFactory.MockHelixManager helixManager =
        helixManagerFactory.getHelixManager(InstanceType.PARTICIPANT);
    assertTrue(helixManager.isConnected());
    assertEquals(stateModelDef, helixManager.getStateModelDef());
    assertEquals(AmbryStateModelFactory.class, helixManager.getStateModelFactory().getClass());
    participant.close();
    assertFalse(helixManager.isConnected());
  }

  /**
   * Tests whether the correct metrics that are getting logged for state transitions
   * and correct clearance of metrics
   * @throws Exception exception
   */
  @Test
  public void testHelixParticipantPartitionStateTransitionMetrics() throws Exception {
    props.setProperty("clustermap.enable.partition.state.transition.metrics", "true");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, helixManagerFactory,
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    HelixParticipantMetrics metrics = participant.participantMetrics;
    metrics.incStateTransitionMetric("test-1", ReplicaState.BOOTSTRAP, ReplicaState.STANDBY);
    assertNotNull(metrics.partitionTransitionToCount.get("State-Transition-Partition-test-1-from-BOOTSTRAP-to-STANDBY"));

    assertNull(metrics.partitionTransitionToCount.get(
        "State-Transition-Partition-test-1-from-" + ReplicaState.STANDBY.name() + "-to-" + ReplicaState.LEADER.name()));

    metrics.incStateTransitionMetric("test-1", ReplicaState.STANDBY, ReplicaState.LEADER);
    assertNotNull(metrics.partitionTransitionToCount.get(
        "State-Transition-Partition-test-1-from-" + ReplicaState.STANDBY.name() + "-to-" + ReplicaState.LEADER.name()));

    metrics.incStateTransitionMetric("test-2", ReplicaState.BOOTSTRAP, ReplicaState.STANDBY);
    assertNotNull(metrics.partitionTransitionToCount.get(
        "State-Transition-Partition-test-2-from-" + ReplicaState.BOOTSTRAP.name() + "-to-" + ReplicaState.STANDBY.name()));

    metrics.clearStateTransitionMetric("test-1");
    assertNull(metrics.partitionTransitionToCount.get("Partition-test-1-from-BOOTSTRAP-to-STANDBY"));
    assertNotNull(metrics.partitionTransitionToCount.get(
        "State-Transition-Partition-test-2-from-" + ReplicaState.BOOTSTRAP.name() + "-to-" + ReplicaState.STANDBY.name()));

    metrics.clearStateTransitionMetric("test-2");
    assertNull(metrics.partitionTransitionToCount.get(
        "State-Transition-Partition-test-2-from-" + ReplicaState.BOOTSTRAP.name() + "-to-" + ReplicaState.STANDBY.name()));
  }

  /**
   * Test both replica info addition and removal cases when updating node info in Helix cluster.
   * @throws Exception
   */
  @Test
  public void testUpdateNodeInfoInCluster() throws Exception {
    // override some props for current test
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(true));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    participant.markDisablePartitionComplete();
    // create InstanceConfig for local node. Also, put existing replica into sealed list
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixAdmin helixAdmin = participant.getHelixAdmin();
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);

    DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().values().iterator().next();
    String existingReplicaName = diskConfig.getReplicaConfigs().keySet().iterator().next();
    PartitionId correspondingPartition = testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .stream()
        .filter(p -> p.toPathString().equals(existingReplicaName))
        .findFirst()
        .get();
    ReplicaId existingReplica = correspondingPartition.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getPort() == clusterMapConfig.clusterMapPort)
        .findFirst()
        .get();

    // generate exactly same config for comparison
    DataNodeConfig initialDataNodeConfig = deepCopyDataNodeConfig(dataNodeConfig);
    // 1. add existing replica's info to Helix should be no-op
    assertTrue("Adding existing replica's info should succeed",
        participant.updateDataNodeInfoInCluster(existingReplica, true));
    assertEquals("DataNodeConfig should stay unchanged", initialDataNodeConfig,
        getDataNodeConfigInHelix(helixAdmin, instanceName));
    // create two new replicas on the same disk of local node
    int currentPartitionCount = testPartitionLayout.getPartitionCount();
    Partition newPartition1 = new Partition(currentPartitionCount++, DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE,
        testPartitionLayout.replicaCapacityInBytes);
    Partition newPartition2 = new Partition(currentPartitionCount, DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE,
        testPartitionLayout.replicaCapacityInBytes);
    Disk disk = (Disk) existingReplica.getDiskId();
    // 2. add new partition2 (id = 10, replicaFromPartition2) to Helix
    ReplicaId replicaFromPartition2 = new Replica(newPartition2, disk, clusterMapConfig);
    assertTrue("Adding new replica info to Helix should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition2, true));
    // verify new added replica (replicaFromPartition2) info is present in DataNodeConfig
    Thread.sleep(50);
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition2, true);
    // 3. add new partition1 (replicaFromPartition1) into InstanceConfig
    ReplicaId replicaFromPartition1 = new Replica(newPartition1, disk, clusterMapConfig);
    assertTrue("Adding new replica info into InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, true));
    Thread.sleep(50);
    // verify new added replica (replicaFromPartition1) info is present in InstanceConfig
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition1, true);
    // ensure previous added replica (replicaFromPartition2) still exists
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition2, true);
    // 4. remove recently added new replica (replicaFromPartition1)
    assertTrue("Removing replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, false));
    Thread.sleep(50);
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition1, false);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition2, true);
    // 5. remove same replica again (id = 9, replicaFromPartition1) should be no-op
    assertTrue("Removing non-found replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition1, false));
    // 6. remove recently added new replica (replicaFromPartition2)
    assertTrue("Removing replica info from InstanceConfig should succeed.",
        participant.updateDataNodeInfoInCluster(replicaFromPartition2, false));
    Thread.sleep(50);
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, replicaFromPartition2, false);
    verifyReplicaInfoInDataNodeConfig(dataNodeConfig, existingReplica, true);
    // reset props
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(false));
    participant.close();
  }

  @Test
  public void testRemoveReplicasFromDataNode() throws Exception {
    assumeTrue(dataNodeConfigSourceType == DataNodeConfigSourceType.PROPERTY_STORE);

    props.setProperty("clustermap.update.datanode.info", Boolean.toString(true));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    participant.markDisablePartitionComplete();
    // create InstanceConfig for local node. Also, put existing replica into sealed list
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixAdmin helixAdmin = participant.getHelixAdmin();
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);

    ClusterMap cm = new StaticClusterManager(testPartitionLayout.partitionLayout, dcName, new MetricRegistry());
    List<? extends ReplicaId> replicaIds =
        cm.getReplicaIds(cm.getDataNodeId("localhost", clusterMapConfig.clusterMapPort));
    DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().values().iterator().next();
    List<ReplicaId> replicaIdsOnDisk = replicaIds.stream()
        .filter(replicaId -> diskConfig.getReplicaConfigs().containsKey(replicaId.getPartitionId().toPathString()))
        .collect(Collectors.toList());
    List<ReplicaId> otherReplicas = new ArrayList<>(replicaIds);
    otherReplicas.removeAll(replicaIdsOnDisk);
    // The case to remove empty list
    try {
      participant.removeReplicasFromDataNode(null);
      fail("Null list should fail");
    } catch (Exception e) {
    }
    assertTrue("Remove an empty list of replicas should succeed",
        participant.removeReplicasFromDataNode(Collections.emptyList()));

    // The case to remove all the replicas in one disk
    assertTrue(participant.removeReplicasFromDataNode(replicaIdsOnDisk));
    Thread.sleep(50);
    replicaIdsOnDisk.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            false));
    otherReplicas.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            true));

    // The case to remove the replicas again
    assertTrue(participant.removeReplicasFromDataNode(replicaIdsOnDisk));
    Thread.sleep(50);
    replicaIdsOnDisk.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            false));
    otherReplicas.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            true));

    // Add replicas on another disk to the list
    DataNodeConfig.DiskConfig anotherDiskConfig =
        dataNodeConfig.getDiskConfigs().values().stream().filter(dc -> !dc.equals(diskConfig)).findFirst().get();
    replicaIdsOnDisk.addAll(replicaIds.stream()
        .filter(
            replicaId -> anotherDiskConfig.getReplicaConfigs().containsKey(replicaId.getPartitionId().toPathString()))
        .collect(Collectors.toList()));
    otherReplicas.removeAll(replicaIdsOnDisk);

    // The case to remove some replicas that are already removed and some new replicas
    assertTrue(participant.removeReplicasFromDataNode(replicaIdsOnDisk));
    Thread.sleep(50);
    replicaIdsOnDisk.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            false));
    otherReplicas.forEach(
        replicaId -> verifyReplicaInfoInDataNodeConfig(getDataNodeConfigInHelix(helixAdmin, instanceName), replicaId,
            true));

    participant.close();
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(false));
  }

  /**
   * Test updateDiskCapacity method
   * @throws Exception
   */
  @Test
  public void testUpdateDiskCapacity() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    HelixAdmin helixAdmin = helixParticipant.getHelixAdmin();
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    // By default, there is no disk capacity
    assertFalse(instanceConfig.getInstanceCapacityMap().containsKey(HelixParticipant.DISK_KEY));
    try {
      helixParticipant.updateDiskCapacity(-1);
      fail("Should fail on negative number");
    } catch (IllegalArgumentException e) {
    }
    long updateCount = helixParticipant.participantMetrics.updateDiskCapacityCounter.getCount();
    assertTrue(helixParticipant.updateDiskCapacity(1000));
    assertEquals(updateCount + 1, helixParticipant.participantMetrics.updateDiskCapacityCounter.getCount());
    instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    assertTrue(instanceConfig.getInstanceCapacityMap().containsKey(HelixParticipant.DISK_KEY));
    assertEquals(1000, instanceConfig.getInstanceCapacityMap().get(HelixParticipant.DISK_KEY).intValue());

    // update with the same capacity again, this time we shouldn't update it.
    updateCount++;
    assertTrue(helixParticipant.updateDiskCapacity(1000));
    assertEquals(updateCount, helixParticipant.participantMetrics.updateDiskCapacityCounter.getCount());
    helixParticipant.close();
  }

  /**
   * Test setting disk state in property store.
   * @throws Exception
   */
  @Test
  public void testSetDiskState() throws Exception {
    assumeTrue(dataNodeConfigSourceType == DataNodeConfigSourceType.PROPERTY_STORE);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    HelixAdmin helixAdmin = helixParticipant.getHelixAdmin();
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    // by default, all disks are available
    List<String> mountPaths = new ArrayList<>();
    for (Map.Entry<String, DataNodeConfig.DiskConfig> entry : dataNodeConfig.getDiskConfigs().entrySet()) {
      mountPaths.add(entry.getKey());
      assertTrue(entry.getValue().getState() == HardwareState.AVAILABLE);
    }
    AmbryDataNode dataNode = mock(AmbryDataNode.class);
    List<DiskId> disks = new ArrayList<>();
    for (String mountPath : mountPaths) {
      disks.add(new AmbryDisk(clusterMapConfig, dataNode, mountPath, HardwareState.UNAVAILABLE,
          MIN_DISK_CAPACITY_IN_BYTES * 10));
    }
    DiskId failDisk = mock(MockDiskId.class);
    DiskId missingMountDisk = new AmbryDisk(clusterMapConfig, dataNode, "/missingpath", HardwareState.AVAILABLE,
        MIN_DISK_CAPACITY_IN_BYTES * 10);

    // Test some failure cases
    try {
      helixParticipant.setDisksState(null, HardwareState.UNAVAILABLE);
      fail("Empty list should fail");
    } catch (Exception e) {
    }
    try {
      helixParticipant.setDisksState(Collections.emptyList(), HardwareState.UNAVAILABLE);
      fail("Empty list should fail");
    } catch (Exception e) {
    }
    try {
      helixParticipant.setDisksState(Collections.singletonList(failDisk), HardwareState.UNAVAILABLE);
      fail("Disk is not right");
    } catch (Exception e) {
    }
    try {
      helixParticipant.setDisksState(Collections.singletonList(missingMountDisk), HardwareState.UNAVAILABLE);
      fail("Mount path is missing");
    } catch (Exception e) {
    }

    // Nothing is changed
    assertTrue(helixParticipant.setDisksState(disks, HardwareState.AVAILABLE));
    Thread.sleep(50);
    // All the disks are changed
    assertTrue(helixParticipant.setDisksState(disks, HardwareState.UNAVAILABLE));
    Thread.sleep(50);
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    for (Map.Entry<String, DataNodeConfig.DiskConfig> entry : dataNodeConfig.getDiskConfigs().entrySet()) {
      assertTrue(entry.getValue().getState() == HardwareState.UNAVAILABLE);
    }
    // change it back to available
    assertTrue(helixParticipant.setDisksState(disks, HardwareState.AVAILABLE));
    Thread.sleep(50);
    dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    for (Map.Entry<String, DataNodeConfig.DiskConfig> entry : dataNodeConfig.getDiskConfigs().entrySet()) {
      assertTrue(entry.getValue().getState() == HardwareState.AVAILABLE);
    }
    helixParticipant.close();
  }

  /**
   * Test setting disk order in property store.
   * @throws Exception
   */
  @Test
  public void testSetDisksOrder() throws Exception {
    assumeTrue(dataNodeConfigSourceType == DataNodeConfigSourceType.PROPERTY_STORE);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(),
            new MetricRegistry(), getDefaultZkConnectStr(clusterMapConfig), true);
    HelixAdmin helixAdmin = helixParticipant.getHelixAdmin();
    DataNodeConfig dataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);

    // First test some basic failure cases.
    try {
      helixParticipant.setDisksOrder(null);
      fail("null input map should fail");
    } catch (Exception e) {
    }
    try {
      helixParticipant.setDisksOrder(Collections.emptyMap());
      fail("Empty input map should fail");
    } catch (Exception e) {
    }

    DiskId oldDisk = mock(DiskId.class);
    DiskId newDisk = mock(DiskId.class);
    when(oldDisk.getMountPath()).thenReturn("/mnt0");
    when(newDisk.getMountPath()).thenReturn("/mnt1");
    Map<DiskId, DiskId> newDiskMapping = new HashMap<>();
    newDiskMapping.put(oldDisk, newDisk);
    newDiskMapping.put(newDisk, oldDisk);

    // Get a copy of Helix' disk configs before attempting to update.
    Map<String, DataNodeConfig.DiskConfig> initialDiskConfigs = dataNodeConfig.getDiskConfigs();
    Set<String> initialReplicasMount0 = initialDiskConfigs.get("/mnt0").getReplicaConfigs().keySet();
    Set<String> initialReplicasMount1 = initialDiskConfigs.get("/mnt1").getReplicaConfigs().keySet();

    // Update the disk order in Helix
    assertTrue(helixParticipant.setDisksOrder(newDiskMapping));
    Thread.sleep(50);

    // Now read the new disk order back from Helix and verify that the desired swap has been persisted.
    DataNodeConfig updatedDataNodeConfig = getDataNodeConfigInHelix(helixAdmin, instanceName);
    Map<String, DataNodeConfig.DiskConfig> updatedDiskConfigs = updatedDataNodeConfig.getDiskConfigs();
    Set<String> updatedReplicasMount0 = updatedDiskConfigs.get("/mnt0").getReplicaConfigs().keySet();
    Set<String> updatedReplicasMount1 = updatedDiskConfigs.get("/mnt1").getReplicaConfigs().keySet();
    assertTrue(initialReplicasMount0.equals(updatedReplicasMount1));
    assertTrue(initialReplicasMount1.equals(updatedReplicasMount0));
  }

  /**
   * Test participate method, which triggers state transition.
   * @throws Exception
   */
  @Test
  public void testStateModelParticipation() throws Exception {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    // participate
    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    HelixManager manager = helixParticipant.getHelixManager();
    StateModelFactory<? extends StateModel> factory =
        manager.getStateMachineEngine().getStateModelFactory(ClusterMapConfig.AMBRY_STATE_MODEL_DEF);
    assertTrue(factory instanceof AmbryStateModelFactory);

    ClusterMap cm = new StaticClusterManager(testPartitionLayout.partitionLayout, dcName, new MetricRegistry());
    List<? extends ReplicaId> replicaIds =
        cm.getReplicaIds(cm.getDataNodeId("localhost", clusterMapConfig.clusterMapPort));
    String resource = "10000"; // There is only one resource
    // send state transition messages, this is controller logic, just put it down here to make sure the pipeline
    // of state transition works
    sendStateTransitionMessages(manager, resource, replicaIds, "OFFLINE", "BOOTSTRAP");
    // sleep some time so the state transition can happen
    Thread.sleep(1000);
    // have to get offline to refresh the cache?
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("bootstrap", metricRegistry));

    sendStateTransitionMessages(manager, resource, replicaIds, "BOOTSTRAP", "STANDBY");
    Thread.sleep(500);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("standby", metricRegistry));

    sendStateTransitionMessages(manager, resource, replicaIds, "STANDBY", "LEADER");
    Thread.sleep(500);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("leader", metricRegistry));

    sendStateTransitionMessages(manager, resource, replicaIds, "LEADER", "STANDBY");
    Thread.sleep(500);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("standby", metricRegistry));

    sendStateTransitionMessages(manager, resource, replicaIds, "STANDBY", "INACTIVE");
    Thread.sleep(500);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("inactive", metricRegistry));

    sendStateTransitionMessages(manager, resource, replicaIds, "INACTIVE", "OFFLINE");
    Thread.sleep(500);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("offline", metricRegistry));

    helixParticipant.close();
  }


  @Test
  public void testEnableStatePostParticipate() throws IOException {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    HelixManager manager = helixParticipant.getHelixManager();

    assertEquals(manager.getInstanceType(), InstanceType.PARTICIPANT);
    assertNotNull("HelixManager cannot be null", manager);

    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);

    // Without cloud config, instances should still be in ENABLE state
    assertEquals(InstanceConstants.InstanceOperation.ENABLE,
        manager.getConfigAccessor().getInstanceConfig(clusterName, manager.getInstanceName()).getInstanceOperation().getOperation());

    helixParticipant.close();
  }

  /**
   * Test participate method with property store helix task enabled
   * @throws Exception
   */
  @Test
  public void testParticipateMethodWithHelixPropertyStoreTaskEnabled() throws Exception {
    props.setProperty(ClusterMapConfig.ENABLE_PROPERTY_STORE_CLEAN_UP_TASK, Boolean.toString(true));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    assertNotNull("PropertyStoreCleanUpTask should be registered",
        helixParticipant.getHelixManager().getStateMachineEngine()
            .getStateModelFactory(TaskConstants.STATE_MODEL_NAME));
    helixParticipant.close();

  }

  /**
   * Test participate method with property store helix task disabled
   * @throws Exception
   */
  @Test
  public void testParticipateMethodWithHelixPropertyStoreTaskDisabled() throws Exception {
    props.setProperty(ClusterMapConfig.ENABLE_PROPERTY_STORE_CLEAN_UP_TASK, Boolean.toString(false));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    assertNull("PropertyStoreCleanUpTask should not be registered",
        helixParticipant.getHelixManager().getStateMachineEngine()
            .getStateModelFactory(TaskConstants.STATE_MODEL_NAME));
    helixParticipant.close();
  }

  /**
   * Test reset partition state methods
   * @throws Exception
   */
  @Test
  public void testResetPartitions() throws Exception {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    // Mock a state change listener to throw an exception
    PartitionStateChangeListener listener = mock(PartitionStateChangeListener.class);
    doThrow(new StateTransitionException("error", StateTransitionException.TransitionErrorCode.BootstrapFailure)).when(
        listener).onPartitionBecomeBootstrapFromOffline(anyString());
    helixParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener, listener);

    // participate
    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    HelixManager manager = helixParticipant.getHelixManager();
    ClusterMap cm = new StaticClusterManager(testPartitionLayout.partitionLayout, dcName, new MetricRegistry());
    List<? extends ReplicaId> replicaIds =
        cm.getReplicaIds(cm.getDataNodeId("localhost", clusterMapConfig.clusterMapPort));
    String resource = "10000"; // There is only one resource
    // send state transition messages, this is controller logic, just put it down here to make sure the pipeline
    // of state transition works
    sendStateTransitionMessages(manager, resource, replicaIds, "OFFLINE", "BOOTSTRAP");
    // sleep some time so the state transition can happen
    Thread.sleep(1000);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    // All replicas should at error state now
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("error", metricRegistry));

    // reset one partition
    assertTrue(helixParticipant.resetPartitionState("" + replicaIds.get(0).getPartitionId().getId()));
    Thread.sleep(500);
    assertEquals(1, getNumberOfReplicaInStateFromMetric("offline", metricRegistry));
    assertEquals(replicaIds.size() - 1, getNumberOfReplicaInStateFromMetric("error", metricRegistry));

    // reset remaining partitions
    List<String> remainingPartitions = replicaIds.subList(1, replicaIds.size())
        .stream()
        .map(ReplicaId::getPartitionId)
        .map(PartitionId::getId)
        .map(String::valueOf)
        .collect(Collectors.toList());
    assertTrue(helixParticipant.resetPartitionState(remainingPartitions));
    Thread.sleep(500);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("offline", metricRegistry));
    assertEquals(0, getNumberOfReplicaInStateFromMetric("error", metricRegistry));

    helixParticipant.close();
  }

  /**
   * Test distributed lock with same zookeeper connection
   * @throws Exception
   */
  @Test
  public void testDistributedLock() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);

    // By default, helix library would use shared zookeeper connection
    DistributedLock lock1 = helixParticipant.getDistributedLock("TestDistributedLock", "for testing");
    DistributedLock lock2 = helixParticipant.getDistributedLock("TestDistributedLock", "for testing");
    testTwoDistributedLocks(lock1, lock2);
    helixParticipant.close();
  }

  /**
   * Test distributed lock with different zookeeper connections
   * @throws Exception
   */
  @Test
  public void testDistributedLockWithDifferentConnection() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    String zkConnectionStr = getDefaultZkConnectStr(clusterMapConfig);
    DistributedLock lock1 =
        getDistributedLockWithDedicatedZkClient("Test", clusterMapConfig.clusterMapHostName + "1", "for testing",
            zkConnectionStr);
    // use different user id
    DistributedLock lock2 =
        getDistributedLockWithDedicatedZkClient("Test", clusterMapConfig.clusterMapHostName + "2", "for testing",
            zkConnectionStr);
    testTwoDistributedLocks(lock1, lock2);
  }

  /**
   * Test maintenance mode
   * @throws Exception
   */
  @Test
  public void testMaintenanceMode() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);
    String newPort = null;
    while (newPort == null || props.getProperty("clustermap.port").equals(newPort)) {
      newPort = String.valueOf(testHardwareLayout.getRandomDataNodeFromDc(dcName).getPort());
    }
    Properties props2 = new Properties();
    props2.setProperty("clustermap.host.name", props.getProperty("clustermap.host.name"));
    props2.setProperty("clustermap.port", newPort);
    props2.setProperty("clustermap.cluster.name", clusterName);
    props2.setProperty("clustermap.datacenter.name", dcName);
    props2.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props2.setProperty("clustermap.state.model.definition", stateModelDef);
    props2.setProperty("clustermap.data.node.config.source.type", dataNodeConfigSourceType.name());
    props2.setProperty("clustermap.enable.state.model.listener", "true");
    props2.setProperty(ClusterMapConfig.DISTRIBUTED_LOCK_LEASE_TIMEOUT_IN_MS,
        String.valueOf(distributedLockLeaseTimeout));
    ClusterMapConfig clusterMapConfig2 = new ClusterMapConfig(new VerifiableProperties(props2));
    HelixParticipant helixParticipant2 =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig2, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig2), true);
    // Calling participate so manager can connect to zookeeper
    helixParticipant.participate();
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    helixParticipant2.participate();
    helixParticipant2.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);

    try {
      helixParticipant.exitMaintenanceMode();
      fail("Participant not in maintenance mode");
    } catch (Exception e) {
    }
    assertTrue("First participant should enter maintenance mode", helixParticipant.enterMaintenanceMode("ForTesting"));
    assertFalse("Second participant should fail at entering maintenance mode",
        helixParticipant2.enterMaintenanceMode("ForTesting2"));

    HelixParticipant.MaintenanceRecord record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("ENTER", record.operationType);
    assertEquals("ForTesting", record.reason);

    assertTrue("First participant should exit maintenance mode", helixParticipant.exitMaintenanceMode());
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("EXIT", record.operationType);
    assertEquals("ForTesting", record.reason);

    assertTrue("Second participant should enter maintenance mode",
        helixParticipant2.enterMaintenanceMode("ForTesting2"));
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("ENTER", record.operationType);
    assertEquals("ForTesting2", record.reason);

    assertTrue("Second participant should exit maintenance mode", helixParticipant2.exitMaintenanceMode());
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("EXIT", record.operationType);
    assertEquals("ForTesting2", record.reason);

    // Enter MM again, but this time, other application might enter MM as well
    assertTrue("First participant should enter maintenance mode", helixParticipant.enterMaintenanceMode("ForTesting"));
    helixParticipant2.getHelixAdmin().manuallyEnableMaintenanceMode(clusterName, true, "OVERRIDE", null);
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("ENTER", record.operationType);
    assertEquals("OVERRIDE", record.reason);
    assertTrue("First participant should exit maintenance mode without calling helix participant",
        helixParticipant.exitMaintenanceMode());
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("ENTER", record.operationType);
    assertEquals("OVERRIDE", record.reason);
    helixParticipant2.getHelixAdmin().manuallyEnableMaintenanceMode(clusterName, false, "OVERRIDE", null);

    // Enter MM again, but this time, other application would exit
    assertTrue("First participant should enter maintenance mode", helixParticipant.enterMaintenanceMode("ForTesting"));
    helixParticipant2.getHelixAdmin().manuallyEnableMaintenanceMode(clusterName, true, "OVERRIDE", null);
    helixParticipant2.getHelixAdmin().manuallyEnableMaintenanceMode(clusterName, false, "OVERRIDE", null);
    record = helixParticipant.getLastMaintenanceRecord();
    assertEquals("EXIT", record.operationType);
    assertEquals("OVERRIDE", record.reason);
    assertTrue("First participant should exit maintenance mode without calling helix participant",
        helixParticipant.exitMaintenanceMode());

    helixParticipant.close();
    helixParticipant2.close();
  }

  @Test
  public void testOfflineToBootstrapWithDelayedStateModelRegistration() throws Exception {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            getDefaultZkConnectStr(clusterMapConfig), true);

    // participate
    helixParticipant.participate();
    HelixManager manager = helixParticipant.getHelixManager();

    ClusterMap cm = new StaticClusterManager(testPartitionLayout.partitionLayout, dcName, new MetricRegistry());
    List<? extends ReplicaId> replicaIds =
        cm.getReplicaIds(cm.getDataNodeId("localhost", clusterMapConfig.clusterMapPort));
    String resource = "10000"; // There is only one resource
    // send state transition messages, this is controller logic, just put it down here to make sure the pipeline
    // of state transition works
    sendStateTransitionMessages(manager, resource, replicaIds, "OFFLINE", "BOOTSTRAP");
    // sleep some time
    Thread.sleep(1000);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(0, getNumberOfReplicaInStateFromMetric("bootstrap", metricRegistry));
    // register state machine model
    helixParticipant.registerTasksWithStateMachineModel(Collections.emptyList(), null, null);
    // sleep some time so the state transition can happen
    Thread.sleep(1000);
    // send transition messages again
    sendStateTransitionMessages(manager, resource, replicaIds, "OFFLINE", "BOOTSTRAP");
    StateModelFactory<? extends StateModel> factory =
        manager.getStateMachineEngine().getStateModelFactory(ClusterMapConfig.AMBRY_STATE_MODEL_DEF);
    assertTrue(factory instanceof AmbryStateModelFactory);
    getNumberOfReplicaInStateFromMetric("offline", metricRegistry);
    assertEquals(replicaIds.size(), getNumberOfReplicaInStateFromMetric("bootstrap", metricRegistry));
    helixParticipant.close();
  }

  /**
   * Test two distributed locks
   * @param lock1
   * @param lock2
   * @throws Exception
   */
  private void testTwoDistributedLocks(DistributedLock lock1, DistributedLock lock2) throws Exception {
    // first lock should succeed
    assertTrue(lock1.tryLock());
    // second lock should fail since the lock is acquired by first lock
    assertFalse(lock2.tryLock());
    lock1.unlock();
    // second lock should succeed since the first lock released the lock
    assertTrue(lock2.tryLock());
    lock2.unlock();

    assertTrue(lock1.tryLock());
    assertFalse(lock2.tryLock());
    Thread.sleep(distributedLockLeaseTimeout + 500);
    // After lease timeout, the lock is free to acquire again even if it's not unlock
    assertTrue(lock2.tryLock());

    lock1.unlock();
    lock2.unlock();
    lock1.close();
    lock2.close();
  }

  /**
   * This is a hacky way to create distributed lock with different zookeeper connection.
   * @param resource The resource to lock
   * @param userId The user id for the lock
   * @param reason The reason to acquire the lock
   * @param zkConnectionStr The zookeeper server
   * @return A {@link DistributedLock}.
   * @throws Exception
   */
  private DistributedLock getDistributedLockWithDedicatedZkClient(String resource, String userId, String reason,
      String zkConnectionStr) throws Exception {
    LockScope distributedLockScope =
        new HelixLockScope(HelixLockScope.LockScopeProperty.RESOURCE, Arrays.asList(clusterName, resource));
    String scopePath = distributedLockScope.getPath();
    long leaseTimeout = clusterMapConfig.clustermapDistributedLockLeaseTimeoutInMs;
    int priority = 0;
    int waitingTimeout = Integer.MAX_VALUE;
    int cleanupTimeout = 0;
    boolean isForceful = false;

    Constructor<ZKDistributedNonblockingLock> constructor = getConstructorForZkDistributedLock();
    assertNotNull(constructor);
    ZKDistributedNonblockingLock zkLock =
        constructor.newInstance(scopePath, leaseTimeout, reason, userId, priority, waitingTimeout, cleanupTimeout,
            isForceful, null,
            new ZkBaseDataAccessor<ZNRecord>(zkConnectionStr, ZkBaseDataAccessor.ZkClientType.DEDICATED));

    return new HelixParticipant.DistributedLockImpl(zkLock);
  }

  private Constructor<ZKDistributedNonblockingLock> getConstructorForZkDistributedLock() throws Exception {
    Constructor<?>[] constructors = ZKDistributedNonblockingLock.class.getDeclaredConstructors();
    // Use reflection API to get a private constructor so that we can create a dedicated zookeeper connection.
    for (Constructor<?> constructor : constructors) {
      if ((constructor.getModifiers() & Modifier.PRIVATE) == Modifier.PRIVATE && constructor.getParameterCount() == 10
          && constructor.getParameterTypes()[0] == String.class) {
        constructor.setAccessible(true);
        return (Constructor<ZKDistributedNonblockingLock>) constructor;
      }
    }
    return null;
  }

  private DataNodeConfig getDataNodeConfigInHelix(HelixAdmin helixAdmin, String instanceName) {
    return dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? instanceConfigConverter.convert(
        helixAdmin.getInstanceConfig(clusterName, instanceName)) : propertyStoreAdapter.get(instanceName);
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
   * Deep copy a {@link DataNodeConfig}.
   * @param dataNodeConfig {@link DataNodeConfig} to copy
   * @return {@link InstanceConfig} of given data node.
   */
  private DataNodeConfig deepCopyDataNodeConfig(DataNodeConfig dataNodeConfig) {
    String instanceName = ClusterMapUtils.getInstanceName(dataNodeConfig.getHostName(), dataNodeConfig.getPort());
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(dataNodeConfig.getHostName());
    instanceConfig.setPort(Integer.toString(dataNodeConfig.getPort()));
    instanceConfig.getRecord().setSimpleField(DATACENTER_STR, dataNodeConfig.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(RACKID_STR, dataNodeConfig.getRackId());
    instanceConfig.getRecord().setSimpleField(SCHEMA_VERSION_STR, Integer.toString(CURRENT_SCHEMA_VERSION));
    instanceConfig.getRecord().setSimpleField(SSL_PORT_STR, Integer.toString(dataNodeConfig.getSslPort()));
    instanceConfig.getRecord().setSimpleField(HTTP2_PORT_STR, Integer.toString(dataNodeConfig.getHttp2Port()));
    instanceConfig.getRecord().setSimpleField(XID_STR, Long.toString(dataNodeConfig.getXid()));
    Map<String, Map<String, String>> mountPathToDiskInfos = new TreeMap<>();
    for (Map.Entry<String, DataNodeConfig.DiskConfig> entry : dataNodeConfig.getDiskConfigs().entrySet()) {
      String mountPath = entry.getKey();
      DataNodeConfig.DiskConfig diskConfig = entry.getValue();
      StringBuilder replicaStrBuilder = new StringBuilder();
      for (Map.Entry<String, DataNodeConfig.ReplicaConfig> replicaEntry : diskConfig.getReplicaConfigs().entrySet()) {
        DataNodeConfig.ReplicaConfig replicaConfig = replicaEntry.getValue();
        replicaStrBuilder.append(replicaEntry.getKey())
            .append(REPLICAS_STR_SEPARATOR)
            .append(replicaConfig.getReplicaCapacityInBytes())
            .append(REPLICAS_STR_SEPARATOR)
            .append(replicaConfig.getPartitionClass())
            .append(REPLICAS_DELIM_STR);
      }
      Map<String, String> diskInfo = new HashMap<>();
      diskInfo.put(REPLICAS_STR, replicaStrBuilder.toString());
      diskInfo.put(DISK_CAPACITY_STR, String.valueOf(diskConfig.getDiskCapacityInBytes()));
      diskInfo.put(DISK_STATE, AVAILABLE_STR);
      mountPathToDiskInfos.put(mountPath, diskInfo);
    }
    instanceConfig.getRecord().setMapFields(mountPathToDiskInfos);
    instanceConfig.getRecord().setListField(SEALED_STR, new ArrayList<>(dataNodeConfig.getSealedReplicas()));
    instanceConfig.getRecord().setListField(PARTIALLY_SEALED_STR, new ArrayList<>(dataNodeConfig.getSealedReplicas()));
    instanceConfig.getRecord().setListField(STOPPED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getStoppedReplicas()));
    instanceConfig.getRecord()
        .setListField(DISABLED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getDisabledReplicas()));
    return instanceConfigConverter.convert(instanceConfig);
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

  private void listIsExpectedSize(List<String> list, int expectedSize, String listName) {
    assertNotNull(listName + " is null", list);
    assertEquals(listName + " doesn't have the expected size " + expectedSize, expectedSize, list.size());
  }

  private static class HelixClusterAgentsFactoryWithMockClusterMap extends HelixClusterAgentsFactory {
    public HelixClusterAgentsFactoryWithMockClusterMap(ClusterMapConfig config, HelixFactory helixFactory) {
      super(config, helixFactory);
    }

    public HelixClusterAgentsFactoryWithMockClusterMap(ClusterMapConfig config, MetricRegistry metricRegistry) {
      super(config, metricRegistry);
    }

    @Override
    public HelixClusterManager getClusterMap() throws IOException {
      return mock(HelixClusterManager.class);
    }
  }
}
