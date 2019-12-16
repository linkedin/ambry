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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.TestUtils.*;
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
    List<com.github.ambry.utils.TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, "DC0", (byte) 0, 2199, false));
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
    HelixParticipant helixParticipant =
        new HelixParticipant(new ClusterMapConfig(new VerifiableProperties(props)), helixManagerFactory);
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
    HelixParticipant helixParticipant =
        new HelixParticipant(new ClusterMapConfig(new VerifiableProperties(props)), helixManagerFactory);
    HelixParticipant helixParticipantDummy =
        new HelixParticipant(new ClusterMapConfig(new VerifiableProperties(propsDummy)), helixManagerFactory);
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
   * @throws IOException
   */
  @Test
  public void testBadCases() throws IOException {
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
    HelixParticipant helixParticipant = new HelixParticipant(clusterMapConfig, helixManagerFactory);
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
      new HelixParticipant(clusterMapConfig, helixManagerFactory);
      fail("Instantiation should have failed");
    } catch (IllegalStateException e) {
      // OK
    }

    props.setProperty("clustermap.cluster.name", "HelixParticipantTestCluster");
    props.setProperty("clustermap.dcs.zk.connect.strings", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixParticipant(clusterMapConfig, helixManagerFactory);
      fail("Instantiation should have failed");
    } catch (IOException e) {
      // OK
    }
  }

  /**
   * Test the good path of instantiation, initialization and termination of the {@link HelixParticipant}
   * @throws Exception
   */
  @Test
  public void testHelixParticipant() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant = new HelixParticipant(clusterMapConfig, helixManagerFactory);
    participant.participate(Collections.emptyList());
    MockHelixManagerFactory.MockHelixManager helixManager = helixManagerFactory.getHelixManager();
    assertTrue(helixManager.isConnected());
    assertEquals(stateModelDef, helixManager.getStateModelDef());
    assertEquals(AmbryStateModelFactory.class, helixManager.getStateModelFactory().getClass());
    participant.close();
    assertFalse(helixManager.isConnected());
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
