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
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link HelixParticipant}
 */
public class HelixParticipantTest {
  private final MockHelixManagerFactory helixManagerFactory;
  private final Properties props;
  private final String clusterName = "HelixParticipantTestCluster";
  private final JSONObject zkJson;

  public HelixParticipantTest() throws Exception {
    List<com.github.ambry.utils.TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, "DC0", (byte) 0, 2199, false));
    zkJson = constructZkLayoutJSON(zkInfoList);
    props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "2200");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
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
    // Connect failure.
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManagerFactory.helixManager.beBad = true;
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
    MockHelixManager helixManager = helixManagerFactory.helixManager;
    assertTrue(helixManager.isConnected());
    assertEquals(LeaderStandbySMD.name, helixManager.stateModelDef);
    assertEquals(AmbryStateModelFactory.class, helixManager.stateModelFactory.getClass());
    participant.close();
    assertFalse(helixManager.isConnected());
  }

  /**
   * A Mock implementation of {@link HelixFactory} that returns the {@link MockHelixManager}
   */
  private static class MockHelixManagerFactory extends HelixFactory {
    private final MockHelixManager helixManager;

    /**
     * Construct this factory.
     */
    MockHelixManagerFactory() {
      helixManager = new MockHelixManager();
    }

    /**
     * Return the {@link MockHelixManager}
     * @param clusterName unused.
     * @param instanceName unused.
     * @param instanceType unused.
     * @param zkAddr unused.
     * @return the {@link MockHelixManager}
     */
    @Override
    HelixManager getZKHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddr) {
      return helixManager;
    }

    /**
     * Return the {@link MockHelixAdmin}
     * @param zkAddr unused.
     * @return the {@link MockHelixAdmin}
     */
    @Override
    HelixAdmin getHelixAdmin(String zkAddr) {
      return new MockHelixAdmin();
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

  /**
   * A mock implementation of the {@link HelixManager} for exclusive use for testing the {@link HelixParticipant}
   */
  private static class MockHelixManager implements HelixManager {
    private String stateModelDef;
    private StateModelFactory stateModelFactory;
    private boolean isConnected;
    boolean beBad;
    private final HelixAdmin helixAdmin = new MockHelixAdmin();

    @Override
    public StateMachineEngine getStateMachineEngine() {
      return new StateMachineEngine() {
        @Override
        public boolean registerStateModelFactory(String stateModelDef,
            StateModelFactory<? extends StateModel> factory) {
          MockHelixManager.this.stateModelDef = stateModelDef;
          stateModelFactory = factory;
          return true;
        }

        @Override
        public boolean registerStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory,
            String factoryName) {
          throw new IllegalStateException("Not implemented");
        }

        @Override
        public boolean removeStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory) {
          return false;
        }

        @Override
        public boolean removeStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory,
            String factoryName) {
          return false;
        }

        @Override
        public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName) {
          return null;
        }

        @Override
        public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName, String factoryName) {
          return null;
        }

        @Override
        public MessageHandler createHandler(Message message, NotificationContext context) {
          return null;
        }

        @Override
        public String getMessageType() {
          return null;
        }

        @Override
        public List<String> getMessageTypes() {
          return null;
        }

        @Override
        public void reset() {

        }
      };
    }

    @Override
    public Long getSessionStartTime() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void connect() throws Exception {
      if (beBad) {
        throw new IOException("Being bad");
      }

      isConnected = true;
    }

    @Override
    public boolean isConnected() {
      return isConnected;
    }

    @Override
    public void disconnect() {
      isConnected = false;
    }

    //****************************
    // Not implemented.
    //****************************

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addConfigChangeListener(ScopedConfigChangeListener listener, HelixConfigScope.ConfigScopeProperty scope)
        throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName,
        String sessionId) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerMessageListener(MessageListener listener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public boolean removeListener(PropertyKey key, Object listener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public HelixDataAccessor getHelixDataAccessor() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public ConfigAccessor getConfigAccessor() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getClusterName() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getInstanceName() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getSessionId() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public long getLastNotificationTime() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public HelixAdmin getClusterManagmentTool() {
      return helixAdmin;
    }

    @Override
    public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public ClusterMessagingService getMessagingService() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public InstanceType getInstanceType() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getVersion() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public HelixManagerProperties getProperties() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public boolean isLeader() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void startTimerTasks() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void stopTimerTasks() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addPreConnectCallback(PreConnectCallback callback) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public ParticipantHealthReportCollector getHealthReportCollector() {
      throw new IllegalStateException("Not implemented");
    }
  }
}
