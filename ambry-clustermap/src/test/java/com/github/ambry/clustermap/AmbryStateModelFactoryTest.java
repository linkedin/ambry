/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link AmbryStateModelFactory} and {@link AmbryPartitionStateModel}
 */
@RunWith(Parameterized.class)
public class AmbryStateModelFactoryTest {
  private final ClusterMapConfig config;
  private final String stateModelDef;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{ClusterMapConfig.OLD_STATE_MODEL_DEF}, {ClusterMapConfig.AMBRY_STATE_MODEL_DEF}});
  }

  public AmbryStateModelFactoryTest(String stateModelDef) throws Exception {
    List<TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new TestUtils.ZkInfo(null, "DC0", (byte) 0, 2299, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "AmbryTest");
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.state.model.definition", stateModelDef);
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.enable.state.model.listener", Boolean.toString(true));
    config = new ClusterMapConfig(new VerifiableProperties(props));
    this.stateModelDef = stateModelDef;
  }

  @Test
  public void testDifferentStateModelDefs() {
    AmbryStateModelFactory factory = new AmbryStateModelFactory(config, new PartitionStateChangeListener() {
      @Override
      public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
        // no op
      }

      @Override
      public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
        // no op
      }

      @Override
      public void onPartitionBecomeLeaderFromStandby(String partitionName) {
        // no op
      }

      @Override
      public void onPartitionBecomeStandbyFromLeader(String partitionName) {
        //no op
      }

      @Override
      public void onPartitionBecomeInactiveFromStandby(String partitionName) {
        // no op
      }

      @Override
      public void onPartitionBecomeOfflineFromInactive(String partitionName) {
        // no op
      }

      @Override
      public void onPartitionBecomeDroppedFromOffline(String partitionName) {
        // no op
      }
    });
    StateModel stateModel;
    switch (config.clustermapStateModelDefinition) {
      case ClusterMapConfig.OLD_STATE_MODEL_DEF:
        stateModel = factory.createNewStateModel("0", "1");
        assertTrue("Unexpected state model def", stateModel instanceof DefaultLeaderStandbyStateModel);
        break;
      case ClusterMapConfig.AMBRY_STATE_MODEL_DEF:
        stateModel = factory.createNewStateModel("0", "1");
        assertTrue("Unexpected state model def", stateModel instanceof AmbryPartitionStateModel);
        break;
      default:
        // state model is already validated in clusterMapConfig, no need to test invalid state model here.
    }
  }

  /**
   * Test that {@link HelixParticipantMetrics} keeps track of partition during state transition
   */
  @Test
  public void testAmbryPartitionStateModel() {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    MetricRegistry metricRegistry = new MetricRegistry();
    MockHelixParticipant.metricRegistry = metricRegistry;
    DataNodeConfig mockDataNodeConfig = Mockito.mock(DataNodeConfig.class);
    Set<String> disabledPartitionSet = new HashSet<>();
    Set<String> enabledPartitionSet = new HashSet<>();
    when(mockDataNodeConfig.getDisabledReplicas()).thenReturn(disabledPartitionSet);
    DataNodeConfigSource mockConfigSource = Mockito.mock(DataNodeConfigSource.class);
    when(mockConfigSource.get(anyString())).thenReturn(mockDataNodeConfig);
    HelixAdmin mockHelixAdmin = Mockito.mock(HelixAdmin.class);
    InstanceConfig mockInstanceConfig = Mockito.mock(InstanceConfig.class);
    doAnswer(invocation -> {
      String partitionName = invocation.getArgument(1);
      boolean enable = invocation.getArgument(2);
      if (enable) {
        enabledPartitionSet.add(partitionName);
      }
      return null;
    }).when(mockInstanceConfig).setInstanceEnabledForPartition(any(), any(), anyBoolean());
    when(mockHelixAdmin.getInstanceConfig(anyString(), anyString())).thenReturn(mockInstanceConfig);
    when(mockHelixAdmin.setInstanceConfig(anyString(), anyString(), any())).thenReturn(true);
    HelixManager mockHelixManager = Mockito.mock(HelixManager.class);
    when(mockHelixManager.getClusterManagmentTool()).thenReturn(mockHelixAdmin);
    MockHelixManagerFactory.overrideGetHelixManager = true;
    MockHelixParticipant.mockHelixFactory = new MockHelixManagerFactory(mockConfigSource, mockHelixManager);
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(config);
    HelixParticipantMetrics participantMetrics = mockHelixParticipant.getHelixParticipantMetrics();
    String resourceName = "0";
    String partitionName = "1";
    Message mockMessage = Mockito.mock(Message.class);
    when(mockMessage.getPartitionName()).thenReturn(partitionName);
    when(mockMessage.getResourceName()).thenReturn(resourceName);
    AmbryPartitionStateModel stateModel =
        new AmbryPartitionStateModel(resourceName, partitionName, mockHelixParticipant, config,
            new ConcurrentHashMap<>());
    mockHelixParticipant.setInitialLocalPartitions(new HashSet<>(Collections.singletonList(partitionName)));
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);

    // OFFLINE -> BOOTSTRAP
    stateModel.onBecomeBootstrapFromOffline(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap"), Arrays.asList(0, 1), metricRegistry);
    // BOOTSTRAP -> STANDBY
    stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby"), Arrays.asList(0, 0, 1), metricRegistry);
    // STANDBY -> LEADER
    stateModel.onBecomeLeaderFromStandby(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "leader", "standby"), Arrays.asList(0, 1, 0), metricRegistry);
    // LEADER -> STANDBY
    stateModel.onBecomeStandbyFromLeader(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "leader", "standby"), Arrays.asList(0, 0, 1), metricRegistry);
    // STANDBY -> INACTIVE
    stateModel.onBecomeInactiveFromStandby(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "inactive", "standby"), Arrays.asList(0, 1, 0), metricRegistry);
    // INACTIVE -> OFFLINE
    stateModel.onBecomeOfflineFromInactive(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "inactive"), Arrays.asList(1, 0), metricRegistry);
    // OFFLINE -> DROPPED
    disabledPartitionSet.add(partitionName);
    stateModel.onBecomeDroppedFromOffline(mockMessage, null);
    assertStateCount(Arrays.asList("offline"), Arrays.asList(0), metricRegistry);

    assertEquals("Dropped count should be updated", 1, participantMetrics.partitionDroppedCount.getCount());
    assertTrue("Partition should be removed from disabled partition set", disabledPartitionSet.isEmpty());
    assertEquals("Mismatch in enabled partition", partitionName, enabledPartitionSet.iterator().next());
    // ERROR -> DROPPED
    stateModel.onBecomeDroppedFromError(mockMessage, null);
    assertEquals("Dropped count should be updated", 2, participantMetrics.partitionDroppedCount.getCount());
    // ERROR -> OFFLINE (this occurs when we use Helix API to reset certain partition in ERROR state)
    stateModel.onBecomeOfflineFromError(mockMessage, null);
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    // reset method
    stateModel.reset();
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    // call reset method again to mock the case where same partition is reset multiple times during zk disconnection or shutdown
    stateModel.reset();
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    MockHelixManagerFactory.overrideGetHelixManager = false;
  }

  @Test
  public void testAmbryPartitionStateModelWithDuplicatePartitionIds() {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    MetricRegistry metricRegistry = new MetricRegistry();
    MockHelixParticipant.metricRegistry = metricRegistry;
    DataNodeConfig mockDataNodeConfig = Mockito.mock(DataNodeConfig.class);
    Set<String> disabledPartitionSet = new HashSet<>();
    Set<String> enabledPartitionSet = new HashSet<>();
    when(mockDataNodeConfig.getDisabledReplicas()).thenReturn(disabledPartitionSet);
    DataNodeConfigSource mockConfigSource = Mockito.mock(DataNodeConfigSource.class);
    when(mockConfigSource.get(anyString())).thenReturn(mockDataNodeConfig);
    HelixAdmin mockHelixAdmin = Mockito.mock(HelixAdmin.class);
    InstanceConfig mockInstanceConfig = Mockito.mock(InstanceConfig.class);
    doAnswer(invocation -> {
      String partitionName = invocation.getArgument(1);
      boolean enable = invocation.getArgument(2);
      if (enable) {
        enabledPartitionSet.add(partitionName);
      }
      return null;
    }).when(mockInstanceConfig).setInstanceEnabledForPartition(any(), any(), anyBoolean());
    when(mockHelixAdmin.getInstanceConfig(anyString(), anyString())).thenReturn(mockInstanceConfig);
    when(mockHelixAdmin.setInstanceConfig(anyString(), anyString(), any())).thenReturn(true);
    HelixManager mockHelixManager = Mockito.mock(HelixManager.class);
    when(mockHelixManager.getClusterManagmentTool()).thenReturn(mockHelixAdmin);
    MockHelixManagerFactory.overrideGetHelixManager = true;
    MockHelixParticipant.mockHelixFactory = new MockHelixManagerFactory(mockConfigSource, mockHelixManager);
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(config);
    HelixParticipantMetrics participantMetrics = mockHelixParticipant.getHelixParticipantMetrics();

    String resourceName = "0";
    String partitionName = "1";
    String newResourceName = "10";
    Message mockMessage = Mockito.mock(Message.class);
    when(mockMessage.getPartitionName()).thenReturn(partitionName);
    when(mockMessage.getResourceName()).thenReturn(resourceName);

    Message newMockMessage = Mockito.mock(Message.class);
    when(newMockMessage.getPartitionName()).thenReturn(partitionName);
    when(newMockMessage.getResourceName()).thenReturn(newResourceName);
    ConcurrentMap<String, String> partitionToResource = new ConcurrentHashMap<>();

    AmbryPartitionStateModel stateModel =
        new AmbryPartitionStateModel(resourceName, partitionName, mockHelixParticipant, config, partitionToResource);

    AmbryPartitionStateModel newStateModel =
        new AmbryPartitionStateModel(newResourceName, partitionName, mockHelixParticipant, config, partitionToResource);

    // resource move to bootstrap then new resource start transition
    mockHelixParticipant.setInitialLocalPartitions(new HashSet<>(Collections.singletonList(partitionName)));
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    // resource: OFFLINE -> BOOTSTRAP, should work
    stateModel.onBecomeBootstrapFromOffline(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap"), Arrays.asList(0, 1), metricRegistry);
    // resource: BOOTSTRAP -> STANDBY, should work
    stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby"), Arrays.asList(0, 0, 1), metricRegistry);
    // new resource: OFFLINE -> BOOTSTRAP, should work
    newStateModel.onBecomeBootstrapFromOffline(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby"), Arrays.asList(0, 1, 0), metricRegistry);
    // resource: STANDBY -> LEADER, should not work
    stateModel.onBecomeLeaderFromStandby(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby", "leader"), Arrays.asList(0, 1, 0, 0),
        metricRegistry);
    // resource: LEADER -> STANDBY, should not work
    stateModel.onBecomeStandbyFromLeader(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby", "leader"), Arrays.asList(0, 1, 0, 0),
        metricRegistry);
    // resource: STANDBY -> INACTIVE, should not work
    stateModel.onBecomeInactiveFromStandby(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby", "inactive"), Arrays.asList(0, 1, 0, 0),
        metricRegistry);
    // resource: INACTIVE -> OFFLINE, should not work
    stateModel.onBecomeOfflineFromInactive(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "inactive"), Arrays.asList(0, 1, 0), metricRegistry);
    // resource: OFFLINE -> DROPPED, should not work
    disabledPartitionSet.add(partitionName);
    stateModel.onBecomeDroppedFromOffline(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap"), Arrays.asList(0, 1), metricRegistry);
    assertEquals("Dropped count should not be updated", 0, participantMetrics.partitionDroppedCount.getCount());
    assertFalse("Partition should not be removed from disabled partition set", disabledPartitionSet.isEmpty());

    // resource: ERROR -> DROPPED, should not work
    stateModel.onBecomeDroppedFromError(mockMessage, null);
    assertEquals("Dropped count should not be updated", 0, participantMetrics.partitionDroppedCount.getCount());
    // resource: ERROR -> OFFLINE (this occurs when we use Helix API to reset certain partition in ERROR state), should not work
    stateModel.onBecomeOfflineFromError(mockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap"), Arrays.asList(0, 1), metricRegistry);

    // Now new resource would do the state transition
    // new resource: BOOTSTRAP -> STANDBY, should work
    newStateModel.onBecomeStandbyFromBootstrap(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby"), Arrays.asList(0, 0, 1), metricRegistry);
    // new resource: STANDBY -> LEADER, should work
    newStateModel.onBecomeLeaderFromStandby(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby", "leader"), Arrays.asList(0, 0, 0, 1),
        metricRegistry);
    // new resource: LEADER -> STANDBY, should work
    newStateModel.onBecomeStandbyFromLeader(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "bootstrap", "standby", "leader"), Arrays.asList(0, 0, 1, 0),
        metricRegistry);
    // new resource: STANDBY -> INACTIVE, should work
    newStateModel.onBecomeInactiveFromStandby(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "standby", "inactive"), Arrays.asList(0, 0, 1), metricRegistry);
    // new resource: INACTIVE -> OFFLINE, should work
    newStateModel.onBecomeOfflineFromInactive(newMockMessage, null);
    assertStateCount(Arrays.asList("offline", "inactive"), Arrays.asList(1, 0), metricRegistry);
    // new resource: OFFLINE -> DROPPED, should work
    disabledPartitionSet.add(partitionName);
    newStateModel.onBecomeDroppedFromOffline(newMockMessage, null);
    assertStateCount(Arrays.asList("offline"), Arrays.asList(0), metricRegistry);
    assertEquals("Dropped count should be updated", 1, participantMetrics.partitionDroppedCount.getCount());
    assertTrue("Partition should be removed from disabled partition set", disabledPartitionSet.isEmpty());
    assertEquals("Mismatch in enabled partition", partitionName, enabledPartitionSet.iterator().next());
    // new resource: ERROR -> DROPPED, should work
    newStateModel.onBecomeDroppedFromError(newMockMessage, null);
    assertEquals("Dropped count should be updated", 2, participantMetrics.partitionDroppedCount.getCount());
    // new resource: ERROR -> OFFLINE (this occurs when we use Helix API to reset certain partition in ERROR state)
    newStateModel.onBecomeOfflineFromError(newMockMessage, null);
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    // reset method
    newStateModel.reset();
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    // call reset method again to mock the case where same partition is reset multiple times during zk disconnection or shutdown
    newStateModel.reset();
    assertStateCount(Arrays.asList("offline"), Arrays.asList(1), metricRegistry);
    MockHelixManagerFactory.overrideGetHelixManager = false;
  }

  /**
   * Get value of a certain metric which starts with given keywords.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param className the name of class that metric associates with.
   * @param metricName the keywords of metric name.
   * @return value of found metric.
   */
  private Object getHelixParticipantMetricValue(MetricRegistry metricRegistry, String className, String metricName) {
    String metricKey = metricRegistry.getGauges()
        .keySet()
        .stream()
        .filter(key -> key.startsWith(className + "." + metricName))
        .findFirst()
        .get();
    return metricRegistry.getGauges().get(metricKey).getValue();
  }

  void assertStateCount(List<String> states, List<Integer> counts, MetricRegistry metricRegistry) {
    assertEquals(states.size(), counts.size());
    for (int i = 0; i < states.size(); i++) {
      String state = states.get(i);
      int count = counts.get(i);
      assertEquals(state + " count should be " + count, count,
          getHelixParticipantMetricValue(metricRegistry, HelixParticipant.class.getName(), state + "PartitionCount"));
    }
  }
}
