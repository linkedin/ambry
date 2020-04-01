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
import java.util.List;
import java.util.Properties;
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
    }, new HelixParticipantMetrics(new MetricRegistry()));
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
   * @throws Exception
   */
  @Test
  public void testAmbryPartitionStateModel() throws Exception {
    assumeTrue(stateModelDef.equals(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(config);
    MetricRegistry metricRegistry = new MetricRegistry();
    HelixParticipantMetrics participantMetrics = new HelixParticipantMetrics(metricRegistry);
    String resourceName = "0";
    String partitionName = "1";
    Message mockMessage = Mockito.mock(Message.class);
    when(mockMessage.getPartitionName()).thenReturn(partitionName);
    when(mockMessage.getResourceName()).thenReturn(resourceName);
    AmbryPartitionStateModel stateModel =
        new AmbryPartitionStateModel(resourceName, partitionName, mockHelixParticipant, config, participantMetrics);
    participantMetrics.setLocalPartitionCount(1);
    assertEquals("Offline count is not expected", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".offlinePartitionCount").getValue());
    // OFFLINE -> BOOTSTRAP
    stateModel.onBecomeBootstrapFromOffline(mockMessage, null);
    assertEquals("Bootstrap count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".bootstrapPartitionCount").getValue());
    assertEquals("Offline count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".offlinePartitionCount").getValue());
    // BOOTSTRAP -> STANDBY
    stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
    assertEquals("Standby count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".standbyPartitionCount").getValue());
    assertEquals("Bootstrap count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".bootstrapPartitionCount").getValue());
    // STANDBY -> LEADER
    stateModel.onBecomeLeaderFromStandby(mockMessage, null);
    assertEquals("Leader count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".leaderPartitionCount").getValue());
    assertEquals("Standby count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".standbyPartitionCount").getValue());
    // LEADER -> STANDBY
    stateModel.onBecomeStandbyFromLeader(mockMessage, null);
    assertEquals("Standby count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".standbyPartitionCount").getValue());
    assertEquals("Leader count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".leaderPartitionCount").getValue());
    // STANDBY -> INACTIVE
    stateModel.onBecomeInactiveFromStandby(mockMessage, null);
    assertEquals("Inactive count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".inactivePartitionCount").getValue());
    assertEquals("Standby count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".standbyPartitionCount").getValue());
    // INACTIVE -> OFFLINE
    stateModel.onBecomeOfflineFromInactive(mockMessage, null);
    assertEquals("Offline count should be 1", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".offlinePartitionCount").getValue());
    assertEquals("Inactive count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".inactivePartitionCount").getValue());
    // OFFLINE -> DROPPED
    stateModel.onBecomeDroppedFromOffline(mockMessage, null);
    assertEquals("Offline count should be 0", 0,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".offlinePartitionCount").getValue());
    assertEquals("Dropped count should be updated", 1, participantMetrics.partitionDroppedCount.getCount());

    // reset method
    stateModel.reset();
    assertEquals("Offline count should be 1 after reset", 1,
        metricRegistry.getGauges().get(HelixParticipant.class.getName() + ".offlinePartitionCount").getValue());
  }
}
