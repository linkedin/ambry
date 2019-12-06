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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.helix.participant.statemachine.StateModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Test for {@link AmbryStateModelFactory}
 */
@RunWith(Parameterized.class)
public class AmbryStateModelFactoryTest {
  private final ClusterMapConfig config;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{ClusterMapConfig.OLD_STATE_MODEL_DEF}, {ClusterMapConfig.AMBRY_STATE_MODEL_DEF}});
  }

  public AmbryStateModelFactoryTest(String stateModelDef) {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "AmbryTest");
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.state.model.definition", stateModelDef);
    config = new ClusterMapConfig(new VerifiableProperties(props));
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
}
