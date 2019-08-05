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
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * A factory for creating {@link AmbryDefaultStateModel}
 */
class AmbryStateModelFactory extends StateModelFactory<StateModel> {
  private final String ambryStateModelDef;

  AmbryStateModelFactory(ClusterMapConfig clusterMapConfig) {
    ambryStateModelDef = clusterMapConfig.clustermapStateModelDefinition;
  }

  /**
   * Create and return an instance of {@link AmbryDefaultStateModel}
   * @param resourceName the resource name for which this state model is being created.
   * @param partitionName the partition name for which this state model is being created.
   * @return an instance of {@link AmbryDefaultStateModel}.
   */
  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    StateModel stateModelToReturn;
    switch (ambryStateModelDef) {
      case AmbryStateModelDefinition.AMBRY_LEADER_STANDBY_MODEL:
        stateModelToReturn = new AmbryPartitionStateModel();
        break;
      default:
        stateModelToReturn = new AmbryDefaultStateModel();
    }
    return stateModelToReturn;
  }
}

