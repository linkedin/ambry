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
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * A factory for creating {@link StateModel}.
 */
class AmbryStateModelFactory extends StateModelFactory<StateModel> {
  private final ClusterMapConfig clustermapConfig;
  private final PartitionStateChangeListener partitionStateChangeListener;

  AmbryStateModelFactory(ClusterMapConfig clusterMapConfig, PartitionStateChangeListener partitionStateChangeListener) {
    this.clustermapConfig = clusterMapConfig;
    this.partitionStateChangeListener = partitionStateChangeListener;
  }

  /**
   * Create and return an instance of {@link StateModel} based on given definition
   * @param resourceName the resource name for which this state model is being created.
   * @param partitionName the partition name for which this state model is being created.
   * @return an instance of {@link StateModel}.
   */
  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    StateModel stateModelToReturn;
    switch (clustermapConfig.clustermapStateModelDefinition) {
      case AmbryStateModelDefinition.AMBRY_LEADER_STANDBY_MODEL:
        stateModelToReturn =
            new AmbryPartitionStateModel(resourceName, partitionName, partitionStateChangeListener, clustermapConfig);
        break;
      case LeaderStandbySMD.name:
        stateModelToReturn = new DefaultLeaderStandbyStateModel();
        break;
      default:
        // Code won't get here because state model def is already validated in ClusterMapConfig. We keep exception here
        // in case the validation logic is changed in ClusterMapConfig.
        throw new IllegalArgumentException(
            "Unsupported state model definition: " + clustermapConfig.clustermapStateModelDefinition);
    }
    return stateModelToReturn;
  }
}
