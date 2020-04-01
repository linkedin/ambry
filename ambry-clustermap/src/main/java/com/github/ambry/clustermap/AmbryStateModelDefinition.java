/*
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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;


/**
 * Define state model employed by Ambry.
 * Helix allows user to define custom state model which means Ambry is able to introduce custom states and define the
 * transition logic among these states. This class presents replica states, valid transitions and priorities that would
 * be used by Helix to manage Ambry partitions. Specifically, we add two custom states into new state model: BOOTSTRAP
 * and INACTIVE. The valid transitions among states are depicted as follows:
 *
 *                  <--- INACTIVE <----
 *                 |                   |
 * DROPPED <--- OFFLINE              STANDBY  <----> LEADER
 *                 |                   |
 *                  ---> BOOTSTRAP --->
 *
 */
class AmbryStateModelDefinition {
  static final String AMBRY_LEADER_STANDBY_MODEL = "AmbryLeaderStandby";

  private static final String UPPER_BOUND_REPLICATION_FACTOR = "R";

  static StateModelDefinition getDefinition() {

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(AMBRY_LEADER_STANDBY_MODEL);
    // Init state
    builder.initialState(ReplicaState.OFFLINE.name());

    /*
     * States and their priority which are managed by Helix controller.
     *
     * The implication is that transition to a state which has a higher number is
     * considered a "downward" transition by Helix. Downward transitions are not
     * throttled.
     */
    builder.addState(ReplicaState.LEADER.name(), 0);
    builder.addState(ReplicaState.STANDBY.name(), 1);
    builder.addState(ReplicaState.BOOTSTRAP.name(), 2);
    builder.addState(ReplicaState.INACTIVE.name(), 2);
    builder.addState(ReplicaState.OFFLINE.name(), 3);
    // HelixDefinedState adds two additional states: ERROR and DROPPED. The priority is Integer.MAX_VALUE
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // Add valid transitions between the states.
    builder.addTransition(ReplicaState.OFFLINE.name(), ReplicaState.BOOTSTRAP.name(), 3);
    builder.addTransition(ReplicaState.BOOTSTRAP.name(), ReplicaState.STANDBY.name(), 2);
    builder.addTransition(ReplicaState.STANDBY.name(), ReplicaState.LEADER.name(), 1);
    builder.addTransition(ReplicaState.LEADER.name(), ReplicaState.STANDBY.name(), 0);
    builder.addTransition(ReplicaState.STANDBY.name(), ReplicaState.INACTIVE.name(), 2);
    builder.addTransition(ReplicaState.INACTIVE.name(), ReplicaState.OFFLINE.name(), 3);
    builder.addTransition(ReplicaState.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // States constraints
    /*
     * Static constraint: number of leader replica for certain partition shouldn't exceed 1 at any time.
     */
    builder.upperBound(ReplicaState.LEADER.name(), 1);
    /*
     * Dynamic constraint: R means it should be derived based on the replication factor for the cluster
     * this allows a different replication factor for each resource without having to define a new state model.
     */
    builder.dynamicUpperBound(ReplicaState.STANDBY.name(), UPPER_BOUND_REPLICATION_FACTOR);

    return builder.build();
  }
}
