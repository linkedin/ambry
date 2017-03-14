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
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.StateMachineEngine;
import org.json.JSONException;


/**
 * An implementation of {@link ClusterParticipant} that registers registers as a participant to a Helix cluster.
 */
class HelixParticipant implements ClusterParticipant {
  private HelixManager manager;
  private final String clusterName;
  private final String zkConnectStr;

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @throws JSONException if there is an error in parsing the JSON serialized ZK connect string config.
   */
  HelixParticipant(ClusterMapConfig clusterMapConfig) throws JSONException {
    clusterName = clusterMapConfig.clusterMapClusterName;
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Clustername is empty in clusterMapConfig");
    }
    zkConnectStr = ClusterMapUtils.parseZkJsonAndPopulateZkInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
        .get(clusterMapConfig.clusterMapDatacenterName);
  }

  /**
   * Initialize the participant by registering via the {@link HelixManager} as a participant to the associated Helix
   * cluster.
   * @param hostName the hostname to use when registering as a participant.
   * @param port the port to use when registering as a participant.
   * @throws Exception if there is an error connecting to the Helix cluster.
   */
  @Override
  public void initialize(String hostName, int port) throws Exception {
    String instanceName = ClusterMapUtils.getInstanceName(hostName, port);
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory());
    manager.connect();
  }

  /**
   * Disconnect from the {@link HelixManager}.
   */
  @Override
  public void terminate() {
    manager.disconnect();
  }
}

