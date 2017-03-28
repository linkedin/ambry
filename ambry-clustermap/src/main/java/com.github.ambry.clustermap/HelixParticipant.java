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
import java.io.IOException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.StateMachineEngine;
import org.json.JSONException;


/**
 * An implementation of {@link ClusterParticipant} that registers registers as a participant to a Helix cluster.
 */
class HelixParticipant implements ClusterParticipant {
  private final String clusterName;
  private final String zkConnectStr;
  private final HelixFactory helixFactory;
  private HelixManager manager;

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
   * @throws JSONException if there is an error in parsing the JSON serialized ZK connect string config.
   */
  HelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) throws IOException {
    clusterName = clusterMapConfig.clusterMapClusterName;
    this.helixFactory = helixFactory;
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Clustername is empty in clusterMapConfig");
    }
    try {
      zkConnectStr = ClusterMapUtils.parseZkJsonAndPopulateZkInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
          .get(clusterMapConfig.clusterMapDatacenterName);
    } catch (JSONException e) {
      throw new IOException("Received JSON exception while parsing ZKInfo json string", e);
    }
  }

  /**
   * Initialize the participant by registering via the {@link HelixManager} as a participant to the associated Helix
   * cluster.
   * @param hostName the hostname to use when registering as a participant.
   * @param port the port to use when registering as a participant.
   * @throws IOException if there is an error connecting to the Helix cluster.
   */
  @Override
  public void initialize(String hostName, int port) throws IOException {
    String instanceName = ClusterMapUtils.getInstanceName(hostName, port);
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory());
    try {
      manager.connect();
    } catch (Exception e) {
      throw new IOException("Exception while connecting to the Helix manager", e);
    }
  }

  /**
   * Disconnect from the {@link HelixManager}.
   */
  @Override
  public void close() {
    if (manager != null) {
      manager.disconnect();
      manager = null;
    }
  }
}

