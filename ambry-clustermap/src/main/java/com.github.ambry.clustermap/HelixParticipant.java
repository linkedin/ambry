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


public class HelixParticipant implements ClusterParticipant {
  private HelixManager manager;
  private final String clusterName;
  private final String zkConnectStr;

  HelixParticipant(ClusterMapConfig clusterMapConfig) {
    clusterName = clusterMapConfig.clusterMapClusterName;
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Clustername is empty in clusterMapConfig");
    }
    zkConnectStr = clusterMapConfig.clusterMapParticipantZkConnectString;
  }

  @Override
  public void initialize(String hostName, int port) throws Exception {
    String instanceName = hostName + "_" + port;
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory(zkConnectStr));
    manager.connect();
  }

  @Override
  public void terminate() {
    manager.disconnect();
  }
}

