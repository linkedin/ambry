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

import com.github.ambry.config.ClusterMapConfig;
import java.util.Objects;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@StateModelInfo(initialState = "OFFLINE", states = {"BOOTSTRAP", "LEADER", "STANDBY", "INACTIVE"})
public class AmbryPartitionStateModel extends StateModel {
  private static final Logger logger = LoggerFactory.getLogger(AmbryPartitionStateModel.class);
  private final String resourceName;
  private final String partitionName;
  private final PartitionStateChangeListener partitionStateChangeListener;
  private final ClusterMapConfig clusterMapConfig;

  AmbryPartitionStateModel(String resourceName, String partitionName,
      PartitionStateChangeListener partitionStateChangeListener, ClusterMapConfig clusterMapConfig) {
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.partitionStateChangeListener = Objects.requireNonNull(partitionStateChangeListener);
    this.clusterMapConfig = Objects.requireNonNull(clusterMapConfig);
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(AmbryPartitionStateModel.class);
  }

  @Transition(to = "BOOTSTRAP", from = "OFFLINE")
  public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming BOOTSTRAP from OFFLINE", message.getPartitionName(),
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeBootstrapFromOffline(message.getPartitionName());
    }
  }

  @Transition(to = "STANDBY", from = "BOOTSTRAP")
  public void onBecomeStandbyFromBootstrap(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming STANDBY from BOOTSTRAP", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeStandbyFromBootstrap(partitionName);
    }
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming LEADER from STANDBY", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeLeaderFromStandby(message.getPartitionName());
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming STANDBY from LEADER", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeStandbyFromLeader(message.getPartitionName());
  }

  @Transition(to = "INACTIVE", from = "STANDBY")
  public void onBecomeInactiveFromStandby(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming INACTIVE from STANDBY", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeInactiveFromStandby(partitionName);
    }
  }

  @Transition(to = "OFFLINE", from = "INACTIVE")
  public void onBecomeOfflineFromInactive(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming OFFLINE from INACTIVE", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeOfflineFromInactive(partitionName);
    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming DROPPED from OFFLINE", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeDroppedFromOffline(partitionName);
    }
  }

  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming DROPPED from ERROR", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeDroppedFromError(message.getPartitionName());
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming OFFLINE from ERROR", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeOfflineFromError(message.getPartitionName());
  }

  @Override
  public void reset() {
    logger.info("Reset method invoked. Partition {} in resource {} is reset to OFFLINE", partitionName, resourceName);
    partitionStateChangeListener.onReset(partitionName);
  }
}
