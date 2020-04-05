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
  private final HelixParticipantMetrics helixParticipantMetrics;

  AmbryPartitionStateModel(String resourceName, String partitionName,
      PartitionStateChangeListener partitionStateChangeListener, ClusterMapConfig clusterMapConfig,
      HelixParticipantMetrics helixParticipantMetrics) {
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.partitionStateChangeListener = Objects.requireNonNull(partitionStateChangeListener);
    this.clusterMapConfig = Objects.requireNonNull(clusterMapConfig);
    this.helixParticipantMetrics = Objects.requireNonNull(helixParticipantMetrics);
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
    helixParticipantMetrics.offlineCount.addAndGet(-1);
    helixParticipantMetrics.bootstrapCount.addAndGet(1);
  }

  @Transition(to = "STANDBY", from = "BOOTSTRAP")
  public void onBecomeStandbyFromBootstrap(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming STANDBY from BOOTSTRAP", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeStandbyFromBootstrap(partitionName);
    }
    helixParticipantMetrics.bootstrapCount.addAndGet(-1);
    helixParticipantMetrics.standbyCount.addAndGet(1);
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming LEADER from STANDBY", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeLeaderFromStandby(message.getPartitionName());
    helixParticipantMetrics.standbyCount.addAndGet(-1);
    helixParticipantMetrics.leaderCount.addAndGet(1);
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming STANDBY from LEADER", message.getPartitionName(),
        message.getResourceName());
    partitionStateChangeListener.onPartitionBecomeStandbyFromLeader(message.getPartitionName());
    helixParticipantMetrics.leaderCount.addAndGet(-1);
    helixParticipantMetrics.standbyCount.addAndGet(1);
  }

  @Transition(to = "INACTIVE", from = "STANDBY")
  public void onBecomeInactiveFromStandby(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming INACTIVE from STANDBY", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeInactiveFromStandby(partitionName);
    }
    helixParticipantMetrics.standbyCount.addAndGet(-1);
    helixParticipantMetrics.inactiveCount.addAndGet(1);
  }

  @Transition(to = "OFFLINE", from = "INACTIVE")
  public void onBecomeOfflineFromInactive(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming OFFLINE from INACTIVE", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeOfflineFromInactive(partitionName);
    }
    helixParticipantMetrics.inactiveCount.addAndGet(-1);
    helixParticipantMetrics.offlineCount.addAndGet(1);
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming DROPPED from OFFLINE", partitionName,
        message.getResourceName());
    if (clusterMapConfig.clustermapEnableStateModelListener) {
      partitionStateChangeListener.onPartitionBecomeDroppedFromOffline(partitionName);
    }
    helixParticipantMetrics.offlineCount.addAndGet(-1);
    helixParticipantMetrics.partitionDroppedCount.inc();
  }

  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    logger.info("Partition {} in resource {} is becoming DROPPED from ERROR", message.getPartitionName(),
        message.getResourceName());
    helixParticipantMetrics.partitionDroppedCount.inc();
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context){
    logger.info("Partition {} in resource {} is becoming OFFLINE from ERROR", message.getPartitionName(),
        message.getResourceName());
    helixParticipantMetrics.offlineCount.addAndGet(1);
  }

  @Override
  public void reset() {
    logger.info("Reset method invoked. Partition {} in resource {} is reset to OFFLINE", partitionName, resourceName);
    helixParticipantMetrics.offlineCount.addAndGet(1);
  }
}
