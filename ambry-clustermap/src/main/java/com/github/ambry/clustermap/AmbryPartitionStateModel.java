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
import java.util.concurrent.ConcurrentMap;
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
  private final ConcurrentMap<String, String> partitionNameToResourceName;

  AmbryPartitionStateModel(String resourceName, String partitionName,
      PartitionStateChangeListener partitionStateChangeListener, ClusterMapConfig clusterMapConfig,
      ConcurrentMap<String, String> partitionNameToResourceName) {
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.partitionStateChangeListener = Objects.requireNonNull(partitionStateChangeListener);
    this.clusterMapConfig = Objects.requireNonNull(clusterMapConfig);
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(AmbryPartitionStateModel.class);
    this.partitionNameToResourceName = partitionNameToResourceName;
  }

  @Transition(to = "BOOTSTRAP", from = "OFFLINE")
  public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming BOOTSTRAP from OFFLINE, Should Transition? {}",
        message.getPartitionName(), message.getResourceName(), shouldTransition);
    if (clusterMapConfig.clustermapEnableStateModelListener && shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeBootstrapFromOffline(partitionName);
    }
  }

  @Transition(to = "STANDBY", from = "BOOTSTRAP")
  public void onBecomeStandbyFromBootstrap(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming STANDBY from BOOTSTRAP, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (clusterMapConfig.clustermapEnableStateModelListener && shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeStandbyFromBootstrap(partitionName);
    }
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming LEADER from STANDBY, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeLeaderFromStandby(partitionName);
    }
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming STANDBY from LEADER, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeStandbyFromLeader(partitionName);
    }
  }

  @Transition(to = "INACTIVE", from = "STANDBY")
  public void onBecomeInactiveFromStandby(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming INACTIVE from STANDBY, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (clusterMapConfig.clustermapEnableStateModelListener && shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeInactiveFromStandby(partitionName);
    }
  }

  @Transition(to = "OFFLINE", from = "INACTIVE")
  public void onBecomeOfflineFromInactive(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming OFFLINE from INACTIVE, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (clusterMapConfig.clustermapEnableStateModelListener && shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeOfflineFromInactive(partitionName);
    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming DROPPED from OFFLINE, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (clusterMapConfig.clustermapEnableStateModelListener && shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeDroppedFromOffline(partitionName);
    }
  }

  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming DROPPED from ERROR, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeDroppedFromError(partitionName);
    }
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    boolean shouldTransition = shouldTransition(message);
    String partitionName = message.getPartitionName();
    logger.info("Partition {} in resource {} is becoming OFFLINE from ERROR, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
    if (shouldTransition) {
      partitionStateChangeListener.onPartitionBecomeOfflineFromError(partitionName);
    }
  }

  @Override
  public void reset() {
    logger.info("Reset method invoked. Partition {} in resource {} is reset to OFFLINE", partitionName, resourceName);
    partitionStateChangeListener.onReset(partitionName);
  }

  /**
   * Return true if we should transition the state from the message. Ambry is undergoing resource reconstruction. Same
   * partition will be added to different resources. Resource that has higher id should be considered as the primary
   * resource.
   * @param message Message that carry partition information.
   * @return True if a state transition should happen.
   */
  boolean shouldTransition(Message message) {
    String resourceName = message.getResourceName();
    String partitionName = message.getPartitionName();
    String mappedResourceName = partitionNameToResourceName.compute(partitionName, (k, v) -> {
      if (v == null || v.equals(resourceName)) {
        return resourceName;
      }
      try {
        int oldResourceId = Integer.valueOf(v);
        int newResourceId = Integer.valueOf(resourceName);
        return newResourceId > oldResourceId ? resourceName : v;
      } catch (Exception e) {
        return resourceName;
      }
    });
    return mappedResourceName.equals(resourceName);
  }
}
