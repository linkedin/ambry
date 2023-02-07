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


/**
 * Ambry is currently going through the process of reconstructing resources. We used to assign partition ids to several
 * resources, even if those partition ids belong to the same clique. Now we are trying to gather all the partition ids
 * of the same clique to one resource.
 *
 * In doing so, we should inevitably 1. duplicate all the partition ids with a different resource 2. remove the old
 * resources.  The issue is that resource doesn't really matter in Ambry. Resource is a structure required by Helix.
 * Ambry has a flatter structure. This structure starts with a cluster. Under this cluster, it has partitions. If we
 * duplicate all the partition ids with different resources, we would force these partitions to go through state
 * transition twice. And by removing the old resource, we would effectively force all the partitions to go through
 * downward transition to DROPPED state. We have to make sure that frontend and server would be able to deal with both
 * situations.
 *
 * Current resource names are all numeric numbers starting from 0. The new resource names will be numeric numbers as
 * well, but it will be starting from a much higher number, like 1000. So we can easily find the new resource by just
 * comparing the resource names.
 *
 * When we eventually remove old resources, all the partition ids under those resourced would have to go through state
 * transition to become DROPPED in the end. A dropped partition means all the data will be removed, we don't want any
 * of the partition to remove data. So in state transition, we have to make sure after we add new resources, the state
 * transition messages from the old resources would be ignored and won't take effect anymore.
 *
 * Since in server we honor new resource over the old resource, we have to do the same thing in the clustermap.
 *
 * We have three implementations of {@link PartitionStateChangeListener}, StorageManager's implementation, ReplicationManager's
 * implementation and StatsManager's implementation. Most of the those implementations are fine with going through state
 * transition multiple times and generate the same result, except for ReplicationManager. When transitioning from standby
 * to leader and from leader to standby, replication manager would change the leader pair. If different replicas are chosen
 * to be leader in different transition, then we would have multiple leaders. But this is fine, since replication manager
 * use this leader pair information for cross-colo replication. The worst case is that we have more than one replicas doing
 * cross-colo data replication.
 */
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
    logger.info("Partition {} in resource {} is becoming BOOTSTRAP from OFFLINE, Should Transition? {}", partitionName,
        message.getResourceName(), shouldTransition);
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
        logger.error("Failed to parse resource name to an integer", e);
        throw new StateTransitionException("Failed to parse resource name",
            StateTransitionException.TransitionErrorCode.InvalidResourceName, e);
      }
    });
    return mappedResourceName.equals(resourceName);
  }
}
