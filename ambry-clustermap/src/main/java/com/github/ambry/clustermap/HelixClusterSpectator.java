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

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * Spectator for vcr helix cluster.
 */
public class HelixClusterSpectator implements ClusterSpectator {

  private final ClusterMapConfig clusterMapConfig;
  private final CloudConfig cloudConfig;
  private final List<InstanceConfigChangeListener> registeredInstanceConfigChangeListeners;
  private final List<LiveInstanceChangeListener> registeredLiveInstanceChangeListeners;

  /**
   * Constructor for {@link HelixClusterSpectator}.
   * @param cloudConfig Cluster config of vcr.
   * @param clusterMapConfig Cluster Map config.
   */
  public HelixClusterSpectator(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig) {
    this.cloudConfig = cloudConfig;
    this.clusterMapConfig = clusterMapConfig;
    registeredInstanceConfigChangeListeners = new LinkedList<>();
    registeredLiveInstanceChangeListeners = new LinkedList<>();
  }

  @Override
  public void spectate() throws Exception {
    Map<String, DcZkInfo> dataCenterToZkAddress =
        parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings);
    HelixFactory helixFactory = new HelixFactory();
    String selfInstanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);

    // Should we fail here if even one of the remote zk connection fails? If we have just one datacenter, then this will not be a problem.
    // If we have two data centers, then its not clear if we should pass the startup with one remote zk connection failure. Because if remote
    // zk connection fails on both data centers, then things like replication between data centers might just stop.
    // For now, since we have only one fabric in cloud, and the spectator is being used for only cloud to store replication, this will work.
    // Once we add more fabrics, we should revisit this.
    for (DcZkInfo dcZkInfo : dataCenterToZkAddress.values()) {
      // only handle vcr clusters for now
      if (dcZkInfo.getReplicaType() == ReplicaType.CLOUD_BACKED) {
        HelixManager helixManager =
            helixFactory.getZKHelixManager(cloudConfig.vcrClusterName, selfInstanceName, InstanceType.SPECTATOR,
                dcZkInfo.getZkConnectStrs().get(0));
        helixManager.connect();

        helixManager.addInstanceConfigChangeListener(this);
        helixManager.addLiveInstanceChangeListener(this);
      }
    }
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    for (InstanceConfigChangeListener registeredInstanceConfigChangeListener : registeredInstanceConfigChangeListeners) {
      registeredInstanceConfigChangeListener.onInstanceConfigChange(instanceConfigs, context);
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    for (LiveInstanceChangeListener registeredLiveInstanceChangeListener : registeredLiveInstanceChangeListeners) {
      registeredLiveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);
    }
  }

  @Override
  public void registerInstanceConfigChangeListener(InstanceConfigChangeListener instanceConfigChangeListener) {
    registeredInstanceConfigChangeListeners.add(instanceConfigChangeListener);
  }

  @Override
  public void registerLiveInstanceChangeListener(LiveInstanceChangeListener liveInstanceChangeListener) {
    registeredLiveInstanceChangeListeners.add(liveInstanceChangeListener);
  }
}
