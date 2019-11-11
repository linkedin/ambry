/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;


/**
 * Mock implementation of {@link ClusterSpectator} that can give callback for instance configs.
 */
public class MockClusterSpectator implements ClusterSpectator {
  private final List<InstanceConfig> instanceConfigList;
  private final List<InstanceConfigChangeListener> registeredInstanceConfigChangeListeners;

  /**
   * Constructor for {@link MockClusterSpectator} object.
   * @param cloudDataNodeList list of cloud {@link MockDataNodeId}s in instance config of {@link MockClusterSpectator}.
   */
  public MockClusterSpectator(List<MockDataNodeId> cloudDataNodeList) {
    registeredInstanceConfigChangeListeners = new ArrayList<>();
    this.instanceConfigList = new ArrayList<>();
    for (MockDataNodeId cloudDataNode : cloudDataNodeList) {
      ZNRecord znRecord = new ZNRecord(cloudDataNode.getHostname() + "_" + cloudDataNode.getPort());
      Map<String, String> simpleFields = new HashMap<>();
      simpleFields.put("HELIX_ENABLED", "true");
      simpleFields.put("HELIX_ENABLED_TIMESTAMP", Long.toString(System.currentTimeMillis()));
      simpleFields.put("HELIX_HOST", cloudDataNode.getHostname());
      simpleFields.put("HELIX_PORT", Integer.toString(cloudDataNode.getPort()));
      znRecord.setSimpleFields(simpleFields);
      instanceConfigList.add(new InstanceConfig(znRecord));
    }
  }

  @Override
  public void spectate() {
    onInstanceConfigChange(instanceConfigList, null);
  }

  @Override
  public void registerInstanceConfigChangeListener(InstanceConfigChangeListener instanceConfigChangeListener) {
    registeredInstanceConfigChangeListeners.add(instanceConfigChangeListener);
  }

  @Override
  public void registerLiveInstanceChangeListener(LiveInstanceChangeListener liveInstanceChangeListener) {
    // no op
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    for (InstanceConfigChangeListener registeredInstanceConfigChangeListener : registeredInstanceConfigChangeListeners) {
      registeredInstanceConfigChangeListener.onInstanceConfigChange(instanceConfigs, context);
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {

  }
}
