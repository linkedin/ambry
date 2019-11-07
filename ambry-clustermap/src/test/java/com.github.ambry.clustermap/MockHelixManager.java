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

import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.NotificationContext;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * A mock implementation of the {@link HelixManager} to use in tests.
 */
class MockHelixManager implements HelixManager {
  private final String clusterName;
  private final String instanceName;
  private final InstanceType instanceType;
  private boolean isConnected = false;
  private LiveInstanceChangeListener liveInstanceChangeListener;
  private ExternalViewChangeListener externalViewChangeListener;
  private InstanceConfigChangeListener instanceConfigChangeListener;
  private IdealStateChangeListener idealStateChangeListener;
  private RoutingTableProvider routingTableProvider;
  private final MockHelixAdmin mockAdmin;
  private final MockHelixDataAccessor dataAccessor;
  private final Exception beBadException;
  private final Map<String, ZNRecord> znRecordMap;
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStore;

  /**
   * Instantiate a MockHelixManager.
   * @param instanceName the name of the instance associated with this manager.
   * @param instanceType the {@link InstanceType} of the requester.
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @param helixCluster the {@link MockHelixCluster} associated with this manager.
   * @param znRecordMap a map that contains ZNode path and its {@link ZNRecord} associated with HelixPropertyStore in this manager.
   * @param beBadException the {@link Exception} that this manager will throw on listener registrations.
   */
  MockHelixManager(String instanceName, InstanceType instanceType, String zkAddr, MockHelixCluster helixCluster,
      Map<String, ZNRecord> znRecordMap, Exception beBadException) {
    this.instanceName = instanceName;
    this.instanceType = instanceType;
    mockAdmin = helixCluster.getHelixAdminFactory().getHelixAdmin(zkAddr);
    mockAdmin.addHelixManager(this);
    clusterName = helixCluster.getClusterName();
    dataAccessor = new MockHelixDataAccessor(clusterName, mockAdmin);
    this.beBadException = beBadException;
    this.znRecordMap = znRecordMap;
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path",
        "/" + clusterName + "/" + ClusterMapUtils.PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    helixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) CommonUtils.createHelixPropertyStore(zkAddr, propertyStoreConfig,
            Collections.singletonList(propertyStoreConfig.rootPath));
    if (znRecordMap != null) {
      for (Map.Entry<String, ZNRecord> znodePathAndRecord : znRecordMap.entrySet()) {
        helixPropertyStore.set(znodePathAndRecord.getKey(), znodePathAndRecord.getValue(), AccessOption.PERSISTENT);
      }
    }
  }

  @Override
  public void connect() throws Exception {
    isConnected = true;
  }

  @Override
  public boolean isConnected() {
    return isConnected;
  }

  @Override
  public void disconnect() {
    isConnected = false;
    if (znRecordMap != null) {
      for (String znodePath : znRecordMap.keySet()) {
        helixPropertyStore.remove(znodePath, AccessOption.PERSISTENT);
      }
    }
  }

  @Override
  public void addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener listener) throws Exception {
    // deprecated LiveInstanceChangeListener is no longer supported in testing
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addInstanceConfigChangeListener(org.apache.helix.InstanceConfigChangeListener listener) throws Exception {
    // deprecated InstanceConfigChangeListener is no longer supported in testing
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener listener) throws Exception {
    // deprecated ExternalViewChangeListener is no longer supported in testing
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getClusterName() {
    return clusterName;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    return mockAdmin;
  }

  @Override
  public InstanceType getInstanceType() {
    return instanceType;
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return helixPropertyStore;
  }

  /**
   * Trigger a live instance change notification.
   */
  void triggerLiveInstanceNotification(boolean init) {
    List<LiveInstance> liveInstances = new ArrayList<>();
    for (String instance : mockAdmin.getUpInstances()) {
      liveInstances.add(new LiveInstance(instance));
    }
    NotificationContext notificationContext = new NotificationContext(this);
    if (init) {
      notificationContext.setType(NotificationContext.Type.INIT);
    }
    liveInstanceChangeListener.onLiveInstanceChange(liveInstances, notificationContext);
  }

  void triggerConfigChangeNotification(boolean init) {
    if (!isConnected) {
      return;
    }
    NotificationContext notificationContext = new NotificationContext(this);
    if (init) {
      notificationContext.setType(NotificationContext.Type.INIT);
    }
    instanceConfigChangeListener.onInstanceConfigChange(mockAdmin.getInstanceConfigs(clusterName), notificationContext);
  }

  void triggerIdealStateNotification(boolean init) throws InterruptedException {
    NotificationContext notificationContext = new NotificationContext(this);
    if (init) {
      notificationContext.setType(NotificationContext.Type.INIT);
    }
    idealStateChangeListener.onIdealStateChange(mockAdmin.getIdealStates(), notificationContext);
  }

  void triggerRoutingTableNotification() {
    NotificationContext notificationContext = new NotificationContext(this);
    routingTableProvider.onStateChange(instanceName, Collections.emptyList(), notificationContext);
  }

  //****************************
  // Not implemented.
  //****************************
  @Override
  public void addIdealStateChangeListener(org.apache.helix.IdealStateChangeListener listener) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener idealStateChangeListener) throws Exception {
    if (beBadException != null) {
      throw beBadException;
    }
    this.idealStateChangeListener = idealStateChangeListener;
    triggerIdealStateNotification(true);
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener liveInstanceChangeListener) throws Exception {
    this.liveInstanceChangeListener = liveInstanceChangeListener;
    triggerLiveInstanceNotification(false);
  }

  @Override
  public void addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener listener,
      HelixConfigScope.ConfigScopeProperty scope) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addConfigChangeListener(org.apache.helix.api.listeners.ConfigChangeListener configChangeListener)
      throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener instanceConfigChangeListener)
      throws Exception {
    this.instanceConfigChangeListener = instanceConfigChangeListener;
    triggerConfigChangeNotification(true);
  }

  @Override
  public void addResourceConfigChangeListener(ResourceConfigChangeListener resourceConfigChangeListener)
      throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addClusterfigChangeListener(ClusterConfigChangeListener clusterConfigChangeListener) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener scopedConfigChangeListener,
      HelixConfigScope.ConfigScopeProperty configScopeProperty) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addMessageListener(MessageListener messageListener, String s) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addMessageListener(org.apache.helix.MessageListener listener, String instanceName) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener currentStateChangeListener, String s, String s1)
      throws Exception {
    // Note: routingTableProvider implements current state change listener
    this.routingTableProvider = (RoutingTableProvider) currentStateChangeListener;
  }

  @Override
  public void addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener listener, String instanceName,
      String sessionId) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener externalViewChangeListener) throws Exception {
    if (beBadException != null) {
      throw beBadException;
    }
    this.externalViewChangeListener = externalViewChangeListener;
    NotificationContext notificationContext = new NotificationContext(this);
    notificationContext.setType(NotificationContext.Type.INIT);
    this.externalViewChangeListener.onExternalViewChange(Collections.emptyList(), notificationContext);
  }

  @Override
  public void addTargetExternalViewChangeListener(ExternalViewChangeListener externalViewChangeListener)
      throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerListener(org.apache.helix.ControllerChangeListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerListener(ControllerChangeListener controllerChangeListener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerMessageListener(MessageListener messageListener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerMessageListener(org.apache.helix.MessageListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setEnabledControlPipelineTypes(Set<Pipeline.Type> types) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    return dataAccessor;
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getSessionId() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public long getLastNotificationTime() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getMetadataStoreConnectionString() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getVersion() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public HelixManagerProperties getProperties() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Long getSessionStartTime() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean isLeader() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void startTimerTasks() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void stopTimerTasks() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    throw new IllegalStateException("Not implemented");
  }
}

