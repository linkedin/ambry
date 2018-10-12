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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;


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
  private final MockHelixAdmin mockAdmin;
  private final Exception beBadException;
  private ZNRecord record;
  @Mock
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStore;

  /**
   * Instantiate a MockHelixManager.
   * @param instanceName the name of the instance associated with this manager.
   * @param instanceType the {@link InstanceType} of the requester.
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @param helixCluster the {@link MockHelixCluster} associated with this manager.
   * @param znRecord the {@link ZNRecord} associated with HelixPropertyStore in this manager.
   * @param beBadException the {@link Exception} that this manager will throw on listener registrations.
   */
  MockHelixManager(String instanceName, InstanceType instanceType, String zkAddr, MockHelixCluster helixCluster,
      ZNRecord znRecord, Exception beBadException) {
    this.instanceName = instanceName;
    this.instanceType = instanceType;
    mockAdmin = helixCluster.getHelixAdminFactory().getHelixAdmin(zkAddr);
    mockAdmin.addHelixManager(this);
    clusterName = helixCluster.getClusterName();
    record = znRecord;
    this.beBadException = beBadException;

    MockitoAnnotations.initMocks(this);
    Mockito.when(helixPropertyStore.get(eq(ClusterMapUtils.ZNODE_PATH), eq(null), eq(AccessOption.PERSISTENT)))
        .thenReturn(record);
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
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    if (beBadException != null) {
      throw beBadException;
    }
    liveInstanceChangeListener = listener;
    triggerLiveInstanceNotification(true);
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
    if (beBadException != null) {
      throw beBadException;
    }
    instanceConfigChangeListener = listener;
    triggerConfigChangeNotification(true);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    if (beBadException != null) {
      throw beBadException;
    }
    externalViewChangeListener = listener;
    NotificationContext notificationContext = new NotificationContext(this);
    notificationContext.setType(NotificationContext.Type.INIT);
    externalViewChangeListener.onExternalViewChange(Collections.EMPTY_LIST, notificationContext);
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

  //****************************
  // Not implemented.
  //****************************
  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, HelixConfigScope.ConfigScopeProperty scope)
      throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName, String sessionId)
      throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    throw new IllegalStateException("Not implemented");
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
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return helixPropertyStore;
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

