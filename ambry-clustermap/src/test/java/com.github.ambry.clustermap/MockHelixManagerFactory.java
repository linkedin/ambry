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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
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
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class MockHelixManagerFactory extends HelixFactory {
  private final MockHelixManager helixManager;

  /**
   * Construct this factory.
   */
  public MockHelixManagerFactory() {
    helixManager = new MockHelixManager();
  }

  /**
   * Return the {@link MockHelixManager}
   * @param clusterName unused.
   * @param instanceName unused.
   * @param instanceType unused.
   * @param zkAddr unused.
   * @return the {@link MockHelixManager}
   */
  @Override
  public HelixManager getZKHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddr) {
    return helixManager;
  }

  /**
   * Return the {@link MockHelixAdmin}
   * @param zkAddr unused.
   * @return the {@link MockHelixAdmin}
   */
  @Override
  public HelixAdmin getHelixAdmin(String zkAddr) {
    return new MockHelixAdmin();
  }

  public MockHelixManager getHelixManager() {
    return helixManager;
  }

  /**
   * A mock implementation of the {@link HelixManager} for exclusive use for testing the {@link HelixParticipant}
   */
  static class MockHelixManager implements HelixManager {
    private String stateModelDef;
    private StateModelFactory stateModelFactory;
    private boolean isConnected;
    boolean beBad;
    private final HelixAdmin helixAdmin = new MockHelixAdmin();

    @Override
    public StateMachineEngine getStateMachineEngine() {
      return new StateMachineEngine() {
        @Override
        public boolean registerStateModelFactory(String stateModelDef,
            StateModelFactory<? extends StateModel> factory) {
          MockHelixManager.this.stateModelDef = stateModelDef;
          stateModelFactory = factory;
          return true;
        }

        @Override
        public boolean registerStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory,
            String factoryName) {
          throw new IllegalStateException("Not implemented");
        }

        @Override
        public boolean removeStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory) {
          return false;
        }

        @Override
        public boolean removeStateModelFactory(String stateModelDef, StateModelFactory<? extends StateModel> factory,
            String factoryName) {
          return false;
        }

        @Override
        public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName) {
          return null;
        }

        @Override
        public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName, String factoryName) {
          return null;
        }

        @Override
        public MessageHandler createHandler(Message message, NotificationContext context) {
          return null;
        }

        @Override
        public String getMessageType() {
          return null;
        }

        @Override
        public List<String> getMessageTypes() {
          return null;
        }

        @Override
        public void reset() {

        }
      };
    }

    @Override
    public Long getSessionStartTime() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void connect() throws Exception {
      if (beBad) {
        throw new IOException("Being bad");
      }

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

    String getStateModelDef() {
      return stateModelDef;
    }

    StateModelFactory getStateModelFactory() {
      return stateModelFactory;
    }

    //****************************
    // Not implemented.
    //****************************

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addIdealStateChangeListener(
        org.apache.helix.api.listeners.IdealStateChangeListener idealStateChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addLiveInstanceChangeListener(
        org.apache.helix.api.listeners.LiveInstanceChangeListener liveInstanceChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener configChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addInstanceConfigChangeListener(
        org.apache.helix.api.listeners.InstanceConfigChangeListener instanceConfigChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
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
    public void addConfigChangeListener(
        org.apache.helix.api.listeners.ScopedConfigChangeListener scopedConfigChangeListener,
        HelixConfigScope.ConfigScopeProperty configScopeProperty) throws Exception {

    }

    @Override
    public void addConfigChangeListener(ScopedConfigChangeListener listener, HelixConfigScope.ConfigScopeProperty scope)
        throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addMessageListener(org.apache.helix.api.listeners.MessageListener messageListener, String s)
        throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addCurrentStateChangeListener(
        org.apache.helix.api.listeners.CurrentStateChangeListener currentStateChangeListener, String s, String s1)
        throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName,
        String sessionId) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addExternalViewChangeListener(
        org.apache.helix.api.listeners.ExternalViewChangeListener externalViewChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addTargetExternalViewChangeListener(
        org.apache.helix.api.listeners.ExternalViewChangeListener externalViewChangeListener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerListener(
        org.apache.helix.api.listeners.ControllerChangeListener controllerChangeListener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerMessageListener(org.apache.helix.api.listeners.MessageListener messageListener) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addControllerMessageListener(MessageListener listener) {
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
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public ConfigAccessor getConfigAccessor() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getClusterName() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getMetadataStoreConnectionString() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public String getInstanceName() {
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
    public HelixAdmin getClusterManagmentTool() {
      return helixAdmin;
    }

    @Override
    public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public ClusterMessagingService getMessagingService() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public InstanceType getInstanceType() {
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
}
