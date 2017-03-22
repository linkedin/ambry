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
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Test for {@link HelixParticipant}
 */
public class HelixParticipantTest {
  private final MockHelixManagerFactory helixManagerFactory;
  private final Properties props;

  public HelixParticipantTest() throws Exception {
    List<ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new ZkInfo(null, "DC0", 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "HelixParticipantTestCluster");
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    helixManagerFactory = new MockHelixManagerFactory();
  }

  /**
   * Test bad instantiation and initialization scenarios of the {@link HelixParticipant}
   * @throws IOException
   */
  @Test
  public void testBadCases() throws IOException {
    // Connect failure.
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManagerFactory.helixManager.beBad = true;
    HelixParticipant helixParticipant = new HelixParticipant(clusterMapConfig, helixManagerFactory);
    try {
      helixParticipant.initialize("localhost", 2200);
      fail("Initialization should have failed");
    } catch (IOException e) {
      // OK
    }

    // Bad param during instantiation.
    props.setProperty("clustermap.cluster.name", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixParticipant(clusterMapConfig, helixManagerFactory);
      fail("Instantiation should have failed");
    } catch (IllegalStateException e) {
      // OK
    }

    props.setProperty("clustermap.cluster.name", "HelixParticipantTestCluster");
    props.setProperty("clustermap.dcs.zk.connect.strings", "");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      new HelixParticipant(clusterMapConfig, helixManagerFactory);
      fail("Instantiation should have failed");
    } catch (IOException e) {
      // OK
    }
  }

  /**
   * Test the good path of instantiation, initialization and termination of the {@link HelixParticipant}
   * @throws Exception
   */
  @Test
  public void testHelixParticipant() throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant participant = new HelixParticipant(clusterMapConfig, helixManagerFactory);
    participant.initialize("localhost", 2200);
    MockHelixManager helixManager = helixManagerFactory.helixManager;
    assertTrue(helixManager.isConnected());
    assertEquals(LeaderStandbySMD.name, helixManager.stateModelDef);
    assertEquals(AmbryStateModelFactory.class, helixManager.stateModelFactory.getClass());
    participant.terminate();
    assertFalse(helixManager.isConnected());
  }

  /**
   * A Mock implementaion of {@link HelixFactory} that returns the {@link MockHelixManager}
   */
  private static class MockHelixManagerFactory extends HelixFactory {
    private final MockHelixManager helixManager;

    /**
     * Construct this factory.
     */
    MockHelixManagerFactory() {
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
    HelixManager getZKHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddr) {
      return helixManager;
    }
  }

  /**
   * A mock implementation of the {@link HelixManager} for exclusive use for testing the {@link HelixParticipant}
   */
  private static class MockHelixManager implements HelixManager {
    private String stateModelDef;
    private StateModelFactory stateModelFactory;
    private boolean isConnected;
    boolean beBad;

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
        public MessageHandler createHandler(Message message, NotificationContext context) {
          return null;
        }

        @Override
        public String getMessageType() {
          return null;
        }

        @Override
        public void reset() {

        }
      };
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

    //****************************
    // Not implemented.
    //****************************

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
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
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName,
        String sessionId) throws Exception {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
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
    public String getClusterName() {
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
      throw new IllegalStateException("Not implemented");
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
