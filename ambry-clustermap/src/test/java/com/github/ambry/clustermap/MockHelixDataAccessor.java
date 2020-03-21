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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;


/**
 * A class that mocks {@link HelixDataAccessor} to help with {@link org.apache.helix.spectator.RoutingTableProvider}
 * creation and any state changes within cluster. Some methods are hard coded to directly return result we need.
 */
public class MockHelixDataAccessor implements HelixDataAccessor {
  private static final String SESSION_ID = "sessionId";
  private final String LIVEINSTANCE_PATH;
  private final String INSTANCECONFIG_PATH;
  private final String clusterName;
  private final PropertyKey.Builder propertyKeyBuilder;
  private final MockHelixAdmin mockHelixAdmin;

  MockHelixDataAccessor(String clusterName, MockHelixAdmin mockHelixAdmin) {
    this.clusterName = clusterName;
    this.mockHelixAdmin = mockHelixAdmin;
    propertyKeyBuilder = new PropertyKey.Builder(clusterName);
    LIVEINSTANCE_PATH = "/" + clusterName + "/LIVEINSTANCES";
    INSTANCECONFIG_PATH = "/" + clusterName + "/CONFIGS/PARTICIPANT";
  }

  @Override
  public boolean createStateModelDef(StateModelDefinition stateModelDef) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createControllerMessage(Message message) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createControllerLeader(LiveInstance leader) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createPause(PauseSignal pauseSignal) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createMaintenance(MaintenanceSignal maintenanceSignal) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, DataUpdater<ZNRecord> updater, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    //return (T) (new HelixProperty("id"));
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException) {
    List<T> result = new ArrayList<>();
    // example for key
    for (PropertyKey key : keys) {
      if (key.toString().matches("/Ambry-/INSTANCES/.*/CURRENTSTATES/sessionId/\\d+")) {
        // an example for the key: /Ambry-/INSTANCES/localhost_18089/CURRENTSTATES/sessionId/0
        String[] segments = key.toString().split("/");
        String instanceName = segments[3];
        String resourceName = segments[6];
        Map<String, Map<String, String>> partitionStateMap =
            mockHelixAdmin.getPartitionStateMapForInstance(instanceName);
        ZNRecord record = new ZNRecord(resourceName);
        record.setMapFields(partitionStateMap);
        result.add((T) (new CurrentState(record)));
      }
    }
    return result;
  }

  @Override
  public boolean removeProperty(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public HelixProperty.Stat getPropertyStat(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public List<HelixProperty.Stat> getPropertyStats(List<PropertyKey> keys) {
    List<HelixProperty.Stat> result = new ArrayList<>();
    for (PropertyKey key : keys) {
      // Adding null forces AbstractDataCache to reload PropertyKey from ZK
      result.add(null);
    }
    return result;
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    List<String> result = new ArrayList<>();
    if (key.toString().endsWith("/CURRENTSTATES/" + SESSION_ID)) {
      // Add resource name into result. Note that, in current test setup, all partitions within same dc are under same resource.
      result.add(mockHelixAdmin.getResourcesInCluster(clusterName).get(0));
    }
    return result;
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key, boolean throwException) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key, boolean throwException) {
    Map<String, T> result = new HashMap<>();
    if (key.toString().equals(LIVEINSTANCE_PATH)) {
      for (String instance : mockHelixAdmin.getUpInstances()) {
        LiveInstance liveInstance = new LiveInstance(instance);
        liveInstance.setSessionId(SESSION_ID);
        result.put(instance, (T) liveInstance);
      }
    } else if (key.toString().equals(INSTANCECONFIG_PATH)) {
      for (InstanceConfig config : mockHelixAdmin.getInstanceConfigs(clusterName)) {
        result.put(config.getInstanceName(), (T) config);
      }
    }
    return result;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys, List<T> children) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths, List<DataUpdater<ZNRecord>> updaters,
      int options) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public PropertyKey.Builder keyBuilder() {
    return propertyKeyBuilder;
  }

  @Override
  public BaseDataAccessor<ZNRecord> getBaseDataAccessor() {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }
}
