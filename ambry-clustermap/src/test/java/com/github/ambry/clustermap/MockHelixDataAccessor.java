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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;


/**
 * A class that mocks {@link HelixDataAccessor} to help with {@link org.apache.helix.spectator.RoutingTableProvider}
 * creation and any state changes within cluster. Some methods are hard coded to directly return result we need.
 */
public class MockHelixDataAccessor implements HelixDataAccessor {
  // The session id will be converted to hex string in LiveInstance.getEphemeralOwner(). Hence the hex string of 1024
  // is 0x400.
  private static final long SESSION_ID = 1024L;
  private final String LIVEINSTANCE_PATH;
  private final String INSTANCECONFIG_PATH;
  private final String EXTERNALVIEW_PATH;
  private final boolean isAggregatedViewCluster;
  private final String clusterName;
  private final PropertyKey.Builder propertyKeyBuilder;
  private final MockHelixAdmin mockLocalHelixAdmin;
  private final List<MockHelixAdmin> mockHelixAdminList;
  private Map<PropertyKey, HelixProperty> properties = new ConcurrentHashMap<>();

  MockHelixDataAccessor(String clusterName, MockHelixAdmin mockLocalHelixAdmin, List<MockHelixAdmin> mockHelixAdminList,
      boolean isAggregatedViewCluster) {
    this.clusterName = clusterName;
    this.mockLocalHelixAdmin = mockLocalHelixAdmin;
    propertyKeyBuilder = new PropertyKey.Builder(clusterName);
    LIVEINSTANCE_PATH = "/" + clusterName + "/LIVEINSTANCES";
    INSTANCECONFIG_PATH = "/" + clusterName + "/CONFIGS/PARTICIPANT";
    EXTERNALVIEW_PATH = "/" + clusterName + "/EXTERNALVIEW";
    this.mockHelixAdminList = mockHelixAdminList;
    this.isAggregatedViewCluster = isAggregatedViewCluster;
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
    properties.put(key, value);
    return true;
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
    return (T) getProperty(Collections.singletonList(key), false).get(0);
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException) {
    List<T> result = new ArrayList<>();
    for (PropertyKey key : keys) {
      if (key.toString().matches("/Ambry-/INSTANCES/.*/CURRENTSTATES/\\d+/\\d+")) {
        // an example for the key: /Ambry-/INSTANCES/localhost_18089/CURRENTSTATES/sessionId/0
        String[] segments = key.toString().split("/");
        String instanceName = segments[3];
        String resourceName = segments[6];
        Map<String, Map<String, String>> partitionStateMap =
            mockLocalHelixAdmin.getPartitionStateMapForInstance(instanceName);
        ZNRecord record = new ZNRecord(resourceName);
        record.setMapFields(partitionStateMap);
        result.add((T) (new CurrentState(record)));
      } else if (key.toString().matches("/Ambry-/LIVEINSTANCES/.*_\\d+")) {
        String[] segments = key.toString().split("/");
        String instanceName = segments[3];
        ZNRecord record = new ZNRecord(instanceName);
        record.setEphemeralOwner(SESSION_ID);
        result.add((T) (new LiveInstance(record)));
      } else if (key.toString().matches("/Ambry-/CONFIGS/PARTICIPANT/.*_\\d+")) {
        String[] segments = key.toString().split("/");
        String instanceName = segments[4];
        if (isAggregatedViewCluster) {
          boolean foundInstance = false;
          for (MockHelixAdmin mockHelixAdmin : mockHelixAdminList) {
            for (InstanceConfig instanceConfig : mockHelixAdmin.getInstanceConfigs(clusterName)) {
              if (instanceConfig.getInstanceName().equals(instanceName)) {
                result.add((T) instanceConfig);
                foundInstance = true;
                break;
              }
            }
            if (foundInstance) {
              break;
            }
          }
        } else {
          InstanceConfig instanceConfig = mockLocalHelixAdmin.getInstanceConfigs(clusterName)
              .stream()
              .filter(config -> config.getInstanceName().equals(instanceName))
              .findFirst()
              .get();
          result.add((T) instanceConfig);
        }
      } else if (key.toString().matches("/Ambry-/EXTERNALVIEW/\\d+")) {
        // Add external view for the asked resource
        String[] segments = key.toString().split("/");
        String resourceName = segments[3];
        ZNRecord record = new ZNRecord(resourceName);
        if (isAggregatedViewCluster) {
          // If aggregated view is being used, go through all mock helix clusters and calculate the aggregated mapping
          Map<String, Map<String, String>> aggregatedPartitionToReplicasMap = new HashMap<>();
          for (MockHelixAdmin mockHelixAdmin : mockHelixAdminList) {
            Map<String, Map<String, String>> partitionToReplicasMap =
                mockHelixAdmin.getPartitionToReplicasMapForResource(resourceName);
            for (Map.Entry<String, Map<String, String>> entry : partitionToReplicasMap.entrySet()) {
              String partition = entry.getKey();
              Map<String, String> replicaToState = entry.getValue();
              Map<String, String> aggregatedReplicaToState =
                  aggregatedPartitionToReplicasMap.computeIfAbsent(partition, k -> new HashMap<>());
              aggregatedReplicaToState.putAll(replicaToState);
            }
          }
          record.setMapFields(aggregatedPartitionToReplicasMap);
        } else {
          Map<String, Map<String, String>> partitionToReplicasMap =
              mockLocalHelixAdmin.getPartitionToReplicasMapForResource(resourceName);
          record.setMapFields(partitionToReplicasMap);
        }
        result.add((T) new ExternalView(record));
      } else {
        result.add((T) properties.get(key));
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
    if (key.toString().endsWith("/CURRENTSTATES/" + Long.toHexString(SESSION_ID))) {
      // Add resource name into result. Note that, in current test setup, all partitions within same dc are under same resource.
      result.add(mockLocalHelixAdmin.getResourcesInCluster(clusterName).get(0));
    } else if (key.toString().equals(LIVEINSTANCE_PATH)) {
      if (isAggregatedViewCluster) {
        for (MockHelixAdmin mockHelixAdmin : mockHelixAdminList) {
          result.addAll(mockHelixAdmin.getUpInstances());
        }
      } else {
        result.addAll(mockLocalHelixAdmin.getUpInstances());
      }
    } else if (key.toString().equals(INSTANCECONFIG_PATH)) {
      if (isAggregatedViewCluster) {
        for (MockHelixAdmin mockHelixAdmin : mockHelixAdminList) {
          for (InstanceConfig config : mockHelixAdmin.getInstanceConfigs(clusterName)) {
            result.add(config.getInstanceName());
          }
        }
      } else {
        for (InstanceConfig config : mockLocalHelixAdmin.getInstanceConfigs(clusterName)) {
          result.add(config.getInstanceName());
        }
      }
    } else if (key.toString().equals(EXTERNALVIEW_PATH)) {
      if (isAggregatedViewCluster) {
        // return resources from all DCs. But use set to avoid adding resources twice.
        Set<String> resources = new HashSet<>();
        for (MockHelixAdmin mockHelixAdmin : mockHelixAdminList) {
          resources.addAll(mockHelixAdmin.getResourcesInCluster(clusterName));
        }
        result.addAll(resources);
      } else {
        result.addAll(mockLocalHelixAdmin.getResourcesInCluster(clusterName));
      }
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
    return Collections.emptyMap();
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
