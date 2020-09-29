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
import com.github.ambry.utils.SystemTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class to construct and get a reference to a {@link HelixManager}
 */
public class HelixFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixFactory.class);
  // exposed for use in testing
  private final Map<ManagerKey, HelixManager> helixManagers = new ConcurrentHashMap<>();
  private final Map<String, DataNodeConfigSource> dataNodeConfigSources = new ConcurrentHashMap<>();

  /**
   * Get a reference to a {@link HelixManager}
   * @param clusterName the name of the cluster for which the manager is to be gotten.
   * @param instanceName the name of the instance on whose behalf the manager is to be gotten.
   * @param instanceType the {@link InstanceType} of the requester.
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @return the constructed {@link HelixManager}.
   */
  public HelixManager getZKHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddr) {
    ManagerKey managerKey = new ManagerKey(clusterName, instanceName, instanceType, zkAddr);
    return helixManagers.computeIfAbsent(managerKey,
        k -> buildZKHelixManager(clusterName, instanceName, instanceType, zkAddr));
  }

  /**
   * Get a reference to a {@link HelixManager} and connect to it, if not already connected
   * @param clusterName the name of the cluster for which the manager is to be gotten.
   * @param instanceName the name of the instance on whose behalf the manager is to be gotten.
   * @param instanceType the {@link InstanceType} of the requester.
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @return the constructed and connected {@link HelixManager}.
   * @throws Exception if connecting failed.
   */
  public HelixManager getZkHelixManagerAndConnect(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddr) throws Exception {
    HelixManager manager = getZKHelixManager(clusterName, instanceName, instanceType, zkAddr);
    synchronized (manager) {
      if (!manager.isConnected()) {
        LOGGER.info("Connecting to HelixManager at {}", zkAddr);
        manager.connect();
        LOGGER.info("Established connection to HelixManager at {}", zkAddr);
      } else {
        LOGGER.info("HelixManager at {} already connected", zkAddr);
      }
    }
    return manager;
  }

  /**
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param zkAddr the ZooKeeper address to connect to. If a {@link HelixManager} is required and one is already in the
   *               pool with this address, it will be reused.
   * @param metrics the {@link DataNodeConfigSourceMetrics} to use.
   * @return either a new instance of {@link DataNodeConfigSource} with the supplied configuration, or one from the pool
   *         if there is already one created for this address.
   */
  public DataNodeConfigSource getDataNodeConfigSource(ClusterMapConfig clusterMapConfig, String zkAddr,
      DataNodeConfigSourceMetrics metrics) {
    return dataNodeConfigSources.computeIfAbsent(zkAddr,
        k -> buildDataNodeConfigSource(clusterMapConfig, zkAddr, metrics));
  }

  /**
   * Construct a new instance of {@link HelixManager}. Exposed so that tests can override if needed.
   * @param clusterName the name of the cluster for the manager.
   * @param instanceName the name of the instance for the manager.
   * @param instanceType the {@link InstanceType} of the requester.
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @return a new instance of {@link HelixManager}.
   */
  HelixManager buildZKHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddr) {
    return HelixManagerFactory.getZKHelixManager(clusterName, instanceName, instanceType, zkAddr);
  }

  /**
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param zkAddr the ZooKeeper address to connect to. If a {@link HelixManager} is required and one is already in the
   *               pool with this address, it will be reused.
   * @param metrics the {@link DataNodeConfigSourceMetrics} to use.
   * @return a new instance of {@link DataNodeConfigSource} with the supplied configuration.
   */
  private DataNodeConfigSource buildDataNodeConfigSource(ClusterMapConfig clusterMapConfig, String zkAddr,
      DataNodeConfigSourceMetrics metrics) {
    try {
      InstanceConfigToDataNodeConfigAdapter instanceConfigSource = null;
      if (clusterMapConfig.clusterMapDataNodeConfigSourceType.isInstanceConfigAware()) {
        instanceConfigSource = new InstanceConfigToDataNodeConfigAdapter(
            getZkHelixManagerAndConnect(clusterMapConfig.clusterMapClusterName,
                ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort),
                InstanceType.SPECTATOR, zkAddr), clusterMapConfig);
      }
      PropertyStoreToDataNodeConfigAdapter propertyStoreSource = null;
      if (clusterMapConfig.clusterMapDataNodeConfigSourceType.isPropertyStoreAware()) {
        propertyStoreSource = new PropertyStoreToDataNodeConfigAdapter(zkAddr, clusterMapConfig);
      }

      switch (clusterMapConfig.clusterMapDataNodeConfigSourceType) {
        case INSTANCE_CONFIG:
          return instanceConfigSource;
        case PROPERTY_STORE:
          return propertyStoreSource;
        case COMPOSITE_INSTANCE_CONFIG_PRIMARY:
          return new CompositeDataNodeConfigSource(instanceConfigSource, propertyStoreSource, SystemTime.getInstance(),
              metrics);
        case COMPOSITE_PROPERTY_STORE_PRIMARY:
          return new CompositeDataNodeConfigSource(propertyStoreSource, instanceConfigSource, SystemTime.getInstance(),
              metrics);
        default:
          throw new IllegalArgumentException("Unknown type: " + clusterMapConfig.clusterMapDataNodeConfigSourceType);
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception while instantiating DataNodeConfigSource", e);
    }
  }

  /**
   * Hashable key used to cache instances of {@link HelixManager} that match desired parameters.
   * Exposed for use in testing.
   */
  static class ManagerKey {
    private final String clusterName;
    private final String instanceName;
    private final InstanceType instanceType;
    private final String zkAddr;

    public ManagerKey(String clusterName, String instanceName, InstanceType instanceType, String zkAddr) {
      this.clusterName = clusterName;
      this.instanceName = instanceName;
      this.instanceType = instanceType;
      this.zkAddr = zkAddr;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ManagerKey that = (ManagerKey) o;
      return Objects.equals(zkAddr, that.zkAddr) && instanceType == that.instanceType && Objects.equals(clusterName,
          that.clusterName) && Objects.equals(instanceName, that.instanceName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterName, instanceName, instanceType, zkAddr);
    }
  }
}
