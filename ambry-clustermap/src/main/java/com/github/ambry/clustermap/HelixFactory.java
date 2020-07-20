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
  Map<ManagerKey, HelixManager> helixManagers = new ConcurrentHashMap<>();

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
        k -> HelixManagerFactory.getZKHelixManager(clusterName, instanceName, instanceType, zkAddr));
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
   * Hashable key used to cache instances of {@link HelixManager} that match desired parameters.
   */
  private static class ManagerKey {
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
      return Objects.equals(clusterName, that.clusterName) && Objects.equals(instanceName, that.instanceName)
          && instanceType == that.instanceType && Objects.equals(zkAddr, that.zkAddr);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterName, instanceName, instanceType, zkAddr);
    }
  }
}
