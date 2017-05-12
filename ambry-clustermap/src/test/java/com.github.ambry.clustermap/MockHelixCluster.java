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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Mocks a cluster in Helix, keeps all states internally.
 */
public class MockHelixCluster {
  private final MockHelixAdminFactory helixAdminFactory;
  private final Map<String, MockHelixAdmin> helixAdmins;
  private final String clusterName;

  /**
   * Instantiate a MockHelixCluster.
   * @param clusterName the name of the cluster.
   * @param hardwareLayoutPath the path to the {@link HardwareLayout} file used to bootstrap this cluster.
   * @param partitionLayoutPath the path to the {@link PartitionLayout} file used to bootstrap this cluster.
   * @param zkLayoutPath the path to the file containing the zk layout json string.
   * @throws Exception
   */
  MockHelixCluster(String clusterName, String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath)
      throws Exception {
    helixAdminFactory = new MockHelixAdminFactory();
    helixAdmins = helixAdminFactory.getAllHelixAdmins();
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterName,
        null, 3, helixAdminFactory);
    this.clusterName = clusterName;
  }

  /**
   * @return the cluster name of this cluster.
   */
  String getClusterName() {
    return clusterName;
  }

  /**
   * @return the {@link MockHelixAdminFactory} associated with this cluster.
   */
  MockHelixAdminFactory getHelixAdminFactory() {
    return helixAdminFactory;
  }

  /**
   * @return the set of zk service addresses associated with this cluster.
   */
  Set<String> getZkAddrs() {
    return helixAdmins.keySet();
  }

  /**
   * @param zkAddr the address of the zk service on which this operation is to be done.
   * @return the set of instances associated with the given zk service that are up.
   */
  List<String> getUpInstances(String zkAddr) {
    return helixAdmins.get(zkAddr).getUpInstances();
  }

  /**
   * @return set of all instances that are up in this cluster.
   */
  Set<String> getUpInstances() {
    Set<String> upInstances = new HashSet<>();
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      upInstances.addAll(helixAdmin.getUpInstances());
    }
    return upInstances;
  }

  /**
   * @param zkAddr the address of the zk service on which this operation is to be done.
   * @return the set of instances associated with the given zk service that are down.
   */
  List<String> getDownInstances(String zkAddr) {
    return helixAdmins.get(zkAddr).getDownInstances();
  }

  /**
   * @return set of all instances that are up in this cluster.
   */
  Set<String> getDownInstances() {
    Set<String> downInstances = new HashSet<>();
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      downInstances.addAll(helixAdmin.getDownInstances());
    }
    return downInstances;
  }

  /**
   * Bring the given instance up.
   * @param instanceName the instance to be brought up.
   */
  void bringInstanceUp(String instanceName) {
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      if (helixAdmin.getInstancesInCluster(clusterName).contains(instanceName)) {
        helixAdmin.bringInstanceUp(instanceName);
      }
    }
  }

  /**
   * Bring all instances in this cluster up.
   */
  void bringAllInstancesUp() {
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      for (String instance : helixAdmin.getDownInstances()) {
        helixAdmin.bringInstanceUp(instance);
      }
    }
  }

  /**
   * Bring the given instance down.
   * @param instanceName the instance to be brought down.
   */
  void bringInstanceDown(String instanceName) {
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      if (helixAdmin.getInstancesInCluster(clusterName).contains(instanceName)) {
        helixAdmin.bringInstanceDown(instanceName);
      }
    }
  }

  /**
   * Bring all instances in this cluster down.
   */
  void bringAllInstancesDown() {
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      for (String instance : helixAdmin.getUpInstances()) {
        helixAdmin.bringInstanceDown(instance);
      }
    }
  }

  /**
   * Set the state of a partition
   * @param partition partition for which state needs to be updated
   * @param partitionState {@link PartitionState} that needs to be set
   */
  void setPartitionState(String partition, PartitionState partitionState) {
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      helixAdmin.setPartitionState(partition, partitionState);
    }
  }

  /**
   * @return the set of all partitions in this cluster.
   */
  Set<String> getAllPartitions() {
    Set<String> partitions = null;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      if (partitions == null) {
        partitions = new HashSet<>(helixAdmin.getPartitions());
      } else {
        partitions.retainAll(helixAdmin.getPartitions());
      }
    }
    return partitions;
  }

  /**
   * @return the set of all partitions in this cluster that are up.
   */
  Set<String> getWritablePartitions() {
    Set<String> writablePartitions = null;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      if (writablePartitions == null) {
        writablePartitions = new HashSet<>(helixAdmin.getWritablePartitions());
      } else {
        writablePartitions.retainAll(helixAdmin.getWritablePartitions());
      }
    }
    return writablePartitions.isEmpty() ? getAllWritablePartitions() : writablePartitions;
  }

  /**
   * @return the set of all partitions in this cluster that are up.
   */
  Set<String> getAllWritablePartitions() {
    Set<String> writablePartitions = null;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      if (writablePartitions == null) {
        writablePartitions = new HashSet<>(helixAdmin.getAllWritablePartitions());
      } else {
        writablePartitions.retainAll(helixAdmin.getAllWritablePartitions());
      }
    }
    return writablePartitions;
  }

  /**
   * @return the count of datacenters in this cluster.
   */
  long getDataCenterCount() {
    return helixAdmins.size();
  }

  /**
   * @return Count of the number of disks in total in this cluster.
   */
  long getDiskCount() {
    long diskCount = 0;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      diskCount += helixAdmin.getTotalDiskCount();
    }
    return diskCount;
  }

  /**
   * @return Count of the number of disks in total in this cluster that are down.
   */
  long getDiskDownCount() {
    long diskDownCount = 0;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      for (String instanceName : helixAdmin.getDownInstances()) {
        diskDownCount += helixAdmin.getDiskCountOnNode(instanceName);
      }
    }
    return diskDownCount;
  }

  /**
   * @return total disk capacity across all disks on all nodes in this cluster.
   */
  long getDiskCapacity() {
    long diskCount = 0;
    for (MockHelixAdmin helixAdmin : helixAdmins.values()) {
      diskCount += helixAdmin.getTotalDiskCapacity();
    }
    return diskCount;
  }
}

