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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;


/**
 * Mock implementation of {@link HelixAdmin} which stores all information internally.
 */
public class MockHelixAdmin implements HelixAdmin {
  private String clusterName;
  private final Map<String, InstanceConfig> instanceNameToinstanceConfigs = new HashMap<>();
  private final Map<String, IdealState> resourcesToIdealStates = new HashMap<>();
  private final Set<String> upInstances = new HashSet<>();
  private final Set<String> downInstances = new HashSet<>();
  private long totalDiskCount = 0;
  private final List<MockHelixManager> helixManagersForThisAdmin = new ArrayList<>();
  private Map<String, Set<String>> partitionToInstances = new HashMap<>();
  private Map<String, PartitionState> partitionToPartitionStates = new HashMap<>();
  private long totalDiskCapacity;

  /**
   * Get the instances that have replicas for the given partition.
   * @param partition the partition name of the partition.
   * @return the set of instances that have replicas for this partition.
   */
  Set<String> getInstancesForPartition(String partition) {
    return partitionToInstances.containsKey(partition) ? partitionToInstances.get(partition) : Collections.EMPTY_SET;
  }

  @Override
  public List<String> getClusters() {
    return Collections.singletonList(clusterName);
  }

  @Override
  public boolean addCluster(String clusterName) {
    if (this.clusterName == null) {
      this.clusterName = clusterName;
      return true;
    } else {
      throw new IllegalStateException("A cluster has already been added");
    }
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record) {
  }

  @Override
  public void addInstance(String clusterName, InstanceConfig instanceConfig) {
    instanceNameToinstanceConfigs.put(instanceConfig.getInstanceName(), instanceConfig);
    upInstances.add(instanceConfig.getInstanceName());
    Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
    totalDiskCount += diskInfos.size();
    for (Map<String, String> diskInfo : diskInfos.values()) {
      totalDiskCapacity += Long.valueOf(diskInfo.get(ClusterMapUtils.DISK_CAPACITY_STR));
    }
  }

  @Override
  public void addResource(String clusterName, String resourceName, IdealState idealstate) {
    resourcesToIdealStates.put(resourceName, idealstate);
    for (String partition : idealstate.getPartitionSet()) {
      if (partitionToInstances.get(partition) == null) {
        Set<String> instanceSet = new HashSet<>();
        partitionToInstances.put(partition, instanceSet);
        partitionToPartitionStates.put(partition, PartitionState.READ_WRITE);
      }
      partitionToInstances.get(partition).addAll(idealstate.getInstanceSet(partition));
    }
  }

  @Override
  public List<String> getResourcesInCluster(String clusterName) {
    List<String> resources = new ArrayList<>();
    resources.addAll(resourcesToIdealStates.keySet());
    return resources;
  }

  @Override
  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    return resourcesToIdealStates.get(resourceName);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    List<String> instances = new ArrayList<>();
    instances.addAll(instanceNameToinstanceConfigs.keySet());
    return instances;
  }

  List<InstanceConfig> getInstanceConfigs(String clusterName) {
    return new ArrayList<>(instanceNameToinstanceConfigs.values());
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    return instanceNameToinstanceConfigs.get(instanceName);
  }

  @Override
  public boolean setInstanceConfig(String clusterName, String instanceName, InstanceConfig instanceConfig) {
    instanceNameToinstanceConfigs.put(instanceName, instanceConfig);
    return true;
  }

  /**
   * Set or reset the sealed state of the replica for the given partition on the given instance.
   * @param partition the {@link AmbryPartition}
   * @param instance the instance name.
   * @param isSealed if true, the replica will be marked as sealed; otherwise it will be marked as read-write.
   * @param tagAsInit whether the InstanceConfig notification should be tagged with
   *                  {@link org.apache.helix.NotificationContext.Type#INIT}
   */
  void setReplicaSealedState(AmbryPartition partition, String instance, boolean isSealed, boolean tagAsInit) {
    InstanceConfig instanceConfig = getInstanceConfig(clusterName, instance);
    List<String> sealedReplicas = ClusterMapUtils.getSealedReplicas(instanceConfig);
    if (isSealed) {
      sealedReplicas.add(partition.toPathString());
    } else {
      sealedReplicas.remove(partition.toPathString());
    }
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedReplicas);
    triggerInstanceConfigChangeNotification(tagAsInit);
  }

  /**
   * Set or reset the stopped state of the replica for the given partition on the given instance.
   * @param partition the {@link AmbryPartition}
   * @param instance the instance name.
   * @param isStopped if true, the replica will be marked as stopped; otherwise it is proper functioning.
   * @param tagAsInit whether the InstanceConfig notification should be tagged with
   *                  {@link org.apache.helix.NotificationContext.Type#INIT}
   */
  void setReplicaStoppedState(AmbryPartition partition, String instance, boolean isStopped, boolean tagAsInit) {
    InstanceConfig instanceConfig = getInstanceConfig(clusterName, instance);
    List<String> stoppedReplicas = ClusterMapUtils.getStoppedReplicas(instanceConfig);
    if (isStopped) {
      stoppedReplicas.add(partition.toPathString());
    } else {
      stoppedReplicas.remove(partition.toPathString());
    }
    instanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, stoppedReplicas);
    triggerInstanceConfigChangeNotification(tagAsInit);
  }

  /**
   * Associate the given Helix manager with this admin.
   * @param helixManager the {@link MockHelixManager} to associate this admin with.
   */
  void addHelixManager(MockHelixManager helixManager) {
    helixManagersForThisAdmin.add(helixManager);
  }

  /**
   * @return all instances registered via this Helix admin that are up.
   */
  List<String> getUpInstances() {
    List<String> upList = new ArrayList<>();
    upList.addAll(upInstances);
    return upList;
  }

  /**
   * Bring up the given instance (that was registered via this Helix admin).
   * @param instance the instance to be brought up.
   */
  void bringInstanceUp(String instance) {
    downInstances.remove(instance);
    upInstances.add(instance);
    triggerLiveInstanceChangeNotification();
  }

  /**
   * @return all instances registered via this Helix admin that are down.
   */
  List<String> getDownInstances() {
    List<String> downList = new ArrayList<>();
    downList.addAll(downInstances);
    return downList;
  }

  /**
   * Bring down the given instance (that was registered via this Helix admin).
   * @param instance the instance to be brought down.
   */
  void bringInstanceDown(String instance) {
    downInstances.add(instance);
    upInstances.remove(instance);
    triggerLiveInstanceChangeNotification();
  }

  /**
   * Sets the state of a partition
   * @param partition partition for which state needs to be updated
   * @param partitionState {@link PartitionState} that needs to be set
   */
  void setPartitionState(String partition, PartitionState partitionState) {
    for (IdealState entry : resourcesToIdealStates.values()) {
      for (String partitionInIdealState : entry.getPartitionSet()) {
        if (partitionInIdealState.equals(partition)) {
          partitionToPartitionStates.put(partition, partitionState);
        }
      }
    }
  }

  /**
   * Trigger a live instance change notification.
   */
  private void triggerLiveInstanceChangeNotification() {
    for (MockHelixManager helixManager : helixManagersForThisAdmin) {
      helixManager.triggerLiveInstanceNotification(false);
    }
  }

  /**
   * Trigger an instance config change notification.
   * @param tagAsInit whether the InstanceConfig notification should be tagged with
   *                  {@link org.apache.helix.NotificationContext.Type#INIT}
   */
  void triggerInstanceConfigChangeNotification(boolean tagAsInit) {
    for (MockHelixManager helixManager : helixManagersForThisAdmin) {
      helixManager.triggerConfigChangeNotification(tagAsInit);
    }
  }

  /**
   * @return a list of all partitions registered via this admin.
   */
  Set<String> getPartitions() {
    return partitionToInstances.keySet();
  }

  /**
   * @return all writable partitions registered via this Helix admin.
   */
  Set<String> getWritablePartitions() {
    Set<String> healthyWritablePartitions = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : partitionToInstances.entrySet()) {
      if (partitionToPartitionStates.get(entry.getKey()).equals(PartitionState.READ_WRITE)) {
        boolean up = true;
        for (String instance : entry.getValue()) {
          if (!getUpInstances().contains(instance)) {
            up = false;
            break;
          }
        }
        if (up) {
          healthyWritablePartitions.add(entry.getKey());
        }
      }
    }
    return healthyWritablePartitions;
  }

  /**
   * @return all writable partitions registered via this Helix admin.
   */
  Set<String> getAllWritablePartitions() {
    Set<String> writablePartitions = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : partitionToInstances.entrySet()) {
      if (partitionToPartitionStates.get(entry.getKey()).equals(PartitionState.READ_WRITE)) {
        writablePartitions.add(entry.getKey());
      }
    }
    return writablePartitions;
  }

  /**
   * @return the count of disks registered via this admin.
   */
  long getTotalDiskCount() {
    return totalDiskCount;
  }

  /**
   * @param instanceName the instance on which this operation is being done.
   * @return the count of disks registered for the given node via this admin.
   */
  long getDiskCountOnNode(String instanceName) {
    InstanceConfig instanceConfig = instanceNameToinstanceConfigs.get(instanceName);
    return instanceConfig.getRecord().getMapFields().size();
  }

  /**
   * @return the total capacity across all disks on all nodes registered via this admin.
   */
  long getTotalDiskCapacity() {
    return totalDiskCapacity;
  }

  // ***************************************
  // Not implemented. Implement as required.
  // ***************************************

  @Override
  public List<String> getResourcesInClusterWithTag(String clusterName, String tag) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean addCluster(String clusterName, boolean recreateIfExists) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addClusterToGrandCluster(String clusterName, String grandCluster) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, String rebalanceStrategy) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, int bucketSize) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, int bucketSize, int maxPartitionsPerInstance) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addResource(String clusterName, String resourceName, int numPartitions, String stateModelRef,
      String rebalancerMode, String rebalanceStrategy, int bucketSize, int maxPartitionsPerInstance) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig instanceConfig) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setResourceIdealState(String clusterName, String resourceName, IdealState idealState) {
    addResource(clusterName, resourceName, idealState);
  }

  @Override
  public void enableInstance(String clusterName, String instanceName, boolean enabled) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableResource(String clusterName, String resourceName, boolean enabled) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enablePartition(boolean enabled, String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableCluster(String clusterName, boolean enabled) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void resetInstance(String clusterName, List<String> instanceNames) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void resetResource(String clusterName, List<String> resourceNames) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record,
      boolean recreateIfExists) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void dropResource(String clusterName, String resourceName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> getStateModelDefs(String clusterName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public ExternalView getResourceExternalView(String clusterName, String resourceName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void dropCluster(String clusterName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setConfig(HelixConfigScope scope, Map<String, String> properties) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeConfig(HelixConfigScope scope, List<String> keys) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Map<String, String> getConfig(HelixConfigScope scope, List<String> keys) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> getConfigKeys(HelixConfigScope scope) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addIdealState(String clusterName, String resourceName, String idealStateFile) throws IOException {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDefName, String stateModelDefFile)
      throws IOException {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setConstraint(String clusterName, ClusterConstraints.ConstraintType constraintType, String constraintId,
      ConstraintItem constraintItem) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeConstraint(String clusterName, ClusterConstraints.ConstraintType constraintType,
      String constraintId) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public ClusterConstraints getConstraints(String clusterName, ClusterConstraints.ConstraintType constraintType) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void rebalance(String clusterName, IdealState currentIdealState, List<String> instanceNames) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica, List<String> instances) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica, String keyPrefix, String group) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addInstanceTag(String clusterName, String instanceName, String tag) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeInstanceTag(String clusterName, String instanceName, String tag) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setInstanceZoneId(String clusterName, String instanceName, String zoneId) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close() {
    // no-op.
  }
}
