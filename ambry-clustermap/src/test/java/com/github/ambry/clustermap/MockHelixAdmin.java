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
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.api.topology.ClusterTopology;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.api.status.ClusterManagementMode;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * Mock implementation of {@link HelixAdmin} which stores all information internally.
 */
public class MockHelixAdmin implements HelixAdmin {
  private final Map<String, InstanceConfig> instanceNameToInstanceConfigs = new HashMap<>();
  private final Map<String, IdealState> resourcesToIdealStates = new HashMap<>();
  private final Set<String> upInstances = new HashSet<>();
  private final Set<String> downInstances = new HashSet<>();
  private final List<MockHelixManager> helixManagersForThisAdmin = new ArrayList<>();
  private String clusterName;
  private long totalDiskCount = 0;
  private Map<String, Set<String>> partitionToInstances = new HashMap<>();
  private Map<String, PartitionState> partitionToPartitionStates = new HashMap<>();
  // A map of instanceName to state infos of all replicas on this instance
  private Map<String, ReplicaStateInfos> instanceToReplicaStateInfos = new HashMap<>();
  // A map of partitionId to instanceName associated with leader replica
  private Map<String, String> partitionToLeaderReplica = new HashMap<>();
  // A map of resource name to resource information such as partitions of the resource and their replica states. This is
  // used in external view.
  private Map<String, ResourceInfo> resourceToResourceInfoMap = new HashMap<>();
  // A map to reverse look up resource for a partition. This is used to update resourceToResourceInfoMap when a
  // replica of a partition changes its state.
  private Map<String, String> partitionToResourceMap = new HashMap<>();
  private long totalDiskCapacity;
  private int setInstanceConfigCallCount = 0;

  @Override
  public CustomizedView getResourceCustomizedView(String clusterName, String resourceName, String customizedStateType) {
    return null;
  }

  @Override
  public ClusterTopology getClusterTopology(String clusterName) {
    return null;
  }

  @Override
  public void setClusterManagementMode(ClusterManagementModeRequest request) {
  }

  @Override
  public ClusterManagementMode getClusterManagementMode(String clusterName) {
    return null;
  }

  @Override
  public void enableInstance(String clusterName, String instanceName, boolean enabled,
      InstanceConstants.InstanceDisabledType disabledType, String reason) {
  }

  @Override
  public void purgeOfflineInstances(String clusterName, long offlineDuration) {
  }

  /**
   * Get the instances that have replicas for the given partition.
   * @param partition the partition name of the partition.
   * @return the set of instances that have replicas for this partition.
   */
  Set<String> getInstancesForPartition(String partition) {
    return partitionToInstances.getOrDefault(partition, Collections.emptySet());
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
    instanceNameToInstanceConfigs.put(instanceConfig.getInstanceName(), instanceConfig);
    upInstances.add(instanceConfig.getInstanceName());
    Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
    totalDiskCount += diskInfos.size();
    for (Map<String, String> diskInfo : diskInfos.values()) {
      totalDiskCapacity += Long.parseLong(diskInfo.get(ClusterMapUtils.DISK_CAPACITY_STR));
    }
  }

  @Override
  public void addResource(String clusterName, String resourceName, IdealState idealstate) {
    resourcesToIdealStates.put(resourceName, idealstate);
    ResourceInfo resourceInfo = resourceToResourceInfoMap.computeIfAbsent(resourceName, k -> new ResourceInfo());
    for (String partition : idealstate.getPartitionSet()) {
      partitionToResourceMap.put(partition, resourceName);
      if (partitionToInstances.get(partition) == null) {
        Set<String> instanceSet = new HashSet<>();
        partitionToInstances.put(partition, instanceSet);
        partitionToPartitionStates.put(partition, PartitionState.READ_WRITE);
      }
      List<String> instances = new ArrayList<>(idealstate.getInstanceSet(partition));
      partitionToInstances.get(partition).addAll(instances);
      for (int i = 0; i < instances.size(); ++i) {
        String instanceName = instances.get(i);
        String stateStr;
        if (i == 0) {
          stateStr = ReplicaState.LEADER.name();
          partitionToLeaderReplica.put(partition, instanceName);
        } else {
          stateStr = ReplicaState.STANDBY.name();
        }
        instanceToReplicaStateInfos.computeIfAbsent(instanceName, k -> new ReplicaStateInfos())
            .setReplicaState(partition, stateStr);
        resourceInfo.setReplicasState(partition, instanceName, stateStr);
      }
    }
  }

  @Override
  public List<String> getResourcesInCluster(String clusterName) {
    return new ArrayList<>(resourcesToIdealStates.keySet());
  }

  @Override
  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    return resourcesToIdealStates.get(resourceName);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    return new ArrayList<>(instanceNameToInstanceConfigs.keySet());
  }

  List<InstanceConfig> getInstanceConfigs(String clusterName) {
    return new ArrayList<>(instanceNameToInstanceConfigs.values());
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    return instanceNameToInstanceConfigs.get(instanceName);
  }

  @Override
  public boolean setInstanceConfig(String clusterName, String instanceName, InstanceConfig instanceConfig) {
    setInstanceConfigCallCount++;
    removeDisabledReplicasIfNeeded(instanceConfig);
    instanceNameToInstanceConfigs.put(instanceName, instanceConfig);
    return true;
  }

  /**
   * Remove disabled replicas from InstanceConfig if needed. This is to mock last step of disabling replica, which will
   * remove the replica entry from InstanceConfig.
   * @param instanceConfig the instanceConfig to check.
   */
  private void removeDisabledReplicasIfNeeded(InstanceConfig instanceConfig) {
    if (instanceConfig == null) {
      return;
    }
    Map<String, List<String>> disabledReplicasByResource = instanceConfig.getDisabledPartitionsMap();
    if (!disabledReplicasByResource.isEmpty()) {
      // if disabled replica map is not empty, we remove those replicas from InstanceConfig to mock replica decommission
      Set<String> replicasToRemove = new HashSet<>();
      disabledReplicasByResource.values().forEach(replicasToRemove::addAll);
      Map<String, Map<String, String>> newMapFields = new HashMap<>();
      Map<String, Map<String, String>> mapFields = instanceConfig.getRecord().getMapFields();
      for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
        if (entry.getKey().startsWith("/mnt")) {
          String replicasStr = entry.getValue().get(REPLICAS_STR);
          if (!replicasStr.isEmpty()) {
            String[] replicaArray = replicasStr.split(REPLICAS_DELIM_STR);
            StringBuilder sb = new StringBuilder();
            for (String replicaStr : replicaArray) {
              if (!replicasToRemove.contains(replicaStr.split(REPLICAS_STR_SEPARATOR)[0])) {
                sb.append(replicaStr).append(REPLICAS_DELIM_STR);
              }
            }
            entry.getValue().put(REPLICAS_STR, sb.toString());
          }
          newMapFields.put(entry.getKey(), entry.getValue());
        }
      }
      instanceConfig.getRecord().setMapFields(newMapFields);
    }
  }

  /**
   * Change leader replica of certain partition from current one to the replica on specified instance.
   * @param partition the partition whose leader replica should be changed.
   * @param newLeaderInstance the instance on which new leader replica resides.
   */
  void changeLeaderReplicaForPartition(String partition, String newLeaderInstance) {
    String currentLeaderInstance = partitionToLeaderReplica.get(partition);
    // set current leader replica to STANDBY state
    instanceToReplicaStateInfos.get(currentLeaderInstance).setReplicaState(partition, ReplicaState.STANDBY.name());
    // set previous standby replica to LEADER state
    instanceToReplicaStateInfos.get(newLeaderInstance).setReplicaState(partition, ReplicaState.LEADER.name());
    partitionToLeaderReplica.put(partition, newLeaderInstance);
    // Update external view maps
    String resource = partitionToResourceMap.get(partition);
    ResourceInfo resourceInfo = resourceToResourceInfoMap.get(resource);
    resourceInfo.setReplicasState(partition, currentLeaderInstance, ReplicaState.STANDBY.name());
    resourceInfo.setReplicasState(partition, newLeaderInstance, ReplicaState.LEADER.name());
  }

  /**
   * Set the replica state of the specified replica of the specified partition to the specified state.
   * @param partition the partition whose leader replica should be changed.
   * @param instance the instance of the replica for which state needs to be changed.
   * @param newReplicaState The new state of the replica.
   */
  void setReplicaStateForPartition(String partition, String instance, ReplicaState newReplicaState) {
    instanceToReplicaStateInfos.get(instance).setReplicaState(partition, newReplicaState.name());
    // Update external view maps
    String resource = partitionToResourceMap.get(partition);
    ResourceInfo resourceInfo = resourceToResourceInfoMap.get(resource);
    resourceInfo.setReplicasState(partition, instance, newReplicaState.name());
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
   * Remove ideal state for the given resource name. The {@code clusterName} is not used. It will trigger ideal state
   * change notification.
   * @param clusterName The name of the helix cluster, not used.
   * @param resourceName The name of the resource.
   * @throws Exception
   */
  void removeResourceIdealState(String clusterName, String resourceName) throws Exception {
    resourcesToIdealStates.remove(resourceName);
    resourceToResourceInfoMap.remove(resourceName);
    triggerIdealStateChangeNotification();
  }

  /**
   * Add new resource and its associated ideal state into cluster (Note that, each dc has its own HelixAdmin so resource
   * is actually added into dc where current HelixAdmin sits). This would trigger ideal state change which should be captured
   * by Helix Cluster Manager on each node.
   * @param resourceName name of resource. (The resource may contain one or more partitions)
   * @param idealState ideal state associated with the resource. (it defines location of each replica from each partition)
   * @throws Exception
   */
  void addNewResource(String resourceName, IdealState idealState) throws Exception {
    resourcesToIdealStates.put(resourceName, idealState);
    triggerIdealStateChangeNotification();
  }

  /**
   * Associate the given Helix manager with this admin.
   * @param helixManager the {@link MockHelixManager} to associate this admin with.
   */
  void addHelixManager(MockHelixManager helixManager) {
    helixManagersForThisAdmin.add(helixManager);
  }

  /**
   * @return a list of {@link IdealState} via Helix admin.
   */
  List<IdealState> getIdealStates() {
    return new ArrayList<>(new HashSet<>(resourcesToIdealStates.values()));
  }

  /**
   * @return all instances registered via this Helix admin that are up.
   */
  List<String> getUpInstances() {
    return new ArrayList<>(upInstances);
  }

  /**
   * Bring up the given instance (that was registered via this Helix admin).
   * @param instance the instance to be brought up.
   */
  void bringInstanceUp(String instance) {
    downInstances.remove(instance);
    upInstances.add(instance);
  }

  /**
   * @return all instances registered via this Helix admin that are down.
   */
  List<String> getDownInstances() {
    return new ArrayList<>(downInstances);
  }

  /**
   * Bring down the given instance (that was registered via this Helix admin).
   * @param instance the instance to be brought down.
   */
  void bringInstanceDown(String instance) {
    downInstances.add(instance);
    upInstances.remove(instance);
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
  void triggerLiveInstanceChangeNotification() {
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

  void triggerIdealStateChangeNotification() throws Exception {
    for (MockHelixManager helixManager : helixManagersForThisAdmin) {
      helixManager.triggerIdealStateNotification(false);
    }
  }

  /**
   * Trigger a routing table change notification
   */
  void triggerRoutingTableNotification() {
    for (MockHelixManager helixManager : helixManagersForThisAdmin) {
      helixManager.triggerRoutingTableNotification();
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
      if (!partitionToPartitionStates.get(entry.getKey()).equals(PartitionState.READ_ONLY)) {
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
      if (!partitionToPartitionStates.get(entry.getKey()).equals(PartitionState.READ_ONLY)) {
        writablePartitions.add(entry.getKey());
      }
    }
    return writablePartitions;
  }

  /**
   * Get states of all partitions that reside on given instance.
   * @param instanceName the name of instance where partitions reside
   * @return a map representing states of partitions from given instance.
   */
  Map<String, Map<String, String>> getPartitionStateMapForInstance(String instanceName) {
    ReplicaStateInfos replicaStateInfos = instanceToReplicaStateInfos.get(instanceName);
    return replicaStateInfos != null ? replicaStateInfos.getReplicaStateMap() : new HashMap<>();
  }

  /**
   * Get partitions and their replicas to state map for a given resource. This is used in construction of external view.
   * @param resource resource name
   * @return a map of partitions to their replica infos.
   */
  Map<String, Map<String, String>> getPartitionToReplicasMapForResource(String resource) {
    ResourceInfo resourceInfo = resourceToResourceInfoMap.get(resource);
    return resourceInfo != null ? resourceInfo.getPartitionToReplicasMap() : new HashMap<>();
  }

  /**
   * @return a map of partition to its leader replica in current dc.
   */
  Map<String, String> getPartitionToLeaderReplica() {
    return Collections.unmodifiableMap(partitionToLeaderReplica);
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
    InstanceConfig instanceConfig = instanceNameToInstanceConfigs.get(instanceName);
    return instanceConfig.getRecord().getMapFields().size();
  }

  /**
   * @return the total capacity across all disks on all nodes registered via this admin.
   */
  long getTotalDiskCapacity() {
    return totalDiskCapacity;
  }

  /**
   * @return the number of calls to the {@link #setInstanceConfig} method.
   */
  int getSetInstanceConfigCallCount() {
    return setInstanceConfigCallCount;
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
  public void addCustomizedStateConfig(String clusterName, CustomizedStateConfig customizedStateConfig) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeCustomizedStateConfig(String clusterName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void addTypeToCustomizedStateConfig(String clusterName, String type) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeTypeFromCustomizedStateConfig(String clusterName, String type) {
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
  public void updateIdealState(String s, String s1, IdealState idealState) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeFromIdealState(String s, String s1, IdealState idealState) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableInstance(String clusterName, String instanceName, boolean enabled) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableInstance(String s, List<String> list, boolean b) {
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
  public void enableCluster(String s, boolean b, String s1) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableMaintenanceMode(String s, boolean b) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableMaintenanceMode(String s, boolean b, String s1) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void autoEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      MaintenanceSignal.AutoTriggerReason internalReason) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void manuallyEnableMaintenanceMode(String clusterName, boolean enabled, String reason,
      Map<String, String> customFields) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean isInMaintenanceMode(String clusterName) {
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
  public void addCloudConfig(String clusterName, CloudConfig cloudConfig) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void removeCloudConfig(String clusterName) {
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
  public void enableBatchMessageMode(String s, boolean b) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void enableBatchMessageMode(String s, String s1, boolean b) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Map<String, String> getBatchDisabledInstances(String s) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> getInstancesByDomain(String s, String s1) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close() {
    // no-op.
  }

  @Override
  public boolean addResourceWithWeight(String clusterName, IdealState idealState, ResourceConfig resourceConfig) {
    return false;
  }

  @Override
  public boolean enableWagedRebalance(String clusterName, List<String> resourceNames) {
    return false;
  }

  @Override
  public Map<String, Boolean> validateResourcesForWagedRebalance(String clusterName, List<String> resourceNames) {
    return null;
  }

  @Override
  public Map<String, Boolean> validateInstancesForWagedRebalance(String clusterName, List<String> instancesNames) {
    return null;
  }

  /**
   * Private class that holds partition state infos from one data node.
   */
  class ReplicaStateInfos {
    Map<String, Map<String, String>> replicaStateMap;

    ReplicaStateInfos() {
      replicaStateMap = new HashMap<>();
    }

    void setReplicaState(String partition, String state) {
      Map<String, String> stateMap = new HashMap<>();
      stateMap.put(CurrentState.CurrentStateProperty.CURRENT_STATE.name(), state);
      replicaStateMap.put(partition, stateMap);
    }

    Map<String, Map<String, String>> getReplicaStateMap() {
      return replicaStateMap;
    }
  }

  class ResourceInfo {
    Map<String, Map<String, String>> partitionToReplicasMap;

    ResourceInfo() {
      partitionToReplicasMap = new HashMap<>();
    }

    void setReplicasState(String partition, String replica, String state) {
      Map<String, String> replicaToState = partitionToReplicasMap.computeIfAbsent(partition, k -> new HashMap<>());
      replicaToState.put(replica, state);
    }

    Map<String, Map<String, String>> getPartitionToReplicasMap() {
      return partitionToReplicasMap;
    }
  }
}
