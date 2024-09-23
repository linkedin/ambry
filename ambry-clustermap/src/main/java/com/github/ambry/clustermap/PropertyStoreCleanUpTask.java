/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task to clean up property store for instances that are not live and not present in ideal state or external view
 */
public class PropertyStoreCleanUpTask implements Task {
  public final static String COMMAND = PropertyStoreCleanUpTask.class.getSimpleName();
  private final HelixManager manager;
  private final DataNodeConfigSource dataNodeConfigSource;
  private final Object helixAdministrationLock = new Object();
  private final ClusterMapConfig clusterMapConfig;
  private static final Logger logger = LoggerFactory.getLogger(PropertyStoreCleanUpTask.class);
  private final Metrics metrics;

  /**
   * Metrics for {@link PropertyStoreCleanUpTask}
   */
  private static class Metrics {
    public final Histogram instancesAndLiveInstancesFetchTimeInMs;
    public final Histogram idealStateAndExternalViewFetchTimeInMS;
    public final Histogram propertyStoreTaskTimeInMs;
    public final Counter propertyStoreCleanUpErrorCount;
    public final Counter propertyStoreCleanUpSuccessCount;

    public Metrics(MetricRegistry registry) {
      instancesAndLiveInstancesFetchTimeInMs = registry.histogram(MetricRegistry
          .name(PropertyStoreCleanUpTask.class, "InstancesAndLiveInstancesFetchTimeInMs"));
      idealStateAndExternalViewFetchTimeInMS = registry.histogram(MetricRegistry
          .name(PropertyStoreCleanUpTask.class, "IdealStateAndExternalViewFetchTimeInMS"));
      propertyStoreTaskTimeInMs = registry.histogram(MetricRegistry
          .name(PropertyStoreCleanUpTask.class, "PropertyStoreTaskTimeInMs"));
      propertyStoreCleanUpErrorCount = registry.counter(MetricRegistry
          .name(PropertyStoreCleanUpTask.class, "PropertyStoreCleanUpErrorCount"));
      propertyStoreCleanUpSuccessCount = registry.counter(MetricRegistry
          .name(PropertyStoreCleanUpTask.class, "PropertyStoreCleanUpSuccessCount"));
    }
  }

  /**
   * Constructor for {@link PropertyStoreCleanUpTask}
   *
   * @param manager
   * @param dataNodeConfigSource
   * @param clusterMapConfig
   * @param registry
   */
  public PropertyStoreCleanUpTask(HelixManager manager, DataNodeConfigSource dataNodeConfigSource,
      ClusterMapConfig clusterMapConfig, MetricRegistry registry) {
    this.manager = manager;
    this.dataNodeConfigSource = dataNodeConfigSource;
    this.metrics = new Metrics(registry);
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public TaskResult run() {
    long startTimeMs = System.currentTimeMillis();
    try {
      // Get all instances and live instances
      HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
      List<String> instances = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      Set<String> liveInstances = new HashSet<>(dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances()));
      metrics.instancesAndLiveInstancesFetchTimeInMs.update(System.currentTimeMillis() - startTimeMs);

      //Get all instances in ideal state and external view for this cluster
      Set<String> instancesInIdealStateAndExternalView = new HashSet<>();
      dataAccessor.getChildValues(dataAccessor.keyBuilder().idealStates(), true).stream()
          .map(IdealState.class::cast)
          .forEach(idealState -> idealState.getPartitionSet().stream()
              .map(idealState::getInstanceSet)
              .forEach(instancesInIdealStateAndExternalView::addAll));

      dataAccessor.getChildValues(dataAccessor.keyBuilder().externalViews(), true).stream()
          .map(ExternalView.class::cast)
          .forEach(externalView -> externalView.getPartitionSet().stream()
              .map(externalView::getStateMap)
              .forEach(stateMap -> instancesInIdealStateAndExternalView.addAll(stateMap.keySet())));

      metrics.idealStateAndExternalViewFetchTimeInMS.update(System.currentTimeMillis() - startTimeMs);

      // Cleanup property store for instances that are not live and not present in ideal state or external view
      instances.stream()
          .filter(
              instance -> shouldDoCleanUpPropertyStore(instance, liveInstances, instancesInIdealStateAndExternalView))
          .forEach(instance -> {
            logger.info("Cleaning up property store for instance {}", instance);
            try {
              cleanupPropertyStore(instance);
            } catch (Exception exception) {
              logger.error("Exception thrown while cleaning PropertyStore for instance = {}", instance, exception);
              metrics.propertyStoreCleanUpErrorCount.inc();
            }
          });

      metrics.propertyStoreTaskTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      return new TaskResult(TaskResult.Status.COMPLETED, "PropertyStoreCleanUpTask completed successfully");
    } catch (Exception exception) {
      logger.error("Exception thrown while executing PropertyStoreCleanUpTask for cluster = {}"
          ,manager.getClusterName(), exception);
      metrics.propertyStoreCleanUpErrorCount.inc();
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    }
  }

  /**
   *  Cleanup should be done if the instance is not live and not present in ideal state or external view
   * @param liveInstances Set of live instances
   * @param instancesInIdealStateAndExternalView Set of instances in ideal state and external view
   * @return
   */
  private boolean shouldDoCleanUpPropertyStore(String instance, Set<String> liveInstances,
      Set<String> instancesInIdealStateAndExternalView) {
    if(liveInstances.contains(instance)) {
      logger.trace("Instance {} is live, skipping cleanup", instance);
      return false;
    }
    if(instancesInIdealStateAndExternalView.contains(instance)) {
      logger.trace("Instance {} is present in ideal state or external view, skipping cleanup", instance);
      return false;
    }
    return true;
  }

  /**
   * Cleanup property store for the given instance
   */
  private void cleanupPropertyStore(String instance) {
    if(clusterMapConfig.clustermapDeleteDataFromDatanodeConfigInPropertyStoreCleanUpTask &&
        clusterMapConfig.clusterMapDataNodeConfigSourceType == DataNodeConfigSourceType.PROPERTY_STORE) {
        synchronized (helixAdministrationLock) {
          // Get the DataNodeConfig for the instance from the ZK
          DataNodeConfig dataNodeConfig = dataNodeConfigSource.get(instance);
          if (dataNodeConfig == null) { return;}

          /* We will first try to remove data from local DataNodeConfig object
             and then set it back in ZK based on configChanged flag
          */
          boolean configChanged;
          // Remove all sealed, stopped, partially sealed and disabled replicas from DataNodeConfig
          configChanged = TaskUtils.removeIfPresent(dataNodeConfig.getSealedReplicas());
          configChanged = TaskUtils.removeIfPresent(dataNodeConfig.getStoppedReplicas()) || configChanged;
          configChanged = TaskUtils.removeIfPresent(dataNodeConfig.getPartiallySealedReplicas()) || configChanged;
          configChanged = TaskUtils.removeIfPresent(dataNodeConfig.getDisabledReplicas()) || configChanged;
          Map<String, DataNodeConfig.DiskConfig> diskConfigs = dataNodeConfig.getDiskConfigs();

          // Remove all replicas for each disk in the DataNodeConfig
          for (DataNodeConfig.DiskConfig diskConfig : diskConfigs.values()) {
            configChanged = TaskUtils.removeIfPresent(diskConfig.getReplicaConfigs()) || configChanged;
          }
          // Only set the DatanodeConfig in ZK if it has changed
          if (configChanged) {
            if (dataNodeConfigSource.set(dataNodeConfig)) {
              logger.info("PropertyStore cleanup successful for instance = {}", instance);
              metrics.propertyStoreCleanUpSuccessCount.inc();
            } else {
              logger.error("PropertyStore cleanup failed for instance while setting config = {}", instance);
              metrics.propertyStoreCleanUpErrorCount.inc();
            }
          }
        }
    }
  }

  @Override
  public void cancel() {

  }
}
