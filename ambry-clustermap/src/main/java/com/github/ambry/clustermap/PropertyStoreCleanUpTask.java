package com.github.ambry.clustermap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
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
  public static String COMMAND = PropertyStoreCleanUpTask.class.getSimpleName();
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
      HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
      HelixAdmin admin = manager.getClusterManagmentTool();
      List<String> instances = dataAccessor.getProperty(dataAccessor.keyBuilder().instances());
      Set<String> liveInstances = dataAccessor.getProperty(dataAccessor.keyBuilder().liveInstances());
      metrics.instancesAndLiveInstancesFetchTimeInMs.update(System.currentTimeMillis() - startTimeMs);

      Set<String> allInstancesInIdealState = new HashSet<>();
      Set<String> allInstancesInExternalView = new HashSet<>();
      List<String> resourcesInCluster = admin.getResourcesInCluster(manager.getClusterName());

      for (String resource : resourcesInCluster) {
        IdealState idealState = admin.getResourceIdealState(manager.getClusterName(), resource);
        Set<String> partitions = idealState.getPartitionSet();
        for (String partition : partitions) {
          allInstancesInIdealState.addAll(idealState.getInstanceSet(partition));
        }

        ExternalView externalView = admin.getResourceExternalView(manager.getClusterName(), resource);
        Set<String> partitionSet = externalView.getPartitionSet();
        for (String partition : partitionSet) {
          allInstancesInExternalView.addAll(externalView.getStateMap(partition).keySet());
        }
      }
      metrics.idealStateAndExternalViewFetchTimeInMS.update(System.currentTimeMillis() - startTimeMs);

      // Find instances that are not live and also not present in ideal state or external view

      for (String instance : instances) {
        if (!liveInstances.contains(instance) && !allInstancesInIdealState.contains(instance)
            && !allInstancesInExternalView.contains(instance)) {
          logger.info("Cleaning up property store for instance {}", instance);
          // Do the cleanup
          if(clusterMapConfig.clustermapEnablePropertyStoreCleanUpTask) {
            try {
              synchronized (helixAdministrationLock) {
                DataNodeConfig dataNodeConfig = dataNodeConfigSource.get(instance);
                if (dataNodeConfig == null) {
                  continue;
                }
                boolean configChanged;
                configChanged = TaskUtils.removeConfig(dataNodeConfig.getSealedReplicas());
                configChanged = configChanged || TaskUtils.removeConfig(dataNodeConfig.getStoppedReplicas());
                configChanged = configChanged || TaskUtils.removeConfig(dataNodeConfig.getPartiallySealedReplicas());
                configChanged = configChanged || TaskUtils.removeConfig(dataNodeConfig.getDisabledReplicas());
                Map<String, DataNodeConfig.DiskConfig> diskConfigs = dataNodeConfig.getDiskConfigs();
                for (DataNodeConfig.DiskConfig diskConfig : diskConfigs.values()) {
                  configChanged = configChanged || TaskUtils.removeConfig(diskConfig.getReplicaConfigs());
                }
                if (configChanged) {
                  dataNodeConfigSource.set(dataNodeConfig);
                }
              }
            } catch (Exception exception) {
              logger.error("Exception thrown while cleaning PropertyStore for instance = {}", instance, exception);
              metrics.propertyStoreCleanUpErrorCount.inc();
            }
          }
        }
      }
      metrics.propertyStoreCleanUpSuccessCount.inc();
      metrics.propertyStoreTaskTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      return new TaskResult(TaskResult.Status.COMPLETED, "PropertyStoreCleanUpTask completed successfully");
    } catch (Exception exception) {
      logger.error("Exception thrown while executing PropertyStoreCleanUpTask for cluster = {}"
          ,manager.getClusterName(), exception);
      metrics.propertyStoreCleanUpErrorCount.inc();
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    }
  }

  @Override
  public void cancel() {

  }
}
