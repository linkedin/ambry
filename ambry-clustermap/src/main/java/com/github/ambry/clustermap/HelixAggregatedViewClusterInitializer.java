/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

import com.github.ambry.clustermap.ClusterMapUtils.DcZkInfo;
import com.github.ambry.clustermap.HelixClusterManager.HelixClusterChangeHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents the startup procedure for registering with helix aggregated view cluster which contains the
 * cluster information of all colos. Using the helix aggregation view cluster, we could get entire cluster information
 * from one place instead of connecting to multiple helix ZK servers and getting individual data center information from
 * each of them. The logic in this class is similar to {@link HelixDatacenterInitializer}.
 */
class HelixAggregatedViewClusterInitializer {
  private static final Logger logger = LoggerFactory.getLogger(HelixAggregatedViewClusterInitializer.class);
  private final ClusterMapConfig clusterMapConfig;
  private final HelixFactory helixFactory;
  private final String selfInstanceName;
  private final DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics;
  private final HelixClusterManager helixClusterManager;
  private final Map<String, DcZkInfo> dataCenterToZkAddress;
  private Exception exception = null;
  List<DataNodeConfigSource> dataNodeConfigSources = new ArrayList<>();
  HelixClusterChangeHandler clusterChangeHandler;

  /**
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param dataCenterToZkAddress map of data center to ZK information.
   * @param helixFactory the {@link HelixFactory} instance to construct managers.
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param dataNodeConfigSourceMetrics metrics related to {@link DataNodeConfigSource}.
   * @param helixClusterManager reference to {@link HelixClusterManager} that creates this class.
   */
  HelixAggregatedViewClusterInitializer(ClusterMapConfig clusterMapConfig, Map<String, DcZkInfo> dataCenterToZkAddress,
      HelixFactory helixFactory, String selfInstanceName, DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics,
      HelixClusterManager helixClusterManager) {
    this.clusterMapConfig = clusterMapConfig;
    this.dataCenterToZkAddress = dataCenterToZkAddress;
    this.helixFactory = helixFactory;
    this.selfInstanceName = selfInstanceName;
    this.dataNodeConfigSourceMetrics = dataNodeConfigSourceMetrics;
    this.helixClusterManager = helixClusterManager;
  }

  /**
   * Begin the startup procedure for connecting (and registering listeners) for getting cluster information from helix
   * service.
   * @return {@link HelixAggregatedViewClusterInfo} containing the information associated with view cluster.
   */
  public HelixAggregatedViewClusterInfo start() throws Exception {
    try {
      HelixAggregatedViewClusterInfo clusterInfo = initialize();
      if (exception != null) {
        // We could have hit an error in one of the helix listener callbacks in HelixClusterChangeHandler. In such cases,
        // rethrow the exception.
        throw exception;
      } else {
        return clusterInfo;
      }
    } catch (Exception e) {
      logger.warn("Exception while initializing cluster", e);
      throw e;
    }
  }

  /**
   * Perform initialization steps related to registering with Helix Aggregated view cluster.
   * @return the {@link HelixAggregatedViewClusterInfo} for the cluster.
   * @throws Exception if something went wrong during startup
   */
  public HelixAggregatedViewClusterInfo initialize() throws Exception {
    // We will have Helix Aggregated view cluster in each data center. Connect to helix aggregated view cluster in local
    // DC.
    String localDcName = clusterMapConfig.clusterMapDatacenterName;
    String aggregatedViewClusterName = clusterMapConfig.clusterMapAggregatedViewClusterName;
    DcZkInfo localDcZkInfo = dataCenterToZkAddress.get(localDcName);

    // The first ZK endpoint (if there are more than one endpoints) will be adopted by default for initialization since
    // Ambry doesn't support multiple spectators and uses only one source of truth.
    String localZkConnectStr = localDcZkInfo.getZkConnectStrs().get(0);
    HelixManager helixManager =
        helixFactory.getZkHelixManagerAndConnect(aggregatedViewClusterName, selfInstanceName, InstanceType.SPECTATOR,
            localZkConnectStr);
    logger.info("Helix cluster name {}", helixManager.getClusterName());

    clusterChangeHandler =
        helixClusterManager.new HelixClusterChangeHandler(localDcName, aggregatedViewClusterName, ex -> exception = ex,
            true);

    // Ambry needs below information from Helix:
    // 1. Topology information list of hosts, disks in each host, partitions on each disk, etc. This is stored in helix
    //    "PROPERTYSTORE/DataNodeConfigs" znodes.
    // 2. External view: This tells the current states of various replicas in the cluster. For ex, list of replicas in
    //    leader state in a given partition.
    // 3. Live instances: This tells the list of live hosts in cluster so that we can mark the hosts as down.

    // Currently, Helix only provides aggregated view of External view and Live instances. So, we will have to
    // fetch data in Helix Property store from all colos ourselves.

    // 1. Fetch Data node configs from each Data center and wait until the initialization is complete before registering
    // for changes from Aggregated helix view.
    List<Thread> dcInitThreads = new ArrayList<>();
    for (String dcName : dataCenterToZkAddress.keySet()) {
      DcZkInfo dcZkInfo = dataCenterToZkAddress.get(dcName);
      if (dcZkInfo.getReplicaType() != ReplicaType.DISK_BACKED) {
        // There could be old configs which have CLOUD_BACKED replica information. Keep this until CLOUD_BACKED replicas
        // are completely deprecated.
        continue;
      }
      Thread thread = Utils.newThread("helix-aggregated-view-datanode-config-init-dc-" + dcName, () -> {
        try {
          registerForDataNodeConfigsChanges(dcName);
        } catch (Exception e) {
          exception = e;
        }
      }, false);
      thread.start();
      dcInitThreads.add(thread);
    }
    for (Thread thread : dcInitThreads) {
      thread.join();
    }

    // 2. Fetch External view. Helix provides a helper class RoutingTableProvider to get information in external view in
    // a convenient way.
    logger.info("Creating routing table provider for cluster {} via Helix manager at {}", aggregatedViewClusterName,
        localZkConnectStr);
    // There are two ways to instantiate a RoutingTable. 1. EXTERNAL_VIEW based, 2. CURRENT_STATES based. In the former
    // one, helix controller generates the external view and this is read by the helix spectator to create the Routing
    // table. In the latter one, CURRENT STATES are read from helix participant to participant at the helix spectator to
    // create the Routing table. According to helix team, the former one usually takes longer time since it is dependent
    // on helix controller to generate up-to-date view but has less read traffic since we have to read constructed view.
    // The latter one takes lesser time since we don't have to wait for controller to calculate the view but has more
    // read traffic.
    // In case of Aggregated view, CURRENT_STATES based RoutingTable doesn't work since CURRENT_STATES are
    // not aggregated by helix. So, we only have one option, i.e EXTERNAL_VIEW based and helix team asked to use it.
    RoutingTableProvider routingTableProvider = new RoutingTableProvider(helixManager, PropertyType.EXTERNALVIEW);
    logger.info("Routing table provider is created for cluster {} via Helix manager at {}", aggregatedViewClusterName,
        localZkConnectStr);
    routingTableProvider.addRoutingTableChangeListener(clusterChangeHandler, null);
    logger.info("Registered routing table change listeners for cluster {}", aggregatedViewClusterName);
    // Since it is possible that initial event occurs before adding routing table listener, we explicitly set snapshot in
    // HelixClusterManager. The reason is, if listener missed initial event, snapshot inside routing table
    // provider should be already populated.
    clusterChangeHandler.setRoutingTableSnapshot(routingTableProvider.getRoutingTableSnapshot());
    // The initial routing table change should populate the instanceConfigs. If it's empty that means initial
    // change didn't come and thread should wait on the init latch to ensure routing table snapshot is non-empty
    if (clusterChangeHandler.getRoutingTableSnapshot(null).getInstanceConfigs().isEmpty()) {
      // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
      // trigger routing table change within 5 minutes
      logger.info("Routing table snapshot for cluster is currently empty. Waiting for initial notification.");
      clusterChangeHandler.waitForInitNotification();
    }

    // 3. Fetch live instance changes in entire cluster
    // When adding a listener, current live instances are fetched from Helix using blocking call and notified to the
    // listener (Please follow below method to
    // https://github.com/apache/helix/blob/fb773839117ee0689451fab7c8b949115f555100/helix-core/src/main/java/org/apache/helix/manager/zk/CallbackHandler.java#L393).
    // I.e. by the time we return from below method, initial notification of live instances states would have been
    // arrived. So, we don't need to separate latch to wait for notification of live instances.
    helixManager.addLiveInstanceChangeListener(clusterChangeHandler);
    logger.info("Registered live instance change listeners for cluster {} via Helix manager at {}",
        aggregatedViewClusterName, localZkConnectStr);

    return new HelixAggregatedViewClusterInfo(helixManager, clusterChangeHandler, dataNodeConfigSources,
        routingTableProvider);
  }

  /**
   * Register for data node configs from Helix Property store.
   * @param dcName data center for which we are interested in data node configs.
   */
  private void registerForDataNodeConfigsChanges(String dcName) throws Exception {
    DcZkInfo dcZkInfo = dataCenterToZkAddress.get(dcName);
    String zkConnectStr = dcZkInfo.getZkConnectStrs().get(0);
    DataNodeConfigSource dataNodeConfigSource =
        helixFactory.getDataNodeConfigSource(clusterMapConfig, zkConnectStr, dataNodeConfigSourceMetrics);
    // When attaching a listener, current data is fetched from Helix using blocking call and notified to the listener
    // (Please see PropertyStoreToDataNodeConfigAdapter.Subscription#start() method). I.e. by the time we return from
    // below method, initial notification of data node configs would have been arrived. So, we don't need separate latch
    // to wait for notification of data node configs.
    logger.info(
        "Registering data node config change listeners for cluster {} in data center {} via Helix manager at {}",
        clusterMapConfig.clusterMapClusterName, dcZkInfo.getDcName(), zkConnectStr);
    dataNodeConfigSource.addDataNodeConfigChangeListener(new DataNodeConfigChangeListener() {
      // We could directly register HelixClusterChangeHandler as listener. But, in order to the pass data center name
      // and helix cluster name for logging purpose, having an additional listener here.
      @Override
      public void onDataNodeConfigChange(Iterable<DataNodeConfig> configs) {
        clusterChangeHandler.handleDataNodeConfigChange(configs, dcName, clusterMapConfig.clusterMapClusterName);
      }

      @Override
      public void onDataNodeDelete(String instanceName) {
        clusterChangeHandler.onDataNodeDelete(instanceName);
      }
    });
    dataNodeConfigSources.add(dataNodeConfigSource);
  }
}
