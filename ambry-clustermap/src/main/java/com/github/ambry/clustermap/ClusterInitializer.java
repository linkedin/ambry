/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.HelixClusterManager.HelixClusterChangeHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents the startup procedure for entire cluster. Calling the start method will begin the startup
 * procedure in a background thread. Calling the {@link #join} method let's the main thread wait for startup of a
 * datacenter to either succeed or fail.
 */
class ClusterInitializer {
  private static final Logger logger = LoggerFactory.getLogger(ClusterInitializer.class);
  private final CompletableFuture<ClusterInfo> initializationFuture = new CompletableFuture<>();
  private final ClusterMapConfig clusterMapConfig;
  private final HelixFactory helixFactory;
  private final String selfInstanceName;
  private final HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback;
  private final DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics;
  private final HelixClusterManager helixClusterManager;
  private final Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;

  /**
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param dataCenterToZkAddress map of data center to ZK information.
   * @param helixFactory the {@link HelixFactory} instance to construct managers.
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param helixClusterManagerCallback a help class to get cluster state from all DCs.
   * @param dataNodeConfigSourceMetrics metrics related to {@link DataNodeConfigSource}.
   * @param helixClusterManager reference to {@link HelixClusterManager} that creates this class.
   */
  ClusterInitializer(ClusterMapConfig clusterMapConfig, Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress,
      HelixFactory helixFactory, String selfInstanceName,
      HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback,
      DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics, HelixClusterManager helixClusterManager) {
    this.clusterMapConfig = clusterMapConfig;
    this.dataCenterToZkAddress = dataCenterToZkAddress;
    this.helixFactory = helixFactory;
    this.selfInstanceName = selfInstanceName;
    this.helixClusterManagerCallback = helixClusterManagerCallback;
    this.dataNodeConfigSourceMetrics = dataNodeConfigSourceMetrics;
    this.helixClusterManager = helixClusterManager;
  }

  /**
   * Begin the startup procedure in a background thread.
   * TODO: Since we would only have one ClusterInitializer (unlike multiple DataCenterInitializers), we could perform
   *  the initialization in the calling thread itself.
   */
  public void start() {
    Utils.newThread(() -> {
      try {
        ClusterInfo clusterInfo = initializeHelixCluster();
        initializationFuture.complete(clusterInfo);
      } catch (Exception e) {
        logger.warn("Exception while initializing cluster", e);
        onInitializationFailure(e);
      }
    }, false).start();
  }

  /**
   * Wait for startup to either succeed or fail.
   * @return the {@link ClusterInfo} that contains the {@link HelixClusterChangeHandler}.
   * @throws Exception if startup of the datacenter fails.
   */
  ClusterInfo join() throws Exception {
    try {
      return initializationFuture.get();
    } catch (ExecutionException e) {
      throw Utils.extractFutureExceptionCause(e);
    }
  }

  /**
   * Callback to complete the future when an error occurs
   * @param e the {@link Exception}.
   */
  private void onInitializationFailure(Exception e) {
    initializationFuture.completeExceptionally(e);
  }

  /**
   * Perform initialization for a helix-managed datacenter of servers.
   * @return the {@link DcInfo} for the datacenter.
   * @throws Exception if something went wrong during startup
   */
  public HelixClusterInfo initializeHelixCluster() throws Exception {
    // We can assume that Helix Aggregated view cluster will be present in same zookeeper hosting regular Ambry cluster
    // information. Get the HelixManager to talk to the local zk service.
    ClusterMapUtils.DcZkInfo localDcZkInfo = dataCenterToZkAddress.get(clusterMapConfig.clusterMapDatacenterName);

    // For now, the first ZK endpoint (if there are more than one endpoints) will be adopted by default for initialization.
    // Note that, Ambry currently doesn't support multiple spectators, because there should be only one source of truth.
    String localZkConnectStr = localDcZkInfo.getZkConnectStrs().get(0);
    HelixManager aggregatedViewManager =
        helixFactory.getZkHelixManagerAndConnect(clusterMapConfig.clusterMapAggregatedViewClusterName, selfInstanceName,
            InstanceType.SPECTATOR, localZkConnectStr);
    logger.info("Cluster name {}", aggregatedViewManager.getClusterName());
    logger.info("Clusters Info {}", aggregatedViewManager.getClusterManagmentTool().getClusters());

    HelixClusterChangeHandler clusterChangeHandler =
        helixClusterManager.new HelixClusterChangeHandler(clusterMapConfig.clusterMapClusterName, helixClusterManagerCallback,
            this::onInitializationFailure, true);

    // Create RoutingTableProvider of entire cluster to keep track of partition(replicas) state. Here, we use current
    // state based RoutingTableProvider to remove dependency on Helix's pipeline and reduce notification latency.
    logger.info("Creating routing table provider for entire cluster associated with Helix manager at {}",
        localZkConnectStr);
    RoutingTableProvider routingTableProvider =
        new RoutingTableProvider(aggregatedViewManager, PropertyType.CURRENTSTATES);
    logger.info("Routing table provider is created for entire cluster");
    routingTableProvider.addRoutingTableChangeListener(clusterChangeHandler, null);
    logger.info("Registered routing table change listeners for entire cluster");

    // Helix property store Znodes (which contain node configs like disks, replicas hosted on each disk, etc) and ideal
    // state Znodes are not aggregated. We need to listen to original helix source clusters for these details from both
    // local and remote colos.
    List<DataNodeConfigSource> dataNodeConfigSources = new ArrayList<>();
    for (ClusterMapUtils.DcZkInfo dcZkInfo : dataCenterToZkAddress.values()) {
      String zkConnectStr = dcZkInfo.getZkConnectStrs().get(0);
      // Register Data node configs listener.
      DataNodeConfigSource dataNodeConfigSource =
          helixFactory.getDataNodeConfigSource(clusterMapConfig, zkConnectStr, dataNodeConfigSourceMetrics);
      dataNodeConfigSource.addDataNodeConfigChangeListener(clusterChangeHandler);
      dataNodeConfigSources.add(dataNodeConfigSource);
      logger.info("Registered data node config change listeners for data center {} via Helix manager at {}",
          dcZkInfo.getDcName(), zkConnectStr);
      // Register ideal state listener.
      HelixManager manager =
          helixFactory.getZkHelixManagerAndConnect(clusterMapConfig.clusterMapClusterName, selfInstanceName,
              InstanceType.SPECTATOR, zkConnectStr);
      manager.addIdealStateChangeListener(clusterChangeHandler);
      logger.info("Registered ideal state change listeners for data center {} via Helix manager at {}",
          dcZkInfo.getDcName(), zkConnectStr);
    }

    // Register listeners to get notified on live instance change in entire cluster.
    aggregatedViewManager.addLiveInstanceChangeListener(clusterChangeHandler);
    logger.info("Registered live instance change listeners for entire cluster via Helix manager at {}",
        localZkConnectStr);

    // Since it is possible that initial event occurs before adding routing table listener, we explicitly set snapshot in
    // HelixClusterManager. The reason is, if listener missed initial event, snapshot inside routing table
    // provider should be already populated.
    clusterChangeHandler.setRoutingTableSnapshot(routingTableProvider.getRoutingTableSnapshot());
    // The initial routing table change should populate the instanceConfigs. If it's empty that means initial
    // change didn't come and thread should wait on the init latch to ensure routing table snapshot is non-empty
    if (helixClusterManagerCallback.getRoutingTableSnapshot(null).getInstanceConfigs().isEmpty()) {
      // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
      // trigger routing table change within 5 minutes
      logger.info("Routing table snapshot for cluster is currently empty. Waiting for initial notification.");
      clusterChangeHandler.waitForInitNotification();
    }

    return new HelixClusterInfo(aggregatedViewManager, clusterChangeHandler, dataNodeConfigSources);
  }
}
