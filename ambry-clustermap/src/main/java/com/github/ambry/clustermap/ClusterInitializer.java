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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
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

  // Fields to pass into both ClusterChangeHandlers
  private final String selfInstanceName;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap;
  private final HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback;
  private final HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback;
  private final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private final DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics;
  private final AtomicLong sealedStateChangeCounter;
  // Fields to pass into only SimpleClusterChangeHandler (These can be removed if SimpleClusterChangeHandler is removed)
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap;
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition;
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas;
  private final Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;

  /**
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param dataCenterToZkAddress map of data center to ZK information.
   * @param helixFactory the {@link HelixFactory} instance to construct managers.
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param partitionOverrideInfoMap a map specifying partitions whose state should be overridden.
   * @param clusterChangeHandlerCallback a call back that allows current handler to update cluster-wide info.
   * @param helixClusterManagerCallback a help class to get cluster state from all DCs.
   * @param helixClusterManagerMetrics metrics that help track of cluster changes and infos.
   * @param dataNodeConfigSourceMetrics metrics related to {@link DataNodeConfigSource}.
   * @param sealedStateChangeCounter a counter that records event when replica is sealed or unsealed
   * @param partitionMap a map from serialized bytes to corresponding partition.
   * @param partitionNameToAmbryPartition a map from partition name to {@link AmbryPartition} object.
   * @param ambryPartitionToAmbryReplicas a map from {@link AmbryPartition} to its replicas.
   */
  ClusterInitializer(ClusterMapConfig clusterMapConfig, Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress,
      HelixFactory helixFactory, String selfInstanceName, Map<String, Map<String, String>> partitionOverrideInfoMap,
      HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback,
      HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback,
      HelixClusterManagerMetrics helixClusterManagerMetrics, DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics,
      AtomicLong sealedStateChangeCounter, ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap,
      ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition,
      ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas) {
    this.clusterMapConfig = clusterMapConfig;
    this.dataCenterToZkAddress = dataCenterToZkAddress;
    this.helixFactory = helixFactory;
    this.selfInstanceName = selfInstanceName;
    this.partitionOverrideInfoMap = partitionOverrideInfoMap;
    this.clusterChangeHandlerCallback = clusterChangeHandlerCallback;
    this.helixClusterManagerCallback = helixClusterManagerCallback;
    this.helixClusterManagerMetrics = helixClusterManagerMetrics;
    this.dataNodeConfigSourceMetrics = dataNodeConfigSourceMetrics;
    this.sealedStateChangeCounter = sealedStateChangeCounter;
    this.partitionMap = partitionMap;
    this.partitionNameToAmbryPartition = partitionNameToAmbryPartition;
    this.ambryPartitionToAmbryReplicas = ambryPartitionToAmbryReplicas;
  }

  /**
   * Begin the startup procedure in a background thread.
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
   * @return the {@link DcInfo} for the datacenter (a handle to the stateful objects related to the datacenter).
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

    // TODO: Check with helix team on below.
    // We can assume that Helix Aggregated view cluster will be present in local zookeeper hosting regular ambry cluster
    // information. Get the HelixManager to talk to the local zk service.
    ClusterMapUtils.DcZkInfo localDcZkInfo = dataCenterToZkAddress.get(clusterMapConfig.clusterMapDatacenterName);
    // For now, the first ZK endpoint (if there are more than one endpoints) will be adopted by default for initialization.
    // Note that, Ambry currently doesn't support multiple spectators, because there should be only one source of truth.
    String localZkConnectStr = localDcZkInfo.getZkConnectStrs().get(0);
    HelixManager aggregatedViewManager =
        helixFactory.getZkHelixManagerAndConnect(clusterMapConfig.clusterMapAggregatedViewClusterName, selfInstanceName,
            InstanceType.SPECTATOR, localZkConnectStr);

    HelixClusterChangeHandler clusterChangeHandler;
    String clusterChangeHandlerType = clusterMapConfig.clusterMapClusterChangeHandlerType;
    if (clusterChangeHandlerType.equals(SimpleClusterChangeHandler.class.getSimpleName())) {
      clusterChangeHandler =
          new SimpleClusterChangeHandler(clusterMapConfig, clusterMapConfig.clusterMapClusterName, selfInstanceName, partitionOverrideInfoMap,
              partitionMap, partitionNameToAmbryPartition, ambryPartitionToAmbryReplicas, helixClusterManagerCallback,
              helixClusterManagerMetrics, this::onInitializationFailure, sealedStateChangeCounter);
    } else if (clusterChangeHandlerType.equals(DynamicClusterChangeHandler.class.getSimpleName())) {
      clusterChangeHandler =
          new DynamicClusterChangeHandler(clusterMapConfig, clusterMapConfig.clusterMapClusterName, selfInstanceName, partitionOverrideInfoMap,
              helixClusterManagerCallback, clusterChangeHandlerCallback, helixClusterManagerMetrics,
              this::onInitializationFailure, sealedStateChangeCounter);
    } else {
      throw new IllegalArgumentException("Unsupported cluster change handler type: " + clusterChangeHandlerType);
    }

    logger.info("Cluster name {}", aggregatedViewManager.getClusterName());
    logger.info("Clusters Info {}", aggregatedViewManager.getClusterManagmentTool().getClusters());
    // Create RoutingTableProvider of each DC to keep track of partition(replicas) state. Here, we use current
    // state based RoutingTableProvider to remove dependency on Helix's pipeline and reduce notification latency.
    logger.info("Creating routing table provider for entire cluster associated with Helix manager at {}",
        localZkConnectStr);
    RoutingTableProvider routingTableProvider =
        new RoutingTableProvider(aggregatedViewManager, PropertyType.CURRENTSTATES);
    logger.info("Routing table provider is created for entire cluster");
    routingTableProvider.addRoutingTableChangeListener(clusterChangeHandler, null);
    logger.info("Registered routing table change listeners for entire cluster");

    // Since data node configs are stored in helix property store now and aggregation view is not supported for
    // property store yet, we need to add listeners for data node config sources
    List<DataNodeConfigSource> dataNodeConfigSources = new ArrayList<>();
    for (ClusterMapUtils.DcZkInfo dcZkInfo : dataCenterToZkAddress.values()) {
      String zkConnectStr = dcZkInfo.getZkConnectStrs().get(0);
      DataNodeConfigSource dataNodeConfigSource =
          helixFactory.getDataNodeConfigSource(clusterMapConfig, zkConnectStr, dataNodeConfigSourceMetrics);
      dataNodeConfigSource.addDataNodeConfigChangeListener(clusterChangeHandler);
      dataNodeConfigSources.add(dataNodeConfigSource);
      logger.info("Registered data node config change listeners for data center {} via Helix manager at {}",
          dcZkInfo.getDcName(), zkConnectStr);
    }

    aggregatedViewManager.addIdealStateChangeListener(clusterChangeHandler);
    logger.info("Registered ideal state change listeners for entire cluster via Helix manager at {}",
        localZkConnectStr);
    // Now register listeners to get notified on live instance change in every datacenter.
    aggregatedViewManager.addLiveInstanceChangeListener(clusterChangeHandler);
    logger.info("Registered live instance change listeners for entire cluster via Helix manager at {}",
        localZkConnectStr);

    // in case initial event occurs before adding routing table listener, here we explicitly set snapshot in
    // ClusterChangeHandler. The reason is, if listener missed initial event, snapshot inside routing table
    // provider should be already populated.
    clusterChangeHandler.setRoutingTableSnapshot(routingTableProvider.getRoutingTableSnapshot());
    // the initial routing table change should populate the instanceConfigs. If it's empty that means initial
    // change didn't come and thread should wait on the init latch to ensure routing table snapshot is non-empty
    if (clusterChangeHandler.getRoutingTableSnapshot().getInstanceConfigs().isEmpty()) {
      // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
      // trigger routing table change within 5 minutes
      logger.info("Routing table snapshot for cluster is currently empty. Waiting for initial notification.");
      clusterChangeHandler.waitForInitNotification();
    }

    return new HelixClusterInfo(aggregatedViewManager, clusterChangeHandler, dataNodeConfigSources);
  }
}
