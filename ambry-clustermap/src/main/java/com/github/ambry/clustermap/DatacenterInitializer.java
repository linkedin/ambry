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
import java.util.Map;
import java.util.Objects;
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
 * This class represents the startup procedure for a datacenter. Calling the start method will begin the startup
 * procedure in a background thread. Calling the {@link #join} method let's the main thread wait for startup of a
 * datacenter to either succeed or fail.
 */
class DatacenterInitializer {
  private static final Logger logger = LoggerFactory.getLogger(DatacenterInitializer.class);
  private final CompletableFuture<DcInfo> initializationFuture = new CompletableFuture<>();
  private final ClusterMapConfig clusterMapConfig;
  private final HelixManager localManager;
  private final HelixFactory helixFactory;
  private final ClusterMapUtils.DcZkInfo dcZkInfo;
  private final String dcName;

  // Fields to pass into both ClusterChangeHandlers
  private final String selfInstanceName;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap;
  private final HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback;
  private final HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback;
  private final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private final AtomicLong sealedStateChangeCounter;
  // Fields to pass into only SimpleClusterChangeHandler (These can be removed if SimpleClusterChangeHandler is removed)
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap;
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition;
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas;

  /**
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param localManager the {@link HelixManager} for the local datacenter that has already been started.
   * @param helixFactory the {@link HelixFactory} instance to construct managers.
   * @param dcZkInfo info about the DC, like connection string, name, and replica type
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param partitionOverrideInfoMap a map specifying partitions whose state should be overridden.
   * @param clusterChangeHandlerCallback a call back that allows current handler to update cluster-wide info.
   * @param helixClusterManagerCallback a help class to get cluster state from all DCs.
   * @param helixClusterManagerMetrics metrics that help track of cluster changes and infos.
   * @param sealedStateChangeCounter a counter that records event when replica is sealed or unsealed
   * @param partitionMap a map from serialized bytes to corresponding partition.
   * @param partitionNameToAmbryPartition a map from partition name to {@link AmbryPartition} object.
   * @param ambryPartitionToAmbryReplicas a map from {@link AmbryPartition} to its replicas.
   */
  DatacenterInitializer(ClusterMapConfig clusterMapConfig, HelixManager localManager, HelixFactory helixFactory,
      ClusterMapUtils.DcZkInfo dcZkInfo, String selfInstanceName,
      Map<String, Map<String, String>> partitionOverrideInfoMap,
      HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback,
      HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback,
      HelixClusterManagerMetrics helixClusterManagerMetrics, AtomicLong sealedStateChangeCounter,
      ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap,
      ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition,
      ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas) {
    this.clusterMapConfig = clusterMapConfig;
    this.localManager = localManager;
    this.helixFactory = helixFactory;
    this.dcZkInfo = dcZkInfo;
    this.selfInstanceName = selfInstanceName;
    this.partitionOverrideInfoMap = partitionOverrideInfoMap;
    this.clusterChangeHandlerCallback = clusterChangeHandlerCallback;
    this.helixClusterManagerCallback = helixClusterManagerCallback;
    this.helixClusterManagerMetrics = helixClusterManagerMetrics;
    this.sealedStateChangeCounter = sealedStateChangeCounter;
    this.partitionMap = partitionMap;
    this.partitionNameToAmbryPartition = partitionNameToAmbryPartition;
    this.ambryPartitionToAmbryReplicas = ambryPartitionToAmbryReplicas;
    dcName = dcZkInfo.getDcName();
  }

  /**
   * Begin the startup procedure in a background thread.
   */
  public void start() {
    Utils.newThread(() -> {
      try {
        DcInfo dcInfo;
        switch (dcZkInfo.getReplicaType()) {
          case DISK_BACKED:
            dcInfo = initializeHelixDatacenter();
            break;
          case CLOUD_BACKED:
            dcInfo = initializeCloudDatacenter();
            break;
          default:
            throw new UnsupportedOperationException("Unknown replica type: " + dcZkInfo.getReplicaType());
        }
        initializationFuture.complete(dcInfo);
      } catch (Exception e) {
        logger.warn("Exception while initializing datacenter {}", dcName, e);
        onInitializationFailure(e);
      }
    }, false).start();
  }

  /**
   * Wait for startup to either succeed or fail.
   * @return the {@link DcInfo} for the datacenter (a handle to the stateful objects related to the datacenter).
   * @throws Exception if startup of the datacenter fails.
   */
  DcInfo join() throws Exception {
    try {
      return initializationFuture.get();
    } catch (ExecutionException e) {
      throw Utils.extractExecutionExceptionCause(e);
    }
  }

  /**
   * @return the datacenter name for this initializer.
   */
  public String getDcName() {
    return dcName;
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
  private DcInfo initializeHelixDatacenter() throws Exception {
    // For now, the first ZK endpoint (if there are more than one endpoints) will be adopted by default for initialization.
    // Note that, Ambry currently doesn't support multiple spectators, because there should be only one source of truth.
    String zkConnectStr = dcZkInfo.getZkConnectStrs().get(0);
    HelixManager manager;
    if (dcZkInfo.getDcName().equals(clusterMapConfig.clusterMapDatacenterName)) {
      manager = Objects.requireNonNull(localManager, "localManager should have been set");
    } else {
      manager = helixFactory.getZKHelixManager(clusterMapConfig.clusterMapClusterName, selfInstanceName,
          InstanceType.SPECTATOR, zkConnectStr);
      logger.info("Connecting to Helix manager at {}", zkConnectStr);
      manager.connect();
      logger.info("Established connection to Helix manager at {}", zkConnectStr);
    }
    HelixClusterChangeHandler clusterChangeHandler;
    String clusterChangeHandlerType = clusterMapConfig.clusterMapClusterChangeHandlerType;
    if (clusterChangeHandlerType.equals(SimpleClusterChangeHandler.class.getSimpleName())) {
      clusterChangeHandler =
          new SimpleClusterChangeHandler(clusterMapConfig, dcName, selfInstanceName, partitionOverrideInfoMap,
              partitionMap, partitionNameToAmbryPartition, ambryPartitionToAmbryReplicas, helixClusterManagerCallback,
              helixClusterManagerMetrics, this::onInitializationFailure, sealedStateChangeCounter);
    } else if (clusterChangeHandlerType.equals(DynamicClusterChangeHandler.class.getSimpleName())) {
      clusterChangeHandler =
          new DynamicClusterChangeHandler(clusterMapConfig, dcName, selfInstanceName, partitionOverrideInfoMap,
              helixClusterManagerCallback, clusterChangeHandlerCallback, helixClusterManagerMetrics,
              this::onInitializationFailure, sealedStateChangeCounter);
    } else {
      throw new IllegalArgumentException("Unsupported cluster change handler type: " + clusterChangeHandlerType);
    }
    // Create RoutingTableProvider of each DC to keep track of partition(replicas) state. Here, we use current
    // state based RoutingTableProvider to remove dependency on Helix's pipeline and reduce notification latency.
    logger.info("Creating routing table provider associated with Helix manager at {}", zkConnectStr);
    RoutingTableProvider routingTableProvider = new RoutingTableProvider(manager, PropertyType.CURRENTSTATES);
    logger.info("Routing table provider is created in {}", dcName);
    routingTableProvider.addRoutingTableChangeListener(clusterChangeHandler, null);
    logger.info("Registered routing table change listeners in {}", dcName);

    // The initial instance config change notification is required to populate the static cluster
    // information, and only after that is complete do we want the live instance change notification to
    // come in. We do not need to do anything extra to ensure this, however, since Helix provides the initial
    // notification for a change from within the same thread that adds the listener, in the context of the add
    // call. Therefore, when the call to add a listener returns, the initial notification will have been
    // received and handled.
    manager.addInstanceConfigChangeListener(clusterChangeHandler);
    logger.info("Registered instance config change listeners for Helix manager at {}", zkConnectStr);
    manager.addIdealStateChangeListener(clusterChangeHandler);
    logger.info("Registered ideal state change listeners for Helix manager at {}", zkConnectStr);
    // Now register listeners to get notified on live instance change in every datacenter.
    manager.addLiveInstanceChangeListener(clusterChangeHandler);
    logger.info("Registered live instance change listeners for Helix manager at {}", zkConnectStr);

    // in case initial event occurs before adding routing table listener, here we explicitly set snapshot in
    // ClusterChangeHandler. The reason is, if listener missed initial event, snapshot inside routing table
    // provider should be already populated.
    clusterChangeHandler.setRoutingTableSnapshot(routingTableProvider.getRoutingTableSnapshot());
    // the initial routing table change should populate the instanceConfigs. If it's empty that means initial
    // change didn't come and thread should wait on the init latch to ensure routing table snapshot is non-empty
    if (clusterChangeHandler.getRoutingTableSnapshot().getInstanceConfigs().isEmpty()) {
      // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
      // trigger routing table change within 5 minutes
      logger.info("Routing table snapshot in {} is currently empty. Waiting for initial notification.", dcName);
      clusterChangeHandler.waitForInitNotification();
    }

    if (!clusterMapConfig.clustermapListenCrossColo && manager != localManager) {
      manager.disconnect();
      logger.info("Stopped listening to cross colo ZK server {}", zkConnectStr);
    }

    return new HelixDcInfo(dcName, dcZkInfo, manager, clusterChangeHandler);
  }

  /**
   * Currently, this does not connect to the VCR zookeeper and assumes that all partitions are supported in the cloud
   * datacenter. This will be the case until the VCR and native storage clusters are unified under the same
   * {@link ClusterMap}. Once this happens, we can use the VCR cluster as a source of truth for supported partitions
   * in the cloud datacenter.
   * @return the {@link DcInfo} for the cloud datacenter.
   * @throws Exception if something went wrong during startup
   */
  private DcInfo initializeCloudDatacenter() throws Exception {
    CloudServiceClusterChangeHandler clusterChangeHandler =
        new CloudServiceClusterChangeHandler(dcName, clusterMapConfig, clusterChangeHandlerCallback);
    return new DcInfo(dcName, dcZkInfo, clusterChangeHandler);
  }
}
