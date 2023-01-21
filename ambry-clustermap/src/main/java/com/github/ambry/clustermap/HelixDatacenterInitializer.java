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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
class HelixDatacenterInitializer {
  private static final Logger logger = LoggerFactory.getLogger(HelixDatacenterInitializer.class);
  private final CompletableFuture<DcInfo> initializationFuture = new CompletableFuture<>();
  private final ClusterMapConfig clusterMapConfig;
  private final HelixManager localManager;
  private final HelixFactory helixFactory;
  private final ClusterMapUtils.DcZkInfo dcZkInfo;
  private final String dcName;
  private final HelixClusterManager helixClusterManager;
  private final String selfInstanceName;
  private final DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics;

  /**
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param localManager the {@link HelixManager} for the local datacenter that has already been started.
   * @param helixFactory the {@link HelixFactory} instance to construct managers.
   * @param dcZkInfo info about the DC, like connection string, name, and replica type
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param dataNodeConfigSourceMetrics metrics related to {@link DataNodeConfigSource}.
   * @param helixClusterManager {@link HelixClusterManager} instance that manages and stores the cluster information.
   */
  HelixDatacenterInitializer(ClusterMapConfig clusterMapConfig, HelixManager localManager, HelixFactory helixFactory,
      ClusterMapUtils.DcZkInfo dcZkInfo, String selfInstanceName,
      DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics, HelixClusterManager helixClusterManager) {
    this.clusterMapConfig = clusterMapConfig;
    this.localManager = localManager;
    this.helixFactory = helixFactory;
    this.dcZkInfo = dcZkInfo;
    this.selfInstanceName = selfInstanceName;
    this.dataNodeConfigSourceMetrics = dataNodeConfigSourceMetrics;
    dcName = dcZkInfo.getDcName();
    this.helixClusterManager = helixClusterManager;
  }

  /**
   * Begin the startup procedure in a background thread.
   */
  public void start() {
    Utils.newThread(() -> {
      try {
        DcInfo dcInfo;
        if (dcZkInfo.getReplicaType() == ReplicaType.DISK_BACKED) {
          dcInfo = initializeHelixDatacenter();
        } else {
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
      throw Utils.extractFutureExceptionCause(e);
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
      manager = helixFactory.getZkHelixManagerAndConnect(clusterMapConfig.clusterMapClusterName, selfInstanceName,
          InstanceType.SPECTATOR, zkConnectStr);
    }
    HelixClusterChangeHandler clusterChangeHandler =
        helixClusterManager.new HelixClusterChangeHandler(dcName, clusterMapConfig.clusterMapClusterName,
            this::onInitializationFailure, false);
    // Create Helix RoutingTableProvider of each DC to keep track of partition(replicas) state. Here, we use CURRENT
    // STATES based RoutingTableProvider to remove dependency on Helix's pipeline and reduce notification latency.
    // To elaborate more, there are two ways to instantiate a RoutingTable. 1. EXTERNAL_VIEW based, 2. CURRENT_STATES
    // based. In the former one, helix controller generates the external view and this is read by the helix spectator to
    // create the Routing table. In the latter one, CURRENT STATES are read from helix participant to participant at the
    // helix spectator to create the Routing table. According to helix team, the former one usually takes longer time
    // since it is dependent on helix controller to generate up-to-date view but has less read traffic since we have to
    // read constructed view. The latter one takes lesser time since we don't have to wait for controller to calculate
    // the view but has more read traffic.
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
    DataNodeConfigSource dataNodeConfigSource =
        helixFactory.getDataNodeConfigSource(clusterMapConfig, zkConnectStr, dataNodeConfigSourceMetrics);
    dataNodeConfigSource.addDataNodeConfigChangeListener(clusterChangeHandler);
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
    if (clusterChangeHandler.getRoutingTableSnapshot(dcName).getInstanceConfigs().isEmpty()) {
      // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
      // trigger routing table change within 5 minutes
      logger.info("Routing table snapshot in {} is currently empty. Waiting for initial notification.", dcName);
      clusterChangeHandler.waitForInitNotification();
    }

    if (!clusterMapConfig.clustermapListenCrossColo && manager != localManager) {
      manager.disconnect();
      logger.info("Stopped listening to cross colo ZK server {}", zkConnectStr);
    }

    return new HelixDcInfo(dcName, dcZkInfo, manager, clusterChangeHandler, dataNodeConfigSource, routingTableProvider);
  }
}
