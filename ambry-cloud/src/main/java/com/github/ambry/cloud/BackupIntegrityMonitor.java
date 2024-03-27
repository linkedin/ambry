/**
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.CompositeClusterManager;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.StorageManager;
import com.github.ambry.utils.Utils;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.*;


/**
 * Checks the integrity of backup partitions in Azure
 */
public class BackupIntegrityMonitor implements Callable<Integer> {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  private final AtomicBoolean stop;
  private final ClusterMapConfig clusterMapConfig;
  private final CompositeClusterManager compositeClusterManager;
  private final DataNodeId nodeId;
  private final FindTokenHelper tokenFactory;
  private final RecoveryManager azureReplicationManager;
  private final RecoveryThread azureReplicator;
  private final ReplicaThread serverReplicator;
  private final ReplicationManager serverReplicationManager;
  private final ScheduledExecutorService executor;
  private final StorageManager storageManager;

  public BackupIntegrityMonitor(RecoveryManager azure, ReplicationManager server,
      CompositeClusterManager cluster, StorageManager storage, DataNodeId node, FindTokenHelper token,
      VerifiableProperties properties) {
    azureReplicationManager = azure;
    azureReplicator = azure.getRecoveryThread("ambry_backup_integrity_monitor");
    clusterMapConfig = new ClusterMapConfig(properties);
    compositeClusterManager = cluster;
    executor = Utils.newScheduler(1, "ambry_backup_integrity_monitor_", true);
    nodeId = node;
    serverReplicationManager = server;
    serverReplicator = server.getBackupCheckerThread("ambry_backup_integrity_monitor");
    stop = new AtomicBoolean(false);
    storageManager = storage;
    tokenFactory = token;
    // log disk state
    compositeClusterManager.getStaticClusterManager().getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds()
        .forEach(d -> logger.info("[BackupIntegrityMonitor] Disk = {} {} {} bytes", d.getMountPath(), d.getState(),
            d.getRawCapacityInBytes()));
    logger.info("Created BackupIntegrityMonitor");
  }

  /**
   * Starts and schedules monitor
   */
  public void start() {
    executor.schedule(this::call, 0, TimeUnit.SECONDS);
    logger.info("[BackupIntegrityMonitor] Started BackupIntegrityMonitor");
  }

  /**
   * Shut down the monitor waiting for in progress operations to complete.
   */
  public void shutdown() {
    /*
      Shutdown executor.
      This arbitrary wait is merely an attempt to allow the worker threads to gracefully exit.
      We will force a shutdown later. All workers are daemons and JVM _will_ exit when only daemons remain.
      Any data inconsistencies must be resolved separately, but not by trying to predict the right shutdown timeout.
    */
    logger.info("[BackupIntegrityMonitor] Shutting down BackupIntegrityMonitor");
    stop.set(true);
    Utils.shutDownExecutorService(executor, 10, TimeUnit.SECONDS);
    logger.info("[BackupIntegrityMonitor] Completed shutting down BackupIntegrityMonitor");
  }

  /**
   * This method randomly picks a partition in a cluster and locates its replica in the cluster and the cloud.
   * Then it downloads and compares data from both replicas.
   *
   * TODO: Implement verificaion logic
   * @return 0 for success, 1 for failure
   * @throws Exception
   */
  @Override
  public Integer call() throws Exception {
    Random random = new Random();
    while (!stop.get()) {
      // TODO: Implement verification logic
      // 1. Pick a partition P, replica R from helix cluster-map
      // 2. Pick a disk D using static cluster-map
      // 3. while(!done) { RecoveryThread::replicate(P) } - recover partition P from cloud and store metadata + data in disk D
      // 4. while(!done) { BackupCheckerThread::replicate(R) } - copy metadata from replica R and compare with metadata in disk D
      List<PartitionId> partitions = compositeClusterManager.getHelixClusterManager().getAllPartitionIds(null);
      PartitionId partition = partitions.get(random.nextInt(partitions.size()));
      List<ReplicaId> replicas = (List<ReplicaId>) partition.getReplicaIds();
      ReplicaId serverReplica = replicas.get(random.nextInt(replicas.size()));
      logger.info("[BackupIntegrityMonitor] Verifying {}:{}", serverReplica.getReplicaPath(),
          serverReplica.getDataNodeId().getHostname());
      // TODO
      sleep(Duration.ofSeconds(10).toMillis());
    }
    return 0;
  }
}
