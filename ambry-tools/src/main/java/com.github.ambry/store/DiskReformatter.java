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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reformats all the stores on a given disk.
 */
public class DiskReformatter {
  private static final String TEMP_RELOCATION_DIR_NAME = "temp_relocated_store";
  private static final String TEMP_COPY_DIR_NAME = "temp_copied_store";
  private static final Logger logger = LoggerFactory.getLogger(DiskReformatter.class);

  private final DataNodeId dataNodeId;
  private final long fetchSizeInBytes;
  private final StoreConfig storeConfig;
  private final StoreKeyFactory storeKeyFactory;
  private final ClusterMap clusterMap;
  private final Time time;
  private final ConsistencyCheckerTool consistencyChecker;
  private final DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);

  /**
   * Config for the reformatter.
   */
  private static class DiskReformatterConfig {
    /**
     * The path to the hardware layout file.
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file.
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * The hostname of the target server as it appears in the partition layout.
     */
    @Config("datanode.hostname")
    final String datanodeHostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to).
     */
    @Config("datanode.port")
    final int datanodePort;

    /**
     * Comma separated list of the mount paths of the disks whose partitions need to be re-formatted
     */
    @Config("disk.mount.paths")
    final String[] diskMountPaths;

    /**
     * Comma separated list of the paths to the scratch spaces to which a partition on the disks can be temporarily
     * relocated. This has to be 1-1 mapped with the list of mount paths.
     */
    @Config("scratch.paths")
    final String[] scratchPaths;

    /**
     * The size of each fetch from the source store.
     */
    @Config("fetch.size.in.bytes")
    @Default("4 * 1024 * 1024")
    final long fetchSizeInBytes;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    DiskReformatterConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
      datanodeHostname = verifiableProperties.getString("datanode.hostname");
      datanodePort = verifiableProperties.getIntInRange("datanode.port", 1, 65535);
      diskMountPaths = verifiableProperties.getString("disk.mount.paths").split(",");
      scratchPaths = verifiableProperties.getString("scratch.paths").split(",");
      fetchSizeInBytes = verifiableProperties.getLongInRange("fetch.size.in.bytes", 4 * 1024 * 1024, 1, Long.MAX_VALUE);
      if (scratchPaths.length != diskMountPaths.length) {
        throw new IllegalArgumentException("The number of disk mount paths != scratch paths");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties properties = ToolUtils.getVerifiableProperties(args);
    DiskReformatterConfig config = new DiskReformatterConfig(properties);
    StoreConfig storeConfig = new StoreConfig(properties);
    try (ClusterMap clusterMap = new StaticClusterAgentsFactory(new ClusterMapConfig(properties),
        config.hardwareLayoutFilePath, config.partitionLayoutFilePath).getClusterMap()) {
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      DataNodeId dataNodeId = clusterMap.getDataNodeId(config.datanodeHostname, config.datanodePort);
      if (dataNodeId == null) {
        throw new IllegalArgumentException(
            "Did not find node in clustermap with hostname:port - " + config.datanodeHostname + ":"
                + config.datanodePort);
      }
      DiskReformatter reformatter =
          new DiskReformatter(dataNodeId, config.fetchSizeInBytes, storeConfig, storeKeyFactory, clusterMap,
              SystemTime.getInstance());
      AtomicInteger exitStatus = new AtomicInteger(0);
      CountDownLatch latch = new CountDownLatch(config.diskMountPaths.length);
      for (int i = 0; i < config.diskMountPaths.length; i++) {
        int finalI = i;
        Runnable runnable = () -> {
          try {
            reformatter.reformat(config.diskMountPaths[finalI], new File(config.scratchPaths[finalI]));
          } catch (Exception e) {
            exitStatus.set(1);
            throw new IllegalStateException(e);
          } finally {
            latch.countDown();
          }
        };
        Thread thread = Utils.newThread(config.diskMountPaths[finalI] + "-reformatter", runnable, true);
        thread.setUncaughtExceptionHandler((t, e) -> {
          logger.error("Reformatting {} failed", config.diskMountPaths[finalI]);
          exitStatus.set(1);
        });
        thread.start();
      }
      latch.await();
      System.exit(exitStatus.get());
    }
  }

  /**
   * @param dataNodeId the {@link DataNodeId} on which {@code diskMountPath} exists.
   * @param fetchSizeInBytes the size of each fetch from the source store during copy
   * @param storeConfig the config for the stores
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @param clusterMap the {@link ClusterMap} to use get details of replicas and partitions.
   * @param time the {@link Time} instance to use.
   */
  public DiskReformatter(DataNodeId dataNodeId, long fetchSizeInBytes, StoreConfig storeConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, Time time) {
    this.dataNodeId = dataNodeId;
    this.fetchSizeInBytes = fetchSizeInBytes;
    this.storeConfig = storeConfig;
    this.storeKeyFactory = storeKeyFactory;
    this.clusterMap = clusterMap;
    this.time = time;
    consistencyChecker =
        new ConsistencyCheckerTool(clusterMap, new StoreToolsMetrics(clusterMap.getMetricRegistry()), time);
  }

  /**
   * Performs a reformat of the disk.
   * 1. Copies one partition on the disk to a scratch space
   * 2. Performs local copies of all other partitions on the disk using {@link StoreCopier} and deletes the source.
   * 3. Copies the partition in the scratch space back on to the disk
   * 4. Deletes the folder in the scratch space
   * @param diskMountPath the mount path of the disk to reformat
   * @param scratch the scratch space to use
   * @throws Exception
   */
  public void reformat(String diskMountPath, File scratch) throws Exception {
    if (!scratch.exists()) {
      throw new IllegalArgumentException("Scratch space " + scratch + " does not exist");
    }
    List<ReplicaId> replicasOnDisk = new ArrayList<>();
    // populate the replicas on disk
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.getDiskId().getMountPath().equals(diskMountPath)) {
        replicasOnDisk.add(replicaId);
      }
    }
    if (replicasOnDisk.size() == 0) {
      throw new IllegalArgumentException("There are no replicas on " + diskMountPath + " of " + dataNodeId);
    }
    replicasOnDisk.sort(Comparator.comparingLong(ReplicaId::getCapacityInBytes));
    logger.info("Found {} on {}", replicasOnDisk, diskMountPath);

    // move the last replica id (the largest one) to scratch space
    ReplicaId toMove = replicasOnDisk.get(replicasOnDisk.size() - 1);
    File scratchSrc = new File(toMove.getReplicaPath());
    File scratchTgt = new File(scratch, TEMP_RELOCATION_DIR_NAME);
    logger.info("Moving {} to {}", scratchSrc, scratchTgt);
    delete(scratchTgt);
    Files.move(scratchSrc.toPath(), scratchTgt.toPath());

    // reformat each store, except the one moved, one by one
    for (int i = 0; i < replicasOnDisk.size() - 1; i++) {
      ReplicaId replicaId = replicasOnDisk.get(i);
      File src = new File(replicaId.getReplicaPath());
      File tgt = new File(replicaId.getMountPath(), TEMP_COPY_DIR_NAME);
      logger.info("Copying {} to {}", src, tgt);
      copy(src, tgt, replicaId.getCapacityInBytes());
      delete(src);
      if (!tgt.renameTo(src)) {
        throw new IllegalStateException("Could not rename " + tgt + " to " + src);
      }
      logger.info("Done reformatting {}", replicaId);
    }

    // reformat the moved store
    copy(scratchTgt, scratchSrc, toMove.getCapacityInBytes());
    delete(scratchTgt);
    logger.info("Done reformatting {}", toMove);
  }

  /**
   * Copy the partition at {@code src} to {@code tgt} using a {@link StoreCopier}.
   * @param src the location of the partition to be copied
   * @param tgt the location where the partition has to be copied to
   * @param capacityInBytes the capacity of the partition.
   * @throws Exception
   */
  private void copy(File src, File tgt, long capacityInBytes) throws Exception {
    try (StoreCopier copier = new StoreCopier(src, tgt, capacityInBytes, fetchSizeInBytes, storeConfig,
        new MetricRegistry(), storeKeyFactory, diskIOScheduler, Collections.EMPTY_LIST, time)) {
      copier.copy(new StoreFindTokenFactory(storeKeyFactory).getNewFindToken());
    }
    // verify that the stores are equivalent
    File[] replicas = {src, tgt};
    if (!consistencyChecker.checkConsistency(replicas)) {
      throw new IllegalStateException("Data in " + src + " and " + tgt + " is not equivalent");
    }
  }

  /**
   * Deletes {@code location}
   * @param location the location to delete
   * @throws IOException if there are any problems deleting {@code location}.
   */
  private void delete(File location) throws IOException {
    if (location.exists() && !FileUtils.deleteQuietly(location)) {
      throw new IOException("Could not delete " + location);
    }
  }
}
