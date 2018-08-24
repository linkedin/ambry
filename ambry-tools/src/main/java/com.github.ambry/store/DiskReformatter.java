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
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reformats all the stores on a given disk.
 *
 * Guide to using this tool:
 * 1. Make sure that the Ambry server is completely shut down before using this tool (i.e there should be nothing else
 * using those directories).
 * 2. Make sure that the scratch space provided has enough space for the largest partition on the disk. If the same
 * scratch space is used for multiple disks, ensure that it has enough space for all.
 *
 * On failures:
 * The tool does not lose any data but may leave things in an inconsistent state after failures.
 * - If the tool fails before a partition is moved to scratch space, then there is nothing to do
 * - If the tool fails after a partition is moved to scratch space but before a copy is started, move the relocated
 * partition back to the disk to go to a consistent state
 * - If the tool fails during a copy, delete the target directory (usually *_under_reformat except for the last
 * partition that is copied back from the scratch space at the end) and move the relocated partition back to
 * the disk (if required) to go to a consistent state
 * - If the tool fails after the source directory of the copy is deleted but before the target is renamed (this can
 * be detected easily by looking at the directories), rename the target, move the relocated partition back to disk (if
 * required) to go to a consistent state (this will not happen for the very last partition which is copied back from
 * the scratch space because there is no rename step).
 */
public class DiskReformatter {
  private static final String RELOCATED_DIR_NAME_SUFFIX = "_relocated";
  private static final String UNDER_REFORMAT_DIR_NAME_SUFFIX = "_under_reformat";
  private static final String RELOCATION_IN_PROGRESS_SUFFIX = "_relocation_in_progress";
  private static final Logger logger = LoggerFactory.getLogger(DiskReformatter.class);

  private final DataNodeId dataNodeId;
  private final List<Transformer> transformers;
  private final long fetchSizeInBytes;
  private final StoreConfig storeConfig;
  private final StoreKeyFactory storeKeyFactory;
  private final ClusterMap clusterMap;
  private final Time time;
  private final ConsistencyCheckerTool consistencyChecker;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);

  /**
   * Config for the reformatter.
   */
  private static class DiskReformatterConfig {
    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
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
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
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
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    ServerConfig serverConfig = new ServerConfig(properties);
    ClusterAgentsFactory clusterAgentsFactory =
        Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig, config.hardwareLayoutFilePath,
            config.partitionLayoutFilePath);
    try (ClusterMap clusterMap = clusterAgentsFactory.getClusterMap()) {
      StoreKeyConverterFactory storeKeyConverterFactory = Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, clusterMap.getMetricRegistry());
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      DataNodeId dataNodeId = clusterMap.getDataNodeId(config.datanodeHostname, config.datanodePort);
      if (dataNodeId == null) {
        throw new IllegalArgumentException(
            "Did not find node in clustermap with hostname:port - " + config.datanodeHostname + ":"
                + config.datanodePort);
      }
      DiskReformatter reformatter =
          new DiskReformatter(dataNodeId, Collections.EMPTY_LIST, config.fetchSizeInBytes, storeConfig, storeKeyFactory,
              clusterMap, SystemTime.getInstance(), storeKeyConverterFactory.getStoreKeyConverter());
      AtomicInteger exitStatus = new AtomicInteger(0);
      CountDownLatch latch = new CountDownLatch(config.diskMountPaths.length);
      for (int i = 0; i < config.diskMountPaths.length; i++) {
        int finalI = i;
        Runnable runnable = () -> {
          try {
            reformatter.reformat(config.diskMountPaths[finalI], new File(config.scratchPaths[finalI]));
            latch.countDown();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        };
        Thread thread = Utils.newThread(config.diskMountPaths[finalI] + "-reformatter", runnable, true);
        thread.setUncaughtExceptionHandler((t, e) -> {
          exitStatus.set(1);
          logger.error("Reformatting {} failed", config.diskMountPaths[finalI], e);
          latch.countDown();
        });
        thread.start();
      }
      latch.await();
      System.exit(exitStatus.get());
    }
  }

  /**
   * @param dataNodeId the {@link DataNodeId} on which {@code diskMountPath} exists.
   * @param transformers the list of the {@link Transformer} to use (in order).
   * @param fetchSizeInBytes the size of each fetch from the source store during copy
   * @param storeConfig the config for the stores
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @param clusterMap the {@link ClusterMap} to use get details of replicas and partitions.
   * @param time the {@link Time} instance to use.
   */
  public DiskReformatter(DataNodeId dataNodeId, List<Transformer> transformers, long fetchSizeInBytes,
      StoreConfig storeConfig, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, Time time,
      StoreKeyConverter storeKeyConverter) {
    this.dataNodeId = dataNodeId;
    this.transformers = transformers;
    this.fetchSizeInBytes = fetchSizeInBytes;
    this.storeConfig = storeConfig;
    this.storeKeyFactory = storeKeyFactory;
    this.clusterMap = clusterMap;
    this.time = time;
    diskSpaceAllocator =
        new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(clusterMap.getMetricRegistry()));
    consistencyChecker = new ConsistencyCheckerTool(clusterMap, storeKeyFactory, storeConfig, null, null,
        new StoreToolsMetrics(clusterMap.getMetricRegistry()), time, storeKeyConverter);
  }

  /**
   * Uses {@link StoreCopier} to convert all the partitions on the given disk (D).
   * 1. Copies one partition on D to a scratch space
   * 2. Using {@link StoreCopier}, performs copies of all other partitions on D using D as a staging area. When a
   * partition is completely copied and verified, the original is replaced by the copy.
   * 3. Copies the partition in the scratch space back onto D.
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
    String partIdString = toMove.getPartitionId().toString();
    File scratchSrc = new File(toMove.getReplicaPath());
    File scratchTmp = new File(scratch, partIdString + RELOCATION_IN_PROGRESS_SUFFIX);
    File scratchTgt = new File(scratch, partIdString + RELOCATED_DIR_NAME_SUFFIX);
    if (scratchTmp.exists()) {
      throw new IllegalStateException(scratchTmp + " already exists");
    }
    if (scratchTgt.exists()) {
      throw new IllegalStateException(scratchTgt + " already exists");
    }
    ensureNotInUse(scratchSrc, toMove.getCapacityInBytes());
    logger.info("Moving {} to {}", scratchSrc, scratchTgt);
    FileUtils.moveDirectory(scratchSrc, scratchTmp);
    if (!scratchTmp.renameTo(scratchTgt)) {
      throw new IllegalStateException("Could not rename " + scratchTmp + " to " + scratchTgt);
    }

    // reformat each store, except the one moved, one by one
    for (int i = 0; i < replicasOnDisk.size() - 1; i++) {
      ReplicaId replicaId = replicasOnDisk.get(i);
      partIdString = replicaId.getPartitionId().toString();
      File src = new File(replicaId.getReplicaPath());
      File tgt = new File(replicaId.getMountPath(), partIdString + UNDER_REFORMAT_DIR_NAME_SUFFIX);
      logger.info("Copying {} to {}", src, tgt);
      copy(partIdString, src, tgt, replicaId.getCapacityInBytes());
      logger.info("Deleting {}", src);
      Utils.deleteFileOrDirectory(src);
      logger.info("Renaming {} to {}", tgt, src);
      if (!tgt.renameTo(src)) {
        throw new IllegalStateException("Could not rename " + tgt + " to " + src);
      }
      logger.info("Done reformatting {}", replicaId);
    }

    // reformat the moved store
    logger.info("Copying {} to {}", scratchTgt, scratchSrc);
    copy(toMove.getPartitionId().toString(), scratchTgt, scratchSrc, toMove.getCapacityInBytes());
    logger.info("Deleting {}", scratchTgt);
    Utils.deleteFileOrDirectory(scratchTgt);
    logger.info("Done reformatting {}", toMove);
    logger.info("Done reformatting disk {}", diskMountPath);
  }

  /**
   * Ensures that the directory provided is not in use by starting and stopping a {@link BlobStore} at the given
   * directory.
   * @param srcDir the directory to use
   * @param storeCapacity the capacity of the store
   * @throws StoreException if there are any problems starting or stopping the store.
   */
  private void ensureNotInUse(File srcDir, long storeCapacity) throws StoreException {
    MessageStoreRecovery recovery = new BlobStoreRecovery();
    StoreMetrics metrics = new StoreMetrics(new MetricRegistry());
    Store store = new BlobStore("move_check_" + UUID.randomUUID().toString(), storeConfig, null, null, diskIOScheduler,
        diskSpaceAllocator, metrics, metrics, srcDir.getAbsolutePath(), storeCapacity, storeKeyFactory, recovery, null,
        time);
    store.start();
    store.shutdown();
  }

  /**
   * Copy the partition at {@code src} to {@code tgt} using a {@link StoreCopier}.
   * @param storeId the name/id of the {@link Store}.
   * @param src the location of the partition to be copied
   * @param tgt the location where the partition has to be copied to
   * @param capacityInBytes the capacity of the partition.
   * @throws Exception
   */
  private void copy(String storeId, File src, File tgt, long capacityInBytes) throws Exception {
    boolean sourceHasProblems;
    // NOTE: Ideally, we would have liked one MetricRegistry instance to record metrics across all these stores
    // However, due to the fact that PersistentIndex and HardDeleter initialize gauges inside StoreMetrics, the
    // MetricRegistry retains references to the PersistentIndex causing a memory leak. If the same instance is to be
    // used, these gauges have to be removed from the registry on store shutdown. This is being deferred to future work.
    StoreMetrics metrics = new StoreMetrics(new MetricRegistry());
    try (
        StoreCopier copier = new StoreCopier(storeId, src, tgt, capacityInBytes, fetchSizeInBytes, storeConfig, metrics,
            storeKeyFactory, diskIOScheduler, diskSpaceAllocator, transformers, time)) {
      sourceHasProblems = copier.copy(new StoreFindTokenFactory(storeKeyFactory).getNewFindToken()).getSecond();
    }
    if (!sourceHasProblems) {
      // verify that the stores are equivalent
      File[] replicas = {src, tgt};
      Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> consistencyCheckResult =
          consistencyChecker.checkConsistency(replicas);
      if (!consistencyCheckResult.getFirst()) {
        // there was a problem with the consistency check
        if (consistencyCheckResult.getSecond().get(src).isIndexSane()) {
          // the src index was sane which means either the tgt was not or that they are not consistent. Problem !
          throw new IllegalStateException("Data in " + src + " and " + tgt + " is not equivalent");
        } else {
          // the src itself was bad - nothing we can do. move on.
          logger.warn("{} was not sane. Consistency check was skipped", src);
        }
      }
    } else {
      logger.warn("{} had problems so did not run consistency checker", src);
    }
  }
}
