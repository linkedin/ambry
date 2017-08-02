/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.JmxReporter;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consistency Checker tool is used to check for consistency
 * 1) Among replicas for any given partition
 * 2) In the index file boundaries on all replicas in a partition
 */
public class ConsistencyCheckerTool {
  /* Path referring to root directory containing one directory per replica
  Expected format of the partition root directory
  - Partition Root Directory
      - Replica_1
            - IndexSegment_0
            - IndexSegment_1
            .
            .
       - Replica_2
            - IndexSegment_0
            - IndexSegment_1
            .
            .
       .
       .
   User of this tool is expected to copy all index files for replicas of interest locally and run the tool with last
   modified times of the files unchanged. In linux use "cp -p" to maintain the file attributes.
   */

  private final DumpIndexTool dumpIndexTool;
  private final Time time;

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyCheckerTool.class);

  public static void main(String args[]) throws Exception {
    VerifiableProperties properties = ToolUtils.getVerifiableProperties(args);
    String hardwareLayoutFilePath = properties.getString("hardware.layout.file.path");
    String partitionLayoutFilePath = properties.getString("partition.layout.file.path");
    String partitionRootDirectory = properties.getString("partition.root.directory");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    JmxReporter reporter = null;
    try (ClusterMap clusterMap = new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutFilePath,
        partitionLayoutFilePath).getClusterMap()) {
      StoreToolsMetrics metrics = new StoreToolsMetrics(clusterMap.getMetricRegistry());
      reporter = JmxReporter.forRegistry(clusterMap.getMetricRegistry()).build();
      reporter.start();
      File rootDir = new File(partitionRootDirectory);
      ConsistencyCheckerTool consistencyCheckerTool =
          new ConsistencyCheckerTool(clusterMap, metrics, SystemTime.getInstance());
      boolean success = consistencyCheckerTool.checkConsistency(rootDir.listFiles());
      System.exit(success ? 0 : 1);
    } finally {
      if (reporter != null) {
        reporter.stop();
      }
    }
  }

  public ConsistencyCheckerTool(ClusterMap clusterMap, StoreToolsMetrics metrics, Time time) {
    this.time = time;
    dumpIndexTool = new DumpIndexTool(clusterMap, true, time, metrics);
  }

  /**
   * Executes the operation with the help of properties passed during initialization of {@link DumpDataTool}
   * @param replicas the replicas b/w which consistency has to be checked
   * @return {@code true} if no real inconsistent blobs. {@code false}
   * @throws Exception
   */
  public boolean checkConsistency(File[] replicas) throws Exception {
    List<String> replicaList = populateReplicaList(replicas);
    Map<String, BlobStatus> blobIdToStatusMap = new HashMap<>();
    collectData(replicas, blobIdToStatusMap);
    return populateOutput(blobIdToStatusMap, replicaList.size()).size() == 0;
  }

  /**
   * Walks through all replicas and collects blob status in each of them.
   * @param replicas An Array of replica directories from which blob status' need to be collected
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @throws Exception
   */
  private void collectData(File[] replicas, Map<String, BlobStatus> blobIdToStatusMap) throws Exception {
    long totalKeysProcessed = 0;
    IndexStats indexStats = new IndexStats();
    for (File replica : replicas) {
      File[] indexFiles = replica.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
      long keysProcessedforReplica = 0;
      Arrays.sort(indexFiles, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
      logger.info("Processing index files for replica {} ", replica.getName());
      for (int i = 0; i < indexFiles.length; i++) {
        keysProcessedforReplica +=
            dumpIndexTool.dumpIndex(indexFiles[i], replica.getName(), populateReplicaList(replicas),
                Collections.EMPTY_LIST, blobIdToStatusMap, indexStats, i >= (indexFiles.length - 2),
                time.milliseconds());
      }
      logger.debug("Total keys processed for {} is {}", replica.getName(), keysProcessedforReplica);
      totalKeysProcessed += keysProcessedforReplica;
    }
    logger.debug("Total Keys Processed {}", totalKeysProcessed);
    logger.debug("Total Put Records {}", indexStats.getTotalPutRecords().get());
    logger.debug("Total Delete Records {}", indexStats.getTotalDeleteRecords().get());
    logger.debug("Total Duplicate Put Records {}", indexStats.getTotalDuplicatePutRecords().get());
    logger.debug("Total Delete before Put Records {}", indexStats.getTotalDeleteBeforePutRecords().get());
    logger.debug("Total Put after Delete Records {}", indexStats.getTotalPutAfterDeleteRecords().get());
    logger.debug("Total Duplicate Delete Records {}", indexStats.getTotalDuplicateDeleteRecords().get());
  }

  /**
   * Walks through blobs and its status in all replicas and collects inconsistent blobs information
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param replicaCount total replica count
   * @return {@link List} of real inconsistent blobIds
   */
  private List<String> populateOutput(Map<String, BlobStatus> blobIdToStatusMap, int replicaCount) {
    List<String> realInconsistentBlobs = new ArrayList<>();
    logger.info("Total Blobs Found {}", blobIdToStatusMap.size());
    long earliestRealInconsistentBlobTimeMs = Long.MAX_VALUE;
    long latestOpTimeMs = Long.MIN_VALUE;
    long totalInconsistentBlobs = 0;
    long inconsistentDueToReplicationCount = 0;
    long acceptableInconsistentBlobs = 0;
    for (String blobId : blobIdToStatusMap.keySet()) {
      BlobStatus consistencyBlobResult = blobIdToStatusMap.get(blobId);
      latestOpTimeMs = Math.max(latestOpTimeMs, consistencyBlobResult.getOpTime());
      // valid blobs : count of available replicas = total replica count or count of deleted replicas = total replica count
      boolean isValid = consistencyBlobResult.getAvailableReplicaSet().size() == replicaCount
          || consistencyBlobResult.getDeletedOrExpiredReplicaSet().size() == replicaCount;
      if (!isValid) {
        totalInconsistentBlobs++;
        if ((consistencyBlobResult.getDeletedOrExpiredReplicaSet().size()
            + consistencyBlobResult.getUnavailableReplicaSet().size() == replicaCount)) {
          // acceptable inconsistent blobs : count of deleted + count of unavailable = total replica count
          logger.debug("Partially deleted (acceptable inconsistency) blob {} isDeletedOrExpired {}. Blob status - {}",
              blobId, consistencyBlobResult.isDeletedOrExpired(), consistencyBlobResult);
          acceptableInconsistentBlobs++;
        } else {
          if (consistencyBlobResult.belongsToRecentIndexSegment()) {
            logger.debug("Inconsistent blob found possibly due to replication {} Status map {} ", blobId,
                consistencyBlobResult);
            inconsistentDueToReplicationCount++;
          } else {
            logger.error("Inconsistent blob found {} Status map {}", blobId, consistencyBlobResult);
            realInconsistentBlobs.add(blobId);
            earliestRealInconsistentBlobTimeMs =
                Math.min(earliestRealInconsistentBlobTimeMs, consistencyBlobResult.getOpTime());
          }
        }
      }
    }
    logger.info("The latest op executed at {} ms", latestOpTimeMs);
    // Inconsistent blobs = real inconsistent + acceptable inconsistent + inconsistent due to replication Lag
    // Acceptable inconsistent = due to deletion, some replicas reports as deleted, whereas some reports as unavailable
    // Inconsistent due to replication lag = Inconsistency due to replication lag.
    // Anything else is considered to be real inconsistent blobs
    logger.info("Total Inconsistent blobs count : {}", totalInconsistentBlobs);
    logger.info("Acceptable Inconsistent blobs count : {}", acceptableInconsistentBlobs);
    logger.info("Inconsistent blobs count due to replication lag : {}", inconsistentDueToReplicationCount);
    logger.info("Real Inconsistent blobs count : {} ", realInconsistentBlobs.size());
    if (realInconsistentBlobs.size() > 0) {
      logger.info(
          "The earliest real inconsistent blob had its last operation at {} ms and difference wrt latest operation time is {} ms",
          earliestRealInconsistentBlobTimeMs, latestOpTimeMs - earliestRealInconsistentBlobTimeMs);
    }
    return realInconsistentBlobs;
  }

  private List<String> populateReplicaList(File[] replicas) {
    List<String> replicaList = new ArrayList<String>();
    for (File replicaFile : replicas) {
      replicaList.add(replicaFile.getName());
    }
    return replicaList;
  }
}

