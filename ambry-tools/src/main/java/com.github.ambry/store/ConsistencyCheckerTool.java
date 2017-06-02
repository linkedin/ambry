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
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consistency Checker tool is used to check for consistency
 * 1) Among replicas for any given partition
 * 2) In the index file boundaries on all replicas in a partition
 */
public class ConsistencyCheckerTool {
  private final VerifiableProperties verifiableProperties;

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
  private final String partitionRootDirectory;

  // True if acceptable inconsistent blobs should be part of the output or not
  private final boolean includeAcceptableInconsistentBlobs;

  private final StoreToolsMetrics metrics;

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyCheckerTool.class);

  private ConsistencyCheckerTool(VerifiableProperties verifiableProperties, StoreToolsMetrics metrics)
      throws IOException, JSONException {
    partitionRootDirectory = verifiableProperties.getString("partition.root.directory");
    // Setting this to true implies user is interested in knowing information about acceptable inconsistent blobs.
    // Setting this to true will only print the blobId and their states in every replica(and not IndexValue in every
    // replica). If user is interested in IndexValues, might have to call DumpIndexTool separately for these blobs
    includeAcceptableInconsistentBlobs =
        verifiableProperties.getBoolean("include.acceptable.inconsistent.blobs", false);
    this.verifiableProperties = verifiableProperties;
    this.metrics = metrics;
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    MetricRegistry registry = new MetricRegistry();
    StoreToolsMetrics metrics = new StoreToolsMetrics(registry);
    JmxReporter reporter = null;
    try {
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();
      ConsistencyCheckerTool consistencyCheckerTool = new ConsistencyCheckerTool(verifiableProperties, metrics);
      boolean success = consistencyCheckerTool.checkConsistency();
      System.exit(success ? 0 : 1);
    } finally {
      if (reporter != null) {
        reporter.stop();
      }
    }
  }

  /**
   * Executes the operation with the help of properties passed during initialization of {@link DumpDataTool}
   * @return {@code true} if no real inconsistent blobs. {@code false}
   * @throws Exception
   */
  private boolean checkConsistency() throws Exception {
    File rootDir = new File(partitionRootDirectory);
    logger.info("Root directory for Partition" + rootDir);
    ArrayList<String> replicaList = populateReplicaList(rootDir);
    logger.trace("Replica List " + replicaList);
    Map<String, BlobStatus> blobIdToStatusMap = new HashMap<>();
    AtomicLong totalKeysProcessed = new AtomicLong(0);
    int replicaCount = replicaList.size();

    collectData(rootDir.listFiles(), replicaList, blobIdToStatusMap, totalKeysProcessed);
    List<String> realInconsistentBlobs = new ArrayList<>();
    populateOutput(totalKeysProcessed, blobIdToStatusMap, replicaCount, includeAcceptableInconsistentBlobs,
        realInconsistentBlobs);
    if (realInconsistentBlobs.size() > 0) {
      logger.info("\nDumping Inconsistent blobs ");
      dumpInconsistentBlobs(rootDir.listFiles(), realInconsistentBlobs);
    }
    return realInconsistentBlobs.size() == 0;
  }

  private ArrayList<String> populateReplicaList(File rootDir) {
    ArrayList<String> replicaList = new ArrayList<String>();
    File[] replicas = rootDir.listFiles();
    for (File replicaFile : replicas) {
      replicaList.add(replicaFile.getName());
    }
    return replicaList;
  }

  /**
   * Walks through all replicas and collects blob status in each of them.
   * @param replicas An Array of replica directories from which blob status' need to be collected
   * @param replicasList {@link List} of all
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param totalKeysProcessed {@link AtomicLong} to track the total keys processed
   * @throws Exception
   */
  private void collectData(File[] replicas, ArrayList<String> replicasList, Map<String, BlobStatus> blobIdToStatusMap,
      AtomicLong totalKeysProcessed) throws Exception {
    DumpIndexTool dumpIndexTool = new DumpIndexTool(verifiableProperties, metrics);
    Time time = SystemTime.getInstance();
    long currentTimeInMs = time.milliseconds();
    IndexStats indexStats = new IndexStats();
    for (File replica : replicas) {
      try {
        File[] indexFiles = replica.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
        long keysProcessedforReplica = 0;
        Arrays.sort(indexFiles, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
        logger.info("Processing index files for replica {} ", replica.getName());
        for (int i = 0; i < indexFiles.length; i++) {
          keysProcessedforReplica +=
              dumpIndexTool.dumpIndex(indexFiles[i], replica.getName(), replicasList, new ArrayList<String>(),
                  blobIdToStatusMap, indexStats, i >= (indexFiles.length - 2), time, currentTimeInMs);
        }
        logger.debug("Total keys processed for {} is {}", replica.getName(), keysProcessedforReplica);
        totalKeysProcessed.addAndGet(keysProcessedforReplica);
      } catch (Exception e) {
        logger.error("Could not complete processing for {}", replica, e);
      }
    }
    logger.debug("Total Keys Processed {}", totalKeysProcessed.get());
    logger.debug("Total Put Records {}", indexStats.getTotalPutRecords().get());
    logger.debug("Total Delete Records {}", indexStats.getTotalDeleteRecords().get());
    logger.debug("Total Duplicate Put Records {}", indexStats.getTotalDuplicatePutRecords().get());
    logger.debug("Total Delete before Put Records {}", indexStats.getTotalDeleteBeforePutRecords().get());
    logger.debug("Total Put after Delete Records {}", indexStats.getTotalPutAfterDeleteRecords().get());
    logger.debug("Total Duplicate Delete Records {}", indexStats.getTotalDuplicateDeleteRecords().get());
  }

  /**
   * Walks through blobs and its status in all replicas and collects inconsistent blobs information
   * @param totalKeysProcessed {@link AtomicLong} to track the total keys processed
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param replicaCount total replica count
   * @param includeAcceptableInconsistentBlobs {@code true} if acceptable inconsistent blobs needs to be considered. {@code false}
   *                                                       otherwise
   * @param realInconsistentBlobs {@link List} of real inconsistent blobIds
   */
  private void populateOutput(AtomicLong totalKeysProcessed, Map<String, BlobStatus> blobIdToStatusMap,
      int replicaCount, boolean includeAcceptableInconsistentBlobs, List<String> realInconsistentBlobs) {
    logger.info("Total keys processed {}", totalKeysProcessed.get());
    logger.info("\nTotal Blobs Found {}", blobIdToStatusMap.size());
    long earliestRealInconsistentBlobTimeMs = Long.MAX_VALUE;
    long latestOpTimeMs = Long.MIN_VALUE;
    long totalInconsistentBlobs = 0;
    AtomicLong inconsistentDueToReplicationCount = new AtomicLong(0);
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
          if (includeAcceptableInconsistentBlobs) {
            logger.error("Partially deleted (acceptable inconsistency) blob {}  isDeletedOrExpired {} \n {}", blobId,
                consistencyBlobResult.isDeletedOrExpired(), consistencyBlobResult);
          }
          acceptableInconsistentBlobs++;
        } else {
          if (consistencyBlobResult.belongsToRecentIndexSegment()) {
            logger.debug("Inconsistent blob found possibly due to replication {} Status map {} ", blobId,
                consistencyBlobResult);
            inconsistentDueToReplicationCount.incrementAndGet();
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
    logger.info("Inconsistent blobs count due to replication lag : {}", inconsistentDueToReplicationCount.get());
    logger.info("Real Inconsistent blobs count : {} ", realInconsistentBlobs.size());
    if (realInconsistentBlobs.size() > 0) {
      logger.info(
          "The earliest real inconsistent blob had its last operation at {} ms and diffenrence wrt latest operation time is {} ms",
          earliestRealInconsistentBlobTimeMs, latestOpTimeMs - earliestRealInconsistentBlobTimeMs);
    }
  }

  /**
   * Dumps inconsistent blobs from all replicas
   * @param replicas An Array of replica directories from which blobs need to be dumped
   * @param blobIdList {@link List} of blobIds to be dumped
   * @throws Exception
   */
  private void dumpInconsistentBlobs(File[] replicas, List<String> blobIdList) throws Exception {
    DumpIndexTool dumpIndexTool = new DumpIndexTool(verifiableProperties, metrics);
    for (File replica : replicas) {
      try {
        dumpIndexTool.dumpIndexesForReplica(replica.getAbsolutePath(), blobIdList, 1);
      } catch (Exception e) {
        logger.error("Could not complete processing for {}", replica, e);
      }
    }
  }
}

