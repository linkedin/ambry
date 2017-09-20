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
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping indices in Ambry
 * Supported operations are
 * 1. Dump Index
 * 2. Dump Index for a replica
 * 3. Dump active blobs for an index
 * 4. Dump active blobs for a replica
 * 5. Dump N random blobs for a replica
 */
public class DumpIndexTool {
  private final ClusterMap clusterMap;
  // set to true if only error logging is required
  private final boolean silent;
  private final StoreToolsMetrics metrics;
  private final Time time;

  private static final Logger logger = LoggerFactory.getLogger(DumpIndexTool.class);

  public DumpIndexTool(ClusterMap clusterMap, boolean silent, Time time, StoreToolsMetrics metrics) {
    this.clusterMap = clusterMap;
    this.silent = silent;
    this.time = time;
    this.metrics = metrics;
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    String hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    String partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    JmxReporter reporter = null;
    try (ClusterMap clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory,
        clusterMapConfig, hardwareLayoutFilePath, partitionLayoutFilePath)).getClusterMap()) {
      String typeOfOperation = verifiableProperties.getString("type.of.operation", "");
      String blobIdsStr = verifiableProperties.getString("blob.id.list", "");
      List<String> blobIds = blobIdsStr.isEmpty() ? Collections.EMPTY_LIST : Arrays.asList(blobIdsStr.split(","));
      boolean activeBlobsOnly = verifiableProperties.getBoolean("active.blobs.only", false);
      boolean silent = verifiableProperties.getBoolean("silent", true);
      logger.info("Type of Operation - {}", typeOfOperation);
      if (blobIds.size() > 0) {
        logger.info("Blobs to look out for - {}", blobIds);
      }

      Time time = SystemTime.getInstance();
      StoreToolsMetrics metrics = new StoreToolsMetrics(clusterMap.getMetricRegistry());
      reporter = JmxReporter.forRegistry(clusterMap.getMetricRegistry()).build();
      reporter.start();
      DumpIndexTool dumpIndexTool = new DumpIndexTool(clusterMap, silent, time, metrics);
      switch (typeOfOperation) {
        case "DumpIndexFile":
          String fileToRead = verifiableProperties.getString("file.to.read");
          logger.info("File to read " + fileToRead);
          if (activeBlobsOnly) {
            dumpIndexTool.dumpActiveBlobsFromIndex(new File(fileToRead), blobIds, time.milliseconds());
          } else {
            dumpIndexTool.dumpIndex(new File(fileToRead), null, null, blobIds, null, new IndexStats(), false,
                time.milliseconds());
          }
          break;
        case "DumpIndexesForReplica":
          String replicaRootDirecotry = verifiableProperties.getString("replica.root.directory");
          long indexEntriesPerSec = verifiableProperties.getLong("index.entries.per.sec", 1000);
          if (activeBlobsOnly) {
            dumpIndexTool.dumpActiveBlobsForReplica(replicaRootDirecotry, blobIds, time.milliseconds());
          } else {
            dumpIndexTool.dumpIndexesForReplica(replicaRootDirecotry, blobIds, indexEntriesPerSec, time.milliseconds());
          }
          break;
        case "DumpNRandomActiveBlobsForReplica":
          replicaRootDirecotry = verifiableProperties.getString("replica.root.directory");
          long activeBlobsCount = verifiableProperties.getLong("active.blobs.count");
          dumpIndexTool.dumpNRandomActiveBlobsForReplica(replicaRootDirecotry, blobIds, activeBlobsCount,
              time.milliseconds());
          break;
        default:
          logger.error("Unknown typeOfOperation " + typeOfOperation);
          break;
      }
    } finally {
      if (reporter != null) {
        reporter.stop();
      }
    }
  }

  /**
   * Dumps all records in an index file and updates the {@link Map} for the blob status
   * @param indexFileToDump the index file that needs to be parsed for
   * @param replica the replica from which the index files are being parsed for
   * @param replicaList total list of all replicas for the partition which this replica is part of
   * @param blobList List of blobIds to be filtered for. Can be {@code null}
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param indexStats the {@link IndexStats} to be updated with some stats info
   * @param recentIndexSegment {@code true} if index file is referring to recent (last or last but one) index segment.
   *                           {@code false} otherwise
   * @param currentTimeInMs the current time to use (in ms)
   * @return the total number of records processed
   * @throws Exception
   */
  long dumpIndex(File indexFileToDump, String replica, List<String> replicaList, List<String> blobList,
      Map<String, BlobStatus> blobIdToStatusMap, IndexStats indexStats, boolean recentIndexSegment,
      long currentTimeInMs) throws Exception {
    final Timer.Context context = metrics.dumpIndexTimeMs.time();
    try {
      Map<String, IndexValue> blobIdToMessageMapPerIndexFile = new HashMap<>();
      logger.trace("Dumping index {} for {}", indexFileToDump.getName(), replica);
      AtomicLong lastModifiedTimeMs = new AtomicLong(0);
      long blobsProcessed =
          dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile, lastModifiedTimeMs, time);

      for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
        if (blobList == null || blobList.size() == 0 || blobList.contains(key)) {
          logger.trace(
              "Processing entry : key " + key + ", value " + blobIdToMessageMapPerIndexFile.get(key).toString());
          IndexValue indexValue = blobIdToMessageMapPerIndexFile.get(key);
          if (blobIdToStatusMap == null) {
            if (!silent) {
              logger.info(key + " : " + indexValue.toString());
            }
            if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs)) {
              indexStats.incrementTotalDeleteRecords();
            } else {
              indexStats.incrementTotalPutRecords();
            }
          } else {
            if (!blobIdToStatusMap.containsKey(key)) {
              blobIdToStatusMap.put(key, new BlobStatus(replicaList));
              if (isDeleted(indexValue)) {
                logger.trace("Delete record found before Put record for {} in replica {}", key, replica);
                indexStats.incrementTotalDeleteBeforePutRecords();
              }
            }
            BlobStatus mapValue = blobIdToStatusMap.get(key);
            mapValue.setBelongsToRecentIndexSegment(recentIndexSegment);
            if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs)) {
              if (mapValue.getAvailableReplicaSet().contains(replica)) {
                indexStats.incrementTotalDeleteRecords();
              } else if (mapValue.getDeletedOrExpiredReplicaSet().contains(replica)) {
                indexStats.incrementTotalDuplicateDeleteRecords();
              }
              mapValue.addDeletedOrExpired(replica, lastModifiedTimeMs.get());
            } else {
              if (mapValue.getDeletedOrExpiredReplicaSet().contains(replica)) {
                logger.error("Put Record found after delete record for {} in replica ", key, replica);
                indexStats.incrementTotalPutAfterDeleteRecords();
              }
              if (mapValue.getAvailableReplicaSet().contains(replica)) {
                logger.error("Duplicate Put record found for {} in replica ", key, replica);
                indexStats.incrementTotalDuplicatePutRecords();
              }
              mapValue.addAvailable(replica, lastModifiedTimeMs.get());
            }
          }
        }
      }
      if (!silent) {
        logger.info(
            "Total Put Records for index file " + indexFileToDump + " " + indexStats.getTotalPutRecords().get());
        logger.info(
            "Total Delete Records for index file " + indexFileToDump + " " + indexStats.getTotalDeleteRecords().get());
        logger.info("Total Duplicate Put Records for index file " + indexFileToDump + " "
            + indexStats.getTotalDuplicatePutRecords().get());
        logger.info("Total Delete before Put Records for index file " + indexFileToDump + " "
            + indexStats.getTotalDeleteBeforePutRecords().get());
        logger.info("Total Put after Delete Records for index file " + indexFileToDump + " "
            + indexStats.getTotalPutAfterDeleteRecords().get());
      }
      return blobsProcessed;
    } finally {
      context.stop();
    }
  }

  /**
   * Dumps all index files for a given Replica
   * @param replicaRootDirectory the root directory for a replica
   * @param blobList list of blobIds to be filtered for. Can be {@code null}
   * @param indexEntriesPerSec throttling value in index entries per sec
   * @param currentTimeInMs the current time to use (in ms)
   * @return a {@link Map} of BlobId to {@link BlobStatus} containing the information about every blob in
   * this replica
   * @throws Exception
   */
  public Map<String, BlobStatus> dumpIndexesForReplica(String replicaRootDirectory, List<String> blobList,
      long indexEntriesPerSec, long currentTimeInMs) throws Exception {
    final Timer.Context context = metrics.dumpReplicaIndexesTimeMs.time();
    try {
      long totalKeysProcessed = 0;
      File replicaDirectory = new File(replicaRootDirectory);
      logger.info("Root directory for replica : " + replicaRootDirectory);
      IndexStats indexStats = new IndexStats();
      Throttler throttler = new Throttler(indexEntriesPerSec, 1000, true, SystemTime.getInstance());
      Map<String, BlobStatus> blobIdToStatusMap = new HashMap<>();
      File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
      Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
      int totalIndexCount = replicas.length;
      int currentIndexCount = 0;
      for (File indexFile : replicas) {
        logger.info("Dumping index " + indexFile + " for replica " + replicaDirectory.getName());
        long keysProcessedPerIndexSegment =
            dumpIndex(indexFile, replicaDirectory.getName(), null, blobList, blobIdToStatusMap, indexStats,
                currentIndexCount++ >= (totalIndexCount - 2), currentTimeInMs);
        throttler.maybeThrottle(keysProcessedPerIndexSegment);
        totalKeysProcessed += keysProcessedPerIndexSegment;
      }
      long totalActiveRecords = 0;
      for (String key : blobIdToStatusMap.keySet()) {
        BlobStatus blobStatus = blobIdToStatusMap.get(key);
        if (!silent) {
          logger.info(key + " : " + blobStatus.toString());
        }
        if (!blobStatus.isDeletedOrExpired()) {
          totalActiveRecords++;
        }
      }
      if (!silent) {
        logger.info("Total Keys processed for replica " + replicaDirectory.getName() + " : " + totalKeysProcessed);
        logger.info("Total Put Records " + indexStats.getTotalPutRecords().get());
        logger.info("Total Delete Records " + indexStats.getTotalDeleteRecords().get());
        logger.info("Total Active Records " + totalActiveRecords);
        logger.info("Total Duplicate Put Records " + indexStats.getTotalDuplicatePutRecords().get());
        logger.info("Total Delete before Put Records " + indexStats.getTotalDeleteBeforePutRecords().get());
        logger.info("Total Put after Delete Records " + indexStats.getTotalPutAfterDeleteRecords().get());
        logger.info("Total Duplicate Delete Records " + indexStats.getTotalDuplicateDeleteRecords().get());
      }
      return blobIdToStatusMap;
    } finally {
      context.stop();
    }
  }

  /**
   * Dumps active blobs for a given index file
   * @param indexFileToDump the index file that needs to be parsed for
   * @param blobList List of BlobIds that needs to be filtered for. Can be {@code null}
   * @param blobIdToBlobMessageMap a {@link Map} of BlobId to {@link IndexValue} that needs to be updated with the information
   *                               about the blobs in the index
   * @param activeBlobStats {@link ActiveBlobStats} to be updated with necessary stats
   * @param currentTimeInMs the current time to use (in ms)
   * @return the total number of blobs parsed from the given index file
   * @throws Exception
   */
  private long dumpActiveBlobsFromIndex(File indexFileToDump, List<String> blobList,
      Map<String, IndexValue> blobIdToBlobMessageMap, ActiveBlobStats activeBlobStats, long currentTimeInMs)
      throws Exception {
    Map<String, IndexValue> blobIdToMessageMapPerIndexFile = new HashMap<>();
    long blobsProcessed =
        dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile, new AtomicLong(0), time);
    for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
      IndexValue indexValue = blobIdToMessageMapPerIndexFile.get(key);
      if (blobIdToBlobMessageMap.containsKey(key)) {
        if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs)) {
          blobIdToBlobMessageMap.remove(key);
          activeBlobStats.incrementTotalDeleteRecords();
        } else {
          logger.error("Found duplicate put record for " + key);
          activeBlobStats.incrementTotalDuplicatePutRecords();
        }
      } else {
        if (!(isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs))) {
          blobIdToBlobMessageMap.put(key, indexValue);
          activeBlobStats.incrementTotalPutRecords();
        } else {
          if (isDeleted(indexValue)) {
            logger.trace("Either duplicate delete record or delete record w/o a put record found for {} ", key);
            activeBlobStats.incrementTotalDeleteBeforePutOrDuplicateDeleteRecords();
          } else if (DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs)) {
            activeBlobStats.incrementTotalPutRecords();
          }
        }
      }
    }
    logger.info("Total Keys processed for index file " + indexFileToDump + " : " + blobsProcessed);
    logActiveBlobsStats(activeBlobStats);
    return blobsProcessed;
  }

  /**
   * Dumps active blobs for a given index file
   * @param indexFileToDump the index file that needs to be parsed for
   * @param blobList list of BlobIds that needs to be filtered for. Can be {@code null}
   * @param currentTimeInMs the current time to use (in ms)
   * @throws Exception
   */
  private void dumpActiveBlobsFromIndex(File indexFileToDump, List<String> blobList, long currentTimeInMs)
      throws Exception {
    Map<String, IndexValue> blobIdToBlobMessageMap = new HashMap<>();
    logger.trace("Dumping index {} ", indexFileToDump);
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    long totalKeysProcessed =
        dumpActiveBlobsFromIndex(indexFileToDump, blobList, blobIdToBlobMessageMap, activeBlobStats, currentTimeInMs);
    for (String blobId : blobIdToBlobMessageMap.keySet()) {
      logger.info(blobId + " : " + blobIdToBlobMessageMap.get(blobId));
    }
    logger.trace("Total Keys processed for index file {} : {}", indexFileToDump, totalKeysProcessed);
    logger.trace("Total Put Records for index file {} : {} ", indexFileToDump,
        activeBlobStats.getTotalPutRecords().get());
    logger.trace("Total Delete Records for index file {} : {} ", indexFileToDump,
        activeBlobStats.getTotalDeleteRecords().get());
    logger.trace("Total Active Records for index file {} : {}", indexFileToDump, blobIdToBlobMessageMap.size());
    logger.trace("Total Duplicate Put Records for index file {} : {} ", indexFileToDump,
        activeBlobStats.getTotalDuplicatePutRecords().get());
    logger.trace("Total Delete before Put Or duplicate Delete Records for index file {} : {} ", indexFileToDump,
        activeBlobStats.getTotalDeleteBeforePutOrDuplicateDeleteRecords().get());
  }

  /**
   * Dumps active blobs for all index files for a given replica
   * @param replicaRootDirectory Root directory of the replica
   * @param blobList List of BlobIds that needs to be filtered for. Can be {@code null}
   * @param currentTimeInMs the current time to use (in ms)
   * @throws Exception
   */
  private void dumpActiveBlobsForReplica(String replicaRootDirectory, List<String> blobList, long currentTimeInMs)
      throws Exception {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    Map<String, IndexValue> blobIdToMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File indexFile : replicas) {
      logger.info("Dumping index " + indexFile.getName() + " for " + replicaDirectory.getName());
      totalKeysProcessed +=
          dumpActiveBlobsFromIndex(indexFile, blobList, blobIdToMessageMap, activeBlobStats, currentTimeInMs);
    }

    for (String blobId : blobIdToMessageMap.keySet()) {
      logger.info(blobId + " : " + blobIdToMessageMap.get(blobId));
    }
    logger.trace("Total Keys processed for replica {} : {} ", replicaDirectory.getName(), totalKeysProcessed);
    logActiveBlobsStats(activeBlobStats);
  }

  /**
   * Dumps stats about active blobs from {@link ActiveBlobStats}
   * @param activeBlobStats the {@link ActiveBlobStats} from which stats needs to be dumped
   */
  private void logActiveBlobsStats(ActiveBlobStats activeBlobStats) {
    logger.trace("Total Put Records " + activeBlobStats.getTotalPutRecords().get());
    logger.trace("Total Delete Records " + activeBlobStats.getTotalDeleteRecords().get());
    logger.trace("Total Duplicate Put Records " + activeBlobStats.getTotalDuplicatePutRecords().get());
    logger.trace("Total Delete before Put or duplicate Delete Records "
        + activeBlobStats.getTotalDeleteBeforePutOrDuplicateDeleteRecords().get());
  }

  /**
   * Dumps N random active blobs for a given replica
   * @param replicaRootDirectory Root directory of the replica
   * @param blobList List of BlobIds that needs to be filtered for. Can be {@code null}
   * @param randomBlobsCount total number of random blobs that needs to be fetched from the replica
   * @param currentTimeInMs the current time to use (in ms)
   * @throws Exception
   */
  private void dumpNRandomActiveBlobsForReplica(String replicaRootDirectory, List<String> blobList,
      long randomBlobsCount, long currentTimeInMs) throws Exception {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    Map<String, IndexValue> blobIdToBlobMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File indexFile : replicas) {
      logger.trace("Dumping index {} for {} ", indexFile.getName(), replicaDirectory.getName());
      totalKeysProcessed +=
          dumpActiveBlobsFromIndex(indexFile, blobList, blobIdToBlobMessageMap, activeBlobStats, currentTimeInMs);
    }
    logger.trace("Total Keys processed for replica {} : {} ", replicaDirectory.getName(), totalKeysProcessed);
    logger.trace("Total Put Records {} ", activeBlobStats.getTotalPutRecords().get());
    logger.trace("Total Delete Records {} ", activeBlobStats.getTotalDeleteRecords().get());
    logger.trace("Total Duplicate Put Records {} ", activeBlobStats.getTotalDuplicatePutRecords().get());
    logger.trace("Total Delete before Put or duplicate Delete Records {} ",
        activeBlobStats.getTotalDeleteBeforePutOrDuplicateDeleteRecords().get());
    long totalBlobsToBeDumped =
        (randomBlobsCount > blobIdToBlobMessageMap.size()) ? blobIdToBlobMessageMap.size() : randomBlobsCount;
    logger.trace("Total blobs to be dumped {} ", totalBlobsToBeDumped);
    List<String> keys = new ArrayList<>(blobIdToBlobMessageMap.keySet());
    int randomCount = 0;
    while (randomCount < totalBlobsToBeDumped) {
      Collections.shuffle(keys);
      logger.info(blobIdToBlobMessageMap.get(keys.remove(0)).toString());
      randomCount++;
    }
    logger.info("Total blobs dumped " + totalBlobsToBeDumped);
  }

  /**
   * Dumps all blobs in an index file
   * @param indexFileToDump the index file that needs to be parsed
   * @param blobList List of blobIds to be filtered for
   * @param blobIdToMessageMap {@link Map} of BlobId to {@link IndexValue} to hold the information
   *                                          about blobs in the index after parsing
   * @param lastModifiedTimeMs {@link AtomicLong} referring to last modified time in ms
   * @return the total number of keys/records processed
   * @throws Exception
   */
  private long dumpBlobsFromIndex(File indexFileToDump, List<String> blobList,
      Map<String, IndexValue> blobIdToMessageMap, AtomicLong lastModifiedTimeMs, Time time) throws Exception {
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics storeMetrics =
        new StoreMetrics(indexFileToDump.getParent(), metricRegistry, new AggregatedStoreMetrics(metricRegistry));
    IndexSegment segment = new IndexSegment(indexFileToDump, false, storeKeyFactory, config, storeMetrics,
        new Journal(indexFileToDump.getParent(), 0, 0), time);
    lastModifiedTimeMs.set(segment.getLastModifiedTimeMs());
    List<MessageInfo> entries = new ArrayList<>();
    final Timer.Context context = metrics.findAllEntriesPerIndexTimeMs.time();
    try {
      segment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
    } finally {
      context.stop();
    }
    long numberOfKeysProcessed = 0;
    for (MessageInfo entry : entries) {
      StoreKey key = entry.getStoreKey();
      IndexValue value = segment.find(key);
      numberOfKeysProcessed++;
      if (blobList == null || blobList.size() == 0 || blobList.contains(key.toString())) {
        if (blobIdToMessageMap.containsKey(key.getID())) {
          logger.error(
              "Duplicate record found for same blob " + key.getID() + ". Prev record " + blobIdToMessageMap.get(
                  key.getID()) + ", New record " + value);
        }
        blobIdToMessageMap.put(key.getID(), value);
      }
    }
    return numberOfKeysProcessed;
  }

  /**
   * @param indexValue the {@link IndexValue} to be checked if deleted
   * @return {@code true} if the value represents deleted entry, {@code false} otherwise
   */
  private boolean isDeleted(IndexValue indexValue) {
    return indexValue.isFlagSet(IndexValue.Flags.Delete_Index);
  }

  /**
   * Holds statistics about active blobs viz total number of put records, delete records, duplicate records and so on
   */
  private class ActiveBlobStats {
    private AtomicLong totalPutRecords = new AtomicLong(0);
    private AtomicLong totalDeleteRecords = new AtomicLong(0);
    private AtomicLong totalDuplicatePutRecords = new AtomicLong(0);
    private AtomicLong totalDeleteBeforePutOrDuplicateDeleteRecords = new AtomicLong(0);

    AtomicLong getTotalPutRecords() {
      return totalPutRecords;
    }

    void incrementTotalPutRecords() {
      this.totalPutRecords.incrementAndGet();
    }

    AtomicLong getTotalDeleteRecords() {
      return totalDeleteRecords;
    }

    void incrementTotalDeleteRecords() {
      this.totalDeleteRecords.incrementAndGet();
    }

    AtomicLong getTotalDuplicatePutRecords() {
      return totalDuplicatePutRecords;
    }

    void incrementTotalDuplicatePutRecords() {
      this.totalDuplicatePutRecords.incrementAndGet();
    }

    AtomicLong getTotalDeleteBeforePutOrDuplicateDeleteRecords() {
      return totalDeleteBeforePutOrDuplicateDeleteRecords;
    }

    void incrementTotalDeleteBeforePutOrDuplicateDeleteRecords() {
      this.totalDeleteBeforePutOrDuplicateDeleteRecords.incrementAndGet();
    }
  }
}
