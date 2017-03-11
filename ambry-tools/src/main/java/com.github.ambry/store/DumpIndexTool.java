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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONException;
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
  // The index file that needs to be dumped
  private final String fileToRead;
  // File path referring to the hardware layout
  private final String hardwareLayoutFilePath;
  // File path referring to the partition layout
  private final String partitionLayoutFilePath;
  // The type of operation to perform
  private final String typeOfOperation;
  // List of blobIds (comma separated values) to filter
  private final String blobIdList;
  // Path referring to replica root directory
  private final String replicaRootDirecotry;
  // Count of active blobs
  private final long activeBlobsCount;
  // True if active blobs onlhy needs to be dumped, false otherwise
  private final boolean activeBlobsOnly;
  // True if blob stats needs to be logged, false otherwise
  private final boolean logBlobStats;

  private static final Logger logger = LoggerFactory.getLogger(DumpIndexTool.class);

  public DumpIndexTool(VerifiableProperties verifiableProperties) throws IOException, JSONException {
    fileToRead = verifiableProperties.getString("file.to.read");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    typeOfOperation = verifiableProperties.getString("type.of.operation");
    blobIdList = verifiableProperties.getString("blobId.list", "");
    replicaRootDirecotry = verifiableProperties.getString("replica.root.directory");
    activeBlobsCount = verifiableProperties.getInt("active.blobs.count", -1);
    activeBlobsOnly = verifiableProperties.getBoolean("active.blobs.only", false);
    logBlobStats = verifiableProperties.getBoolean("log.blob.stats", false);
    if (!new File(hardwareLayoutFilePath).exists() || !new File(partitionLayoutFilePath).exists()) {
      throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
    }
    this.clusterMap = new ClusterMapManager(hardwareLayoutFilePath, partitionLayoutFilePath,
        new ClusterMapConfig(verifiableProperties));
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = StoreToolsUtil.getVerifiableProperties(args);
    DumpIndexTool dumpIndexTool = new DumpIndexTool(verifiableProperties);
    dumpIndexTool.doOperation();
  }

  /**
   * Executes the operation with the help of properties passed
   * @throws IOException
   */
  public void doOperation() throws Exception {
    ArrayList<String> blobs = null;
    String[] blobArray;
    if (!blobIdList.equals("")) {
      blobArray = blobIdList.split(",");
      blobs = new ArrayList<>();
      blobs.addAll(Arrays.asList(blobArray));
      logger.info("Blobs to look out for :: " + blobs);
    }

    logger.info("Type of Operation " + typeOfOperation);
    if (fileToRead != null) {
      logger.info("File to read " + fileToRead);
    }

    switch (typeOfOperation) {
      case "DumpIndexFile":
        if (activeBlobsOnly) {
          dumpActiveBlobsFromIndex(new File(fileToRead), blobs);
        } else {
          dumpIndex(new File(fileToRead), null, null, blobs, null, new IndexStats(), logBlobStats);
        }
        break;
      case "DumpIndexesForReplica":
        if (activeBlobsOnly) {
          dumpActiveBlobsForReplica(replicaRootDirecotry, blobs);
        } else {
          dumpIndexesForReplica(replicaRootDirecotry, blobs, logBlobStats);
        }
        break;
      case "DumpNRandomActiveBlobsForReplica":
        if (activeBlobsCount == -1) {
          throw new IllegalArgumentException("Active Blobs count should be set for operation " + typeOfOperation);
        }
        dumpNRandomActiveBlobsForReplica(replicaRootDirecotry, blobs, activeBlobsCount);
        break;
      default:
        logger.error("Unknown typeOfOperation " + typeOfOperation);
        break;
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
   * @param logBlobStats {@code true} if blobs stats needs to be logged, {@code false} otherwise
   * @return the total number of records processed
   * @throws Exception
   */
  long dumpIndex(File indexFileToDump, String replica, ArrayList<String> replicaList, ArrayList<String> blobList,
      Map<String, BlobStatus> blobIdToStatusMap, IndexStats indexStats, boolean logBlobStats) throws Exception {
    Map<String, IndexValue> blobIdToMessageMapPerIndexFile = new HashMap<>();
    logger.trace("Dumping index {} for {}", indexFileToDump.getName(), replica);
    long blobsProcessed = dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile);

    for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
      if (blobList == null || blobList.size() == 0 || blobList.contains(key)) {
        logger.trace(blobIdToMessageMapPerIndexFile.get(key).toString());
        IndexValue indexValue = blobIdToMessageMapPerIndexFile.get(key);
        if (blobIdToStatusMap == null) {
          logger.info(key + " : " + indexValue.toString());
          if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs())) {
            indexStats.incrementTotalDeleteRecords();
          } else {
            indexStats.incrementTotalPutRecords();
          }
        } else {
          if (blobIdToStatusMap.containsKey(key)) {
            BlobStatus mapValue = blobIdToStatusMap.get(key);
            if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs())) {
              if (mapValue.getAvailable().contains(replica)) {
                indexStats.incrementTotalDeleteRecords();
              } else if (mapValue.getDeletedOrExpired().contains(replica)) {
                indexStats.incrementTotalDuplicateDeleteRecords();
              }
              mapValue.addDeletedOrExpired(replica);
            } else {
              if (mapValue.getDeletedOrExpired().contains(replica)) {
                logger.error("Put Record found after delete record for " + replica);
                indexStats.incrementTotalPutAfterDeleteRecords();
              }
              if (mapValue.getAvailable().contains(replica)) {
                logger.error("Duplicate Put record found for " + replica);
                indexStats.incrementTotalDuplicatePutRecords();
              }
              mapValue.addAvailable(replica);
            }
          } else {
            BlobStatus mapValue =
                new BlobStatus(replica, isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs()),
                    replicaList);
            blobIdToStatusMap.put(key, mapValue);
            if (isDeleted(indexValue)) {
              logger.trace("Delete record found before Put record for {} ", key);
              indexStats.incrementTotalDeleteBeforePutRecords();
            } else {
              indexStats.incrementTotalPutRecords();
            }
          }
        }
      }
    }
    if (logBlobStats) {
      logger.info("Total Put Records for index file " + indexFileToDump + " " + indexStats.getTotalPutRecords().get());
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
  }

  /**
   * Dumps all index files for a given Replica
   * @param replicaRootDirectory the root directory for a replica
   * @param blobList list of blobIds to be filtered for. Can be {@code null}
   * @param logBlobStats {@code true} if blobs stats needs to be logged, {@code false} otherwise
   * @return a {@link Map} of BlobId to {@link BlobStatus} containing the information about every blob in
   * this replica
   * @throws Exception
   */
  private Map<String, BlobStatus> dumpIndexesForReplica(String replicaRootDirectory, ArrayList<String> blobList,
      boolean logBlobStats) throws Exception {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    logger.info("Root directory for replica : " + replicaRootDirectory);
    IndexStats indexStats = new IndexStats();
    Map<String, BlobStatus> blobIdToStatusMap = new HashMap<>();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File indexFile : replicas) {
      logger.info("Dumping index " + indexFile + " for replica " + replicaDirectory.getName());
      totalKeysProcessed +=
          dumpIndex(indexFile, replicaDirectory.getName(), null, blobList, blobIdToStatusMap, indexStats, logBlobStats);
    }
    long totalActiveRecords = 0;
    for (String key : blobIdToStatusMap.keySet()) {
      BlobStatus blobStatus = blobIdToStatusMap.get(key);
      if (logBlobStats) {
        logger.info(key + " : " + blobStatus.toString());
      }
      if (!blobStatus.getIsDeletedOrExpired()) {
        totalActiveRecords++;
      }
    }
    logger.info("Total Keys processed for replica " + replicaDirectory.getName() + " : " + totalKeysProcessed);
    logger.info("Total Put Records " + indexStats.getTotalPutRecords().get());
    logger.info("Total Delete Records " + indexStats.getTotalDeleteRecords().get());
    logger.info("Total Active Records " + totalActiveRecords);
    logger.info("Total Duplicate Put Records " + indexStats.getTotalDuplicatePutRecords().get());
    logger.info("Total Delete before Put Records " + indexStats.getTotalDeleteBeforePutRecords().get());
    logger.info("Total Put after Delete Records " + indexStats.getTotalPutAfterDeleteRecords().get());
    logger.info("Total Duplicate Delete Records " + indexStats.getTotalDuplicateDeleteRecords().get());
    return blobIdToStatusMap;
  }

  /**
   * Dumps active blobs for a given index file
   * @param indexFileToDump the index file that needs to be parsed for
   * @param blobList List of BlobIds that needs to be filtered for. Can be {@code null}
   * @param blobIdToBlobMessageMap a {@link Map} of BlobId to {@link IndexValue} that needs to be updated with the information
   *                               about the blobs in the index
   * @param activeBlobStats {@link ActiveBlobStats} to be updated with necessary stats
   * @return the total number of blobs parsed from the given index file
   * @throws Exception
   */
  private long dumpActiveBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList,
      Map<String, IndexValue> blobIdToBlobMessageMap, ActiveBlobStats activeBlobStats) throws Exception {
    Map<String, IndexValue> blobIdToMessageMapPerIndexFile = new HashMap<>();

    long blobsProcessed = dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile);
    for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
      IndexValue indexValue = blobIdToMessageMapPerIndexFile.get(key);
      if (blobIdToBlobMessageMap.containsKey(key)) {
        if (isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs())) {
          blobIdToBlobMessageMap.remove(key);
          activeBlobStats.incrementTotalDeleteRecords();
        } else {
          logger.error("Found duplicate put record for " + key);
          activeBlobStats.incrementTotalDuplicatePutRecords();
        }
      } else {
        if (!(isDeleted(indexValue) || DumpDataHelper.isExpired(indexValue.getExpiresAtMs()))) {
          blobIdToBlobMessageMap.put(key, indexValue);
          activeBlobStats.incrementTotalPutRecords();
        } else {
          if (isDeleted(indexValue)) {
            logger.trace("Either duplicate delete record or delete record w/o a put record found for {} ", key);
            activeBlobStats.incrementTotalDeleteBeforePutOrDuplicateDeleteRecords();
          } else if (DumpDataHelper.isExpired(indexValue.getExpiresAtMs())) {
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
   * @throws Exception
   */
  private void dumpActiveBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList) throws Exception {
    Map<String, IndexValue> blobIdToBlobMessageMap = new HashMap<>();
    logger.trace("Dumping index {} ", indexFileToDump);
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    long totalKeysProcessed =
        dumpActiveBlobsFromIndex(indexFileToDump, blobList, blobIdToBlobMessageMap, activeBlobStats);
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
   * @throws Exception
   */
  private void dumpActiveBlobsForReplica(String replicaRootDirectory, ArrayList<String> blobList) throws Exception {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    Map<String, IndexValue> blobIdToMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File indexFile : replicas) {
      logger.info("Dumping index " + indexFile.getName() + " for " + replicaDirectory.getName());
      totalKeysProcessed += dumpActiveBlobsFromIndex(indexFile, blobList, blobIdToMessageMap, activeBlobStats);
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
   * @throws Exception
   */
  private void dumpNRandomActiveBlobsForReplica(String replicaRootDirectory, ArrayList<String> blobList,
      long randomBlobsCount) throws Exception {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    Map<String, IndexValue> blobIdToBlobMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File indexFile : replicas) {
      logger.trace("Dumping index {} for {} ", indexFile.getName(), replicaDirectory.getName());
      totalKeysProcessed += dumpActiveBlobsFromIndex(indexFile, blobList, blobIdToBlobMessageMap, activeBlobStats);
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
   * @return the total number of keys/records processed
   * @throws Exception
   */
  private long dumpBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList,
      Map<String, IndexValue> blobIdToMessageMap) throws Exception {
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    StoreMetrics metrics = new StoreMetrics(indexFileToDump.getParent(), new MetricRegistry());
    IndexSegment segment = new IndexSegment(indexFileToDump, false, storeKeyFactory, config, metrics,
        new Journal(indexFileToDump.getParent(), 0, 0), SystemTime.getInstance());
    List<MessageInfo> entries = new ArrayList<>();
    segment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
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
