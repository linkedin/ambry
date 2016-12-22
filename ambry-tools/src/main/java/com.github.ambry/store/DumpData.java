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
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.PersistentIndex.*;


/**
 * DumpData tool to assist in dumping data from data files in Ambry
 * Supported operations are
 * 1. Dump Index
 * 2. Dump Index for a replica
 * 3. Dump active blobs for an index
 * 4. Dump active blobs for a replica
 * 5. Dump N random blobs for a replica
 * 6. Dump Log
 * 7. Dump Replica Metadata token
 * 8. Compare Index entries to Log entries
 * 9. Compare all entries in all indexes in a replica to Log entries
 * 10. Compare Log entries to index entries
 */
public class DumpData {

  private DumpDataHelper dumpDataHelper;
  private static final Logger logger = LoggerFactory.getLogger(DumpData.class);

  public DumpData(ClusterMap map, int bytesPerSec) {
    dumpDataHelper = new DumpDataHelper(map, bytesPerSec);
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> fileToReadOpt = parser.accepts("fileToRead",
          "The file that needs to be dumped. Index file incase of \"DumpIndex\", "
              + ", \"CompareIndexToLog\" log file incase of \"DumpLog\", replicatoken file in case "
              + "of \"DumpReplicatoken\"").withRequiredArg().describedAs("file_to_read").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file")
              .withRequiredArg()
              .describedAs("hardware_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file")
              .withRequiredArg()
              .describedAs("partition_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt = parser.accepts("typeOfOperation",
          "The type of operation to be performed - DumpLog or DumpIndex or DumpIndexesForReplica or "
              + "or DumpNRandomActiveBlobsForReplica or DumpReplicaToken or CompareIndexToLog or "
              + "CompareReplicaIndexesToLog")
          .withRequiredArg()
          .describedAs("The type of Operation to be " + "performed")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> listOfBlobs =
          parser.accepts("listOfBlobs", "List Of Blobs to look for while performing log or index dump operations")
              .withRequiredArg()
              .describedAs("List of blobs, comma separated")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> startOffsetOpt =
          parser.accepts("startOffset", "Log Offset to start dumping from log")
              .withRequiredArg()
              .describedAs("startOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> endOffsetOpt =
          parser.accepts("endOffset", "Log Offset to end dumping in the log")
              .withRequiredArg()
              .describedAs("endOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> replicaRootDirectoryOpt = parser.accepts("replicaRootDirectory",
          "Root directory of the replica which contains all the index files to be dumped")
          .withRequiredArg()
          .describedAs("replicaRootDirectory")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> activeBlobsCountOpt =
          parser.accepts("activeBlobsCount", "Total number of random active blobs(index msgs) to be dumped")
              .withRequiredArg()
              .describedAs("activeBlobsCount")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> activeBlobsOnlyOpt =
          parser.accepts("activeBlobsOnly", "Dumps only active blobs from index")
              .withRequiredArg()
              .describedAs("activeBlobsOnly")
              .defaultsTo("false")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> logBlobStatsOpt =
          parser.accepts("logBlobStats", "Whether to dump information about status' of blobs in each replica or not")
              .withRequiredArg()
              .describedAs("logBlobStats")
              .defaultsTo("false")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> bytesPerSecOpt = parser.accepts("bytesPerSec",
          "Allowed bytes per sec for the purpose of throttling. Any value greater than 0 "
              + "means throttling will be done at that rate. 0 disables throttling and negative will throw IllegalArgument Exception")
          .withRequiredArg()
          .describedAs("bytesPerSec")
          .ofType(Integer.class)
          .defaultsTo(0);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(typeOfOperationOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      String fileToRead = options.valueOf(fileToReadOpt);
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      String startOffsetStr = options.valueOf(startOffsetOpt);
      String endOffsetStr = options.valueOf(endOffsetOpt);
      String activeBlobsCountStr = options.valueOf(activeBlobsCountOpt);
      int activeBlobsCount = (activeBlobsCountStr == null || activeBlobsCountStr.equalsIgnoreCase("")) ? -1
          : Integer.parseInt(activeBlobsCountStr);
      String replicaRootDirectory = options.valueOf(replicaRootDirectoryOpt);
      boolean activeBlobsOnly = Boolean.parseBoolean(options.valueOf(activeBlobsOnlyOpt));
      boolean logBlobStats = Boolean.parseBoolean(options.valueOf(logBlobStatsOpt));
      int bytesPerSec = options.valueOf(bytesPerSecOpt);
      DumpData dumpData = new DumpData(map, bytesPerSec);
      long startOffset = -1;
      long endOffset = -1;
      if (startOffsetStr != null) {
        startOffset = Long.parseLong(startOffsetStr);
      }
      if (endOffsetStr != null) {
        endOffset = Long.parseLong(endOffsetStr);
      }

      String blobList = options.valueOf(listOfBlobs);
      boolean filter = blobList != null;
      ArrayList<String> blobs = new ArrayList<String>();
      String[] blobArray;
      if (blobList != null) {
        blobArray = blobList.split(",");
        blobs.addAll(Arrays.asList(blobArray));
        logger.info("Blobs to look out for :: " + blobs);
      }

      logger.info("Type of Operation " + typeOfOperation);
      if (fileToRead != null) {
        logger.info("File to read " + fileToRead);
      }

      switch (typeOfOperation) {
        case "DumpIndex":
          if (activeBlobsOnly) {
            dumpData.dumpActiveBlobsFromIndex(new File(fileToRead), (filter) ? blobs : null);
          } else {
            dumpData.dumpIndex(new File(fileToRead), null, null, (filter) ? blobs : null, null, new IndexStats(),
                logBlobStats);
          }
          break;
        case "DumpIndexesForReplica":
          if (activeBlobsOnly) {
            dumpData.dumpActiveBlobsForReplica(replicaRootDirectory, (filter) ? blobs : null);
          } else {
            dumpData.dumpIndexesForReplica(replicaRootDirectory, (filter) ? blobs : null, logBlobStats);
          }
          break;
        case "DumpNRandomActiveBlobsForReplica":
          if (activeBlobsCount == -1) {
            throw new IllegalArgumentException("Active Blobs count should be set for operation " + typeOfOperation);
          }
          dumpData.dumpNRandomActiveBlobsForReplica(replicaRootDirectory, (filter) ? blobs : null, activeBlobsCount);
          break;
        case "DumpLog":
          dumpData.dumpLog(new File(fileToRead), startOffset, endOffset, blobs, filter);
          break;
        case "DumpReplicaToken":
          dumpData.dumpDataHelper.dumpReplicaToken(new File(fileToRead));
          break;
        case "CompareIndexToLog":
          dumpData.compareIndexEntriesToLogContent(new File(fileToRead), false);
          break;
        case "CompareReplicaIndexesToLog":
          dumpData.compareReplicaIndexEntriestoLogContent(replicaRootDirectory);
          break;
        default:
          logger.error("Unknown typeOfOperation " + typeOfOperation);
          break;
      }
    } catch (Exception e) {
      logger.error("Closed with exception ", e);
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
    Map<String, DumpDataHelper.IndexRecord> blobIdToMessageMapPerIndexFile = new HashMap<>();
    logger.trace("Dumping index {} for {}", indexFileToDump.getName(), replica);
    long blobsProcessed = dumpDataHelper.dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile);

    for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
      if (blobList == null || blobList.size() == 0 || blobList.contains(key.toString())) {
        logger.trace(blobIdToMessageMapPerIndexFile.get(key).toString());
        DumpDataHelper.IndexRecord indexRecord = blobIdToMessageMapPerIndexFile.get(key);
        if (blobIdToStatusMap == null) {
          logger.info(indexRecord.toString());
          if (indexRecord.isDeleted() || indexRecord.isExpired()) {
            indexStats.incrementTotalDeleteRecords();
          } else {
            indexStats.incrementTotalPutRecords();
          }
        } else {
          if (blobIdToStatusMap.containsKey(key)) {
            BlobStatus mapValue = blobIdToStatusMap.get(key);
            if (indexRecord.isDeleted() || indexRecord.isExpired()) {
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
                new BlobStatus(replica, indexRecord.isDeleted() || indexRecord.isExpired(), replicaList);
            blobIdToStatusMap.put(key, mapValue);
            if (indexRecord.isDeleted()) {
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
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_FILE_COMPARATOR);
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
   * @param blobIdToBlobMessageMap a {@link Map} of BlobId to Message that needs to be updated with the information
   *                               about the blobs in the index
   * @param activeBlobStats {@link ActiveBlobStats} to be updated with necessary stats
   * @return the total number of blobs parsed from the given index file
   * @throws Exception
   */
  private long dumpActiveBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList,
      Map<String, String> blobIdToBlobMessageMap, ActiveBlobStats activeBlobStats) throws Exception {
    Map<String, DumpDataHelper.IndexRecord> blobIdToMessageMapPerIndexFile = new HashMap<>();

    long blobsProcessed = dumpDataHelper.dumpBlobsFromIndex(indexFileToDump, blobList, blobIdToMessageMapPerIndexFile);
    for (String key : blobIdToMessageMapPerIndexFile.keySet()) {
      DumpDataHelper.IndexRecord indexRecord = blobIdToMessageMapPerIndexFile.get(key);
      if (blobIdToBlobMessageMap.containsKey(key)) {
        if (indexRecord.isDeleted() || indexRecord.isExpired()) {
          blobIdToBlobMessageMap.remove(key);
          activeBlobStats.incrementTotalDeleteRecords();
        } else {
          logger.error("Found duplicate put record for " + key);
          activeBlobStats.incrementTotalDuplicatePutRecords();
        }
      } else {
        if (!(indexRecord.isDeleted() || indexRecord.isExpired())) {
          blobIdToBlobMessageMap.put(key, indexRecord.getMessage());
          activeBlobStats.incrementTotalPutRecords();
        } else {
          if (indexRecord.isDeleted()) {
            logger.trace("Either duplicate delete record or delete record w/o a put record found for {} ", key);
            activeBlobStats.incrementTotalDeleteBeforePutOrDuplicateDeleteRecords();
          } else if (indexRecord.isExpired()) {
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
    Map<String, String> blobIdToBlobMessageMap = new HashMap<>();
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
    Map<String, String> blobIdToMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_FILE_COMPARATOR);
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
    Map<String, String> blobIdToBlobMessageMap = new HashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(PersistentIndex.INDEX_FILE_FILTER);
    Arrays.sort(replicas, PersistentIndex.INDEX_FILE_COMPARATOR);
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
    List<String> keys = new ArrayList<String>(blobIdToBlobMessageMap.keySet());
    int randomCount = 0;
    while (randomCount < totalBlobsToBeDumped) {
      Collections.shuffle(keys);
      logger.info(blobIdToBlobMessageMap.get(keys.remove(0)));
      randomCount++;
    }
    logger.info("Total blobs dumped " + totalBlobsToBeDumped);
  }

  /**
   * Dumps all records in a given log file
   * @param logFile the log file that needs to be parsed for
   * @param startOffset the starting offset from which records needs to be dumped from. Can be {@code null}
   * @param endOffset the end offset until which records need to be dumped to. Can be {@code null}
   * @param blobs List of blobIds to be filtered for
   * @param filter {@code true} if filtering has to be done, {@code false} otherwise
   * @throws IOException
   */
  private void dumpLog(File logFile, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter)
      throws IOException {

    Map<String, DumpDataHelper.LogBlobRecord> blobIdToLogRecord = new HashMap<>();
    dumpDataHelper.dumpLog(logFile, startOffset, endOffset, blobs, filter, blobIdToLogRecord, true);

    long totalInConsistentBlobs = 0;
    for (String blobId : blobIdToLogRecord.keySet()) {
      DumpDataHelper.LogBlobRecord logBlobRecord = blobIdToLogRecord.get(blobId);
      if (!logBlobRecord.isConsistent) {
        totalInConsistentBlobs++;
        logger.error("Inconsistent blob " + blobId + " " + logBlobRecord);
      }
    }
    logger.info("Total inconsistent blob count " + totalInConsistentBlobs);
  }

  /**
   * Compares every entry in every index file of a replica with those in the log.
   * Checks to see if each blob in index is successfully deserializable from the log
   * @param replicaRootDirectory the root directory of the replica
   * @throws Exception
   */
  private void compareReplicaIndexEntriestoLogContent(String replicaRootDirectory) throws Exception {
    logger.info("Comparing Index entries to Log ");
    File[] indexFiles = new File(replicaRootDirectory).listFiles(INDEX_FILE_FILTER);
    if (indexFiles == null) {
      throw new IllegalStateException("Could not read index files from " + replicaRootDirectory);
    }
    Arrays.sort(indexFiles, INDEX_FILE_COMPARATOR);
    for (int i = 0; i < indexFiles.length; i++) {
      // check end offset if this is the last index segment
      boolean checkEndOffset = i == indexFiles.length - 1;
      if (!checkEndOffset) {
        // check end offset if the log segment represented by this index segment is different from the one represented
        // by the next one
        String currLogSegmentRef = IndexSegment.getIndexSegmentStartOffset(indexFiles[i].getName()).getName();
        String nextLogSegmentRef = IndexSegment.getIndexSegmentStartOffset(indexFiles[i + 1].getName()).getName();
        checkEndOffset = !currLogSegmentRef.equals(nextLogSegmentRef);
      }
      compareIndexEntriesToLogContent(indexFiles[i], checkEndOffset);
    }
  }

  /**
   * Log ranges not covered by the index in the log
   * @param coveredRanges {@link Map} of startOffsets to endOffsets of ranges covered by records in the log
   * @param indexEndOffset the end offset in the log that this index segment covers
   */
  private void logRangesNotCovered(Map<Long, Long> coveredRanges, long indexEndOffset) {
    Iterator<Map.Entry<Long, Long>> iterator = coveredRanges.entrySet().iterator();
    Map.Entry<Long, Long> prevEntry = iterator.next();
    logger.trace("Record startOffset {} , endOffset {} ", prevEntry.getKey(), prevEntry.getValue());
    while (iterator.hasNext()) {
      Map.Entry<Long, Long> curEntry = iterator.next();
      logger.trace("Record startOffset {} , endOffset {} ", curEntry.getKey(), curEntry.getValue());
      if (prevEntry.getValue().compareTo(curEntry.getKey()) != 0) {
        logger.error("Cannot find entries in Index ranging from " + prevEntry.getValue() + " to " + curEntry.getKey()
            + " with a hole of size " + (curEntry.getKey() - prevEntry.getValue()) + " in the Log");
      }
      prevEntry = curEntry;
    }
    if (prevEntry.getValue().compareTo(indexEndOffset) != 0) {
      logger.error("End offset mismatch. FileEndPointer from the index segment " + indexEndOffset
          + ", end offset as per records " + prevEntry.getValue());
    }
  }

  /**
   * Compares every entry in an index file with those in the log. Checks to see if each blob in index is successfully
   * deserializable from the log
   * @param indexFile the file that represents the index segment.
   * @param checkLogEndOffsetMatch if {@code true}, checks that the end offset of the log matches the end offset of the
   *                               index.
   * @throws Exception
   */
  private void compareIndexEntriesToLogContent(File indexFile, boolean checkLogEndOffsetMatch) throws Exception {
    logger.info("Dumping index {}", indexFile.getAbsolutePath());
    StoreKeyFactory storeKeyFactory =
        Utils.getObj("com.github.ambry.commons.BlobIdFactory", dumpDataHelper.getClusterMap());
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    StoreMetrics metrics = new StoreMetrics(indexFile.getParent(), new MetricRegistry());
    IndexSegment segment =
        new IndexSegment(indexFile, false, storeKeyFactory, config, metrics, new Journal(indexFile.getParent(), 0, 0));
    Offset startOffset = segment.getStartOffset();
    TreeMap<Long, Long> coveredRanges = new TreeMap<>();
    String logFileName = LogSegmentNameHelper.nameToFilename(segment.getLogSegmentName());
    File logFile = new File(indexFile.getParent(), logFileName);
    RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
    List<MessageInfo> entries = new ArrayList<>();
    segment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
    for (MessageInfo entry : entries) {
      StoreKey key = entry.getStoreKey();
      IndexValue value = segment.find(key);
      boolean isDeleted = value.isFlagSet(IndexValue.Flags.Delete_Index);
      boolean success = dumpDataHelper.readFromLogAndVerify(randomAccessFile, key.getID(), value, coveredRanges);
      if (success) {
        if (isDeleted) {
          long originalOffset = value.getOriginalMessageOffset();
          if (originalOffset != -1) {
            if (!coveredRanges.containsKey(originalOffset)) {
              if (startOffset.getOffset() > originalOffset) {
                logger.trace("Put Record at {} with delete msg offset {} ignored because it is prior to startOffset {}",
                    originalOffset, value.getOffset(), startOffset);
              } else {
                LogBlobRecordInfo logBlobRecordInfo =
                    dumpDataHelper.readSingleRecordFromLog(randomAccessFile, originalOffset);
                coveredRanges.put(originalOffset, originalOffset + logBlobRecordInfo.totalRecordSize);
                logger.trace("PUT Record {} with start offset {} and end offset {} for a delete msg {} at offset {} ",
                    logBlobRecordInfo.blobId, originalOffset, (originalOffset + logBlobRecordInfo.totalRecordSize),
                    key.getID(), value.getOffset());
                if (!logBlobRecordInfo.blobId.getID().equals(key.getID())) {
                  logger.error("BlobId value mismatch between delete record " + key.getID() + " and put record "
                      + logBlobRecordInfo.blobId.getID());
                }
              }
            }
          }
        }
      } else {
        logger.error("Failed for key {} with value {} ", key, value);
      }
    }
    long indexEndOffset = segment.getEndOffset().getOffset();
    if (checkLogEndOffsetMatch && indexEndOffset != randomAccessFile.length()) {
      logger.error("Log end offset {} and index end offset {} do not match", randomAccessFile.length(), indexEndOffset);
    }
    logRangesNotCovered(coveredRanges, indexEndOffset);
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
