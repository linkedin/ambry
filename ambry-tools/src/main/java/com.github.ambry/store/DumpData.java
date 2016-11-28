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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  DumpDataHelper dumpDataHelper;
  private static final Logger logger = LoggerFactory.getLogger(DumpData.class);

  public DumpData(ClusterMap map) {
    dumpDataHelper = new DumpDataHelper(map);
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
              + "CompareReplicaIndexesToLog or CompareLogToIndex")
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

      ArgumentAcceptingOptionSpec<String> logFileOpt = parser.accepts("logFile",
          "Log file that needs to be dumped for comparison operations like \"CompareIndexToLog\" "
              + "\"CompareReplicaIndexesToLog\" and \"CompareLogToIndex\"")
          .withRequiredArg()
          .describedAs("log_file")
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
      String logFile = options.valueOf(logFileOpt);
      String activeBlobsCountStr = options.valueOf(activeBlobsCountOpt);
      int activeBlobsCount = (activeBlobsCountStr == null || activeBlobsCountStr.equalsIgnoreCase("")) ? -1
          : Integer.parseInt(activeBlobsCountStr);
      String replicaRootDirectory = options.valueOf(replicaRootDirectoryOpt);
      boolean activeBlobsOnly = Boolean.parseBoolean(options.valueOf(activeBlobsOnlyOpt));
      boolean logBlobStats = Boolean.parseBoolean(options.valueOf(logBlobStatsOpt));
      DumpData dumpData = new DumpData(map);
      long startOffset = -1;
      long endOffset = -1;
      if (startOffsetStr != null) {
        startOffset = Long.parseLong(startOffsetStr);
      }
      if (endOffsetStr != null) {
        endOffset = Long.parseLong(endOffsetStr);
      }

      String blobList = options.valueOf(listOfBlobs);
      boolean filter = (blobList != null) ? true : false;
      ArrayList<String> blobs = new ArrayList<String>();
      String[] blobArray = null;
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
          dumpData.compareIndexEntriestoLogContentHelper(fileToRead, logFile);
          break;
        case "CompareReplicaIndexesToLog":
          dumpData.compareReplicaIndexEntriestoLogContent(replicaRootDirectory, logFile);
          break;
        case "CompareLogToIndex":
          dumpData.compareLogEntriestoIndex(logFile, blobs, replicaRootDirectory, filter, logBlobStats, startOffset,
              endOffset);
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
   * Dumps all records in an index file and updates the {@link ConcurrentHashMap} for the blob status
   * @param indexFileToDump the index file that needs to be parsed for
   * @param replica the replica from which the index files are being parsed for
   * @param replicaList total list of all replicas for the partition which this replica is part of
   * @param blobList List of blobIds to be filtered for. Can be {@code null}
   * @param blobIdToStatusMap {@link ConcurrentHashMap} of BlobId to {@link BlobStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param indexStats the {@link IndexStats} to be updated with some stats info
   * @param logBlobStats {@code true} if blobs stats needs to be logged, {@code false} otherwise
   * @return the total number of records processed
   */
  public long dumpIndex(File indexFileToDump, String replica, ArrayList<String> replicaList, ArrayList<String> blobList,
      ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap, IndexStats indexStats, boolean logBlobStats) {
    ConcurrentHashMap<String, DumpDataHelper.IndexRecord> blobIdToMessageMapPerIndexFile = new ConcurrentHashMap<>();
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
   * @return a {@link ConcurrentHashMap} of BlobId to {@link BlobStatus} containing the information about every blob in
   * this replica
   */
  public ConcurrentHashMap<String, BlobStatus> dumpIndexesForReplica(String replicaRootDirectory,
      ArrayList<String> blobList, boolean logBlobStats) {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    logger.info("Root directory for replica : " + replicaRootDirectory);
    IndexStats indexStats = new IndexStats();
    ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap = new ConcurrentHashMap<>();
    File[] replicas = replicaDirectory.listFiles(new IndexFileNameFilter());
    Arrays.sort(replicas, new ConsistencyCheckerTool.IndexFileNameComparator());
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
   * @param blobIdToBlobMessageMap a {@link ConcurrentHashMap} of BlobId to Message that needs to be updated with the
   *                               information about the blobs in the index
   * @param activeBlobStats {@link ActiveBlobStats} to be updated with necessary stats
   * @return the total number of blobs parsed from the given index file
   */
  private long dumpActiveBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList,
      ConcurrentHashMap<String, String> blobIdToBlobMessageMap, ActiveBlobStats activeBlobStats) {
    ConcurrentHashMap<String, DumpDataHelper.IndexRecord> blobIdToMessageMapPerIndexFile = new ConcurrentHashMap<>();

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
   */
  public void dumpActiveBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList) {
    ConcurrentHashMap<String, String> blobIdToBlobMessageMap = new ConcurrentHashMap<>();
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
   */
  public void dumpActiveBlobsForReplica(String replicaRootDirectory, ArrayList<String> blobList) {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    ConcurrentHashMap<String, String> blobIdToMessageMap = new ConcurrentHashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(new IndexFileNameFilter());
    Arrays.sort(replicas, new ConsistencyCheckerTool.IndexFileNameComparator());
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
   */
  public void dumpNRandomActiveBlobsForReplica(String replicaRootDirectory, ArrayList<String> blobList,
      long randomBlobsCount) {
    long totalKeysProcessed = 0;
    File replicaDirectory = new File(replicaRootDirectory);
    ConcurrentHashMap<String, String> blobIdToBlobMessageMap = new ConcurrentHashMap<>();
    ActiveBlobStats activeBlobStats = new ActiveBlobStats();
    File[] replicas = replicaDirectory.listFiles(new IndexFileNameFilter());
    Arrays.sort(replicas, new ConsistencyCheckerTool.IndexFileNameComparator());
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
  public void dumpLog(File logFile, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter)
      throws IOException {

    ConcurrentHashMap<String, DumpDataHelper.LogBlobRecord> blobIdToLogRecord = new ConcurrentHashMap<>();
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
   * @param logFile the log file that needs to be parsed
   * @throws Exception
   */
  public void compareReplicaIndexEntriestoLogContent(String replicaRootDirectory, String logFile) throws Exception {
    if (logFile == null) {
      logger.error("logFile needs to be set for compareIndexToLog");
      System.exit(0);
    }
    RandomAccessFile randomAccessFile = new RandomAccessFile(new File(logFile), "r");
    logger.info("Comparing Index entries to Log ");
    File replicaDirectory = new File(replicaRootDirectory);
    File[] replicas = replicaDirectory.listFiles(new IndexFileNameFilter());
    Arrays.sort(replicas, new ConsistencyCheckerTool.IndexFileNameComparator());
    for (File indexFile : replicas) {
      compareIndexEntriesToLogContent(indexFile, replicaDirectory, randomAccessFile);
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
   * Helper to compares every entry in the index with those in the log. Checks to see if each blob in index is
   * successfully deserializable from the log
   * @param indexFile the index file that needs to be checked for
   * @param logFile the log file that needs to be parsed
   * @throws Exception
   */
  public void compareIndexEntriestoLogContentHelper(String indexFile, String logFile) throws Exception {
    if (logFile == null) {
      logger.error("logFile needs to be set for compareIndexToLog");
      System.exit(0);
    }
    RandomAccessFile randomAccessFile = null;
    try {
      randomAccessFile = new RandomAccessFile(new File(logFile), "r");
      logger.info("Comparing Index entries to Log ");
      compareIndexEntriesToLogContent(new File(indexFile), null, randomAccessFile);
    } finally {
      if (randomAccessFile != null) {
        randomAccessFile.close();
      }
    }
  }

  /**
   * Compares every entry in an index file with those in the log. Checks to see if each blob in index is successfully deserializable
   * from the log
   * @param indexFile the index file that needs to be checked for
   * @param replicaDirectory the replica root directory where the index is located
   * @param randomAccessFile the {@link RandomAccessFile} referring to the log file
   * @throws Exception
   */
  private void compareIndexEntriesToLogContent(File indexFile, File replicaDirectory, RandomAccessFile randomAccessFile)
      throws Exception {
    logger.info(
        "Dumping index " + indexFile.getName() + " for " + ((replicaDirectory != null) ? replicaDirectory.getName()
            : null));
    Offset startOffset = IndexSegment.getIndexSegmentStartOffset(indexFile.getName());
    DataInputStream stream = null;
    TreeMap<Long, Long> coveredRanges = new TreeMap<Long, Long>();
    Set<Long> deleteMsgsOriginalOffsets = new HashSet<Long>();
    try {
      stream = new DataInputStream(new FileInputStream(indexFile));
      short version = stream.readShort();
      logger.trace("version " + version);
      if (version == 0) {
        int keysize = stream.readInt();
        int valueSize = stream.readInt();
        long fileEndPointer = stream.readLong();
        logger.trace("key size {} ", keysize);
        logger.trace("value size {} ", valueSize);
        logger.trace("file end pointer {} ", fileEndPointer);
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory =
            Utils.getObj("com.github.ambry.commons.BlobIdFactory", dumpDataHelper.getClusterMap());
        while (stream.available() > Crc_Size) {
          StoreKey key = storeKeyFactory.getStoreKey(stream);
          byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
          stream.read(value);
          IndexValue blobValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(value));
          String msg = "key :" + key + ": value - offset " + blobValue.getOffset() + " size " + blobValue.getSize()
              + " Original Message Offset " + blobValue.getOriginalMessageOffset() + " Flag " + blobValue.getFlags()
              + "\n";
          boolean isDeleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
          boolean success =
              dumpDataHelper.readFromLogAndVerify(randomAccessFile, blobValue.getOffset().getOffset(), key.getID(),
                  blobValue, coveredRanges);
          if (!success) {
            logger.error("Failed for Index Entry " + msg);
            continue;
          }
          if (isDeleted) {
            long originalOffset = blobValue.getOriginalMessageOffset();
            deleteMsgsOriginalOffsets.add(originalOffset);
            if (originalOffset != -1 && !coveredRanges.containsKey(originalOffset)) {
              if (startOffset.getOffset() > originalOffset) {
                logger.trace("Put Record at {} with delete msg offset {} ignored because it is prior to startOffset {}",
                    originalOffset, blobValue.getOffset(), startOffset);
              } else {
                randomAccessFile.seek(originalOffset);
                LogBlobRecordInfo logBlobRecordInfo =
                    dumpDataHelper.readSingleRecordFromLog(randomAccessFile, originalOffset);
                coveredRanges.put(originalOffset, originalOffset + logBlobRecordInfo.totalRecordSize);
                logger.trace("PUT Record {} with start offset {} and end offset {} for a delete msg {} at offset {} ",
                    logBlobRecordInfo.blobId, originalOffset, (originalOffset + logBlobRecordInfo.totalRecordSize),
                    key.getID(), blobValue.getOffset());
                if (!logBlobRecordInfo.blobId.getID().equals(key.getID())) {
                  logger.error("BlobId value mismatch between delete record " + key.getID() + " and put record "
                      + logBlobRecordInfo.blobId.getID());
                }
              }
            }
          }
        }
        logger.trace("crc {} ", stream.readLong());
        logRangesNotCovered(coveredRanges, fileEndPointer);
      }
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  /**
   * Compares every entry in the log to those in the index. Checks to see if the status of the blob is consistent with
   * each other
   * @param logFile the log file to be dumped
   * @param blobList List of BlobIds to be filtered for, Can be {@code null}
   * @param replicaRootDirectory the root directory of the replica
   * @param generateBlobStatusReport {@code true} if verbose report about status of each blob needs to be logged.
   *        {@code false} otherwise
   * @param filter {@code true} if needs to be filtered, {@code false} otherwise
   * @throws Exception
   */
  public void compareLogEntriestoIndex(String logFile, ArrayList<String> blobList, String replicaRootDirectory,
      boolean filter, boolean generateBlobStatusReport, long startOffset, long endOffset) throws Exception {
    if (logFile == null || replicaRootDirectory == null) {
      logger.error("logFile and replicaRootDirectory needs to be set for compareLogToIndex");
      System.exit(0);
    }
    ConcurrentHashMap<String, BlobStatus> indexBlobStatusMap =
        dumpIndexesForReplica(replicaRootDirectory, blobList, generateBlobStatusReport);
    ConcurrentHashMap<String, DumpDataHelper.LogBlobRecord> blobIdToLogRecordStats = new ConcurrentHashMap<>();
    dumpDataHelper.dumpLog(new File(logFile), startOffset, endOffset, blobList, filter, blobIdToLogRecordStats, false);
    long totalInconsistentBlobs = 0;
    long totalMissingBlobs = 0;

    for (String blobId : blobIdToLogRecordStats.keySet()) {
      DumpDataHelper.LogBlobRecord logBlobRecord = blobIdToLogRecordStats.get(blobId);
      if (indexBlobStatusMap.containsKey(blobId)) {
        BlobStatus blobStatus = indexBlobStatusMap.get(blobId);
        if ((logBlobRecord.isDeleted || logBlobRecord.isExpired) && !blobStatus.getIsDeletedOrExpired()) {
          logger.error("Blob " + blobId + " is deleted/expired in log while alive in index. LogRecord " + logBlobRecord
              + ", Index Blob status " + blobStatus);
          totalInconsistentBlobs++;
        }
      } else {
        logger.error("Missing blob record " + blobId + " from index. Log Record details : " + logBlobRecord);
        totalMissingBlobs++;
        totalInconsistentBlobs++;
        if (!logBlobRecord.isConsistent) {
          logger.error("An Inconsistent Blob " + blobId + " found in Log but not in index");
        }
      }
    }
    logger.info(
        "Total Inconsistent blobs count " + totalInconsistentBlobs + ", total Missing blobs " + totalMissingBlobs);
  }

  /**
   * Index file name filter that filters only those files with suffix "index"
   */
  static class IndexFileNameFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith("index");
    }
  }

  /**
   * Holds statistics about active blobs viz total number of put records, delete records, duplicate records and so on
   */
  class ActiveBlobStats {
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
