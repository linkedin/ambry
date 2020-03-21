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
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping data from data files in Ambry
 * Supported operations are
 * 1. Compare Index entries to Log entries
 * 2. Compare all entries in all indexes in a replica to Log entries
 * 3. Compare Log entries to index entries
 */
public class DumpDataTool {
  private final ClusterMap clusterMap;
  // The index file that needs to be dumped for comparison purposes
  private final String fileToRead;

  // File path referring to the hardware layout
  private final String hardwareLayoutFilePath;

  // File path referring to the partition layout
  private final String partitionLayoutFilePath;

  // The type of operation to perform
  private final String typeOfOperation;

  // Path referring to replica root directory
  private final String replicaRootDirectory;

  // The throttling value in index entries per sec
  private final double indexEntriesPerSec;

  private final StoreToolsMetrics metrics;
  private final Throttler throttler;
  private final Time time;
  private final long currentTimeInMs;

  private static final Logger logger = LoggerFactory.getLogger(DumpDataTool.class);

  public DumpDataTool(VerifiableProperties verifiableProperties, StoreToolsMetrics metrics) throws Exception {
    fileToRead = verifiableProperties.getString("file.to.read", "");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    typeOfOperation = verifiableProperties.getString("type.of.operation");
    replicaRootDirectory = verifiableProperties.getString("replica.root.directory", "");
    indexEntriesPerSec = verifiableProperties.getDouble("index.entries.per.sec", 1000);
    throttler = new Throttler(indexEntriesPerSec, 1000, true, SystemTime.getInstance());
    if (!new File(hardwareLayoutFilePath).exists() || !new File(partitionLayoutFilePath).exists()) {
      throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
    }
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    this.clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            hardwareLayoutFilePath, partitionLayoutFilePath)).getClusterMap();
    time = SystemTime.getInstance();
    currentTimeInMs = time.milliseconds();
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
      DumpDataTool dumpDataTool = new DumpDataTool(verifiableProperties, metrics);
      dumpDataTool.doOperation();
    } finally {
      if (reporter != null) {
        reporter.stop();
      }
    }
  }

  /**
   * Executes the operation with the help of properties passed during initialization of {@link DumpDataTool}
   * @throws Exception
   */
  public void doOperation() throws Exception {
    logger.info("Type of Operation " + typeOfOperation);
    switch (typeOfOperation) {
      case "CompareIndexToLog":
        compareIndexEntriesToLogContent(new File(fileToRead), false);
        break;
      case "CompareReplicaIndexesToLog":
        compareReplicaIndexEntriestoLogContent(replicaRootDirectory);
        break;
      default:
        logger.error("Unknown typeOfOperation " + typeOfOperation);
        break;
    }
  }

  /**
   * Compares every entry in every index file of a replica with those in the log.
   * Checks to see if each blob in index is successfully deserializable from the log
   * @param replicaRootDirectory the root directory of the replica
   * @throws Exception
   */
  private void compareReplicaIndexEntriestoLogContent(String replicaRootDirectory) throws Exception {
    if (!new File(replicaRootDirectory).exists()) {
      throw new IllegalArgumentException("Replica root directory does not exist " + replicaRootDirectory);
    }
    final Timer.Context context = metrics.compareReplicaIndexFilesToLogTimeMs.time();
    try {
      logger.info("Comparing Index entries to Log ");
      File[] indexFiles = new File(replicaRootDirectory).listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
      if (indexFiles == null || indexFiles.length == 0) {
        throw new IllegalStateException("No index files found in replica root directory " + replicaRootDirectory);
      }
      Arrays.sort(indexFiles, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
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
    } finally {
      context.stop();
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
        metrics.logRangeNotFoundInIndexError.inc();
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
    if (!indexFile.exists()) {
      throw new IllegalArgumentException("File does not exist " + indexFile);
    }
    final Timer.Context context = metrics.compareIndexFileToLogTimeMs.time();
    try {
      logger.info("Dumping index {}", indexFile.getAbsolutePath());
      StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MetricRegistry metricRegistry = new MetricRegistry();
      StoreMetrics storeMetrics = new StoreMetrics(metricRegistry);
      IndexSegment segment = new IndexSegment(indexFile, false, storeKeyFactory, config, storeMetrics,
          new Journal(indexFile.getParent(), 0, 0), time);
      Offset startOffset = segment.getStartOffset();
      TreeMap<Long, Long> coveredRanges = new TreeMap<>();
      String logFileName = LogSegmentNameHelper.nameToFilename(segment.getLogSegmentName());
      File logFile = new File(indexFile.getParent(), logFileName);
      if (!logFile.exists()) {
        throw new IllegalStateException("Log file does not exist " + logFile);
      }
      RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
      long logFileSize = randomAccessFile.getChannel().size();
      List<MessageInfo> entries = new ArrayList<>();
      segment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
      for (MessageInfo entry : entries) {
        StoreKey key = entry.getStoreKey();
        IndexValue value = segment.find(key).last();
        boolean isDeleted = value.isFlagSet(IndexValue.Flags.Delete_Index);
        if (value.getOffset().getOffset() < logFileSize) {
          boolean success = readFromLogAndVerify(randomAccessFile, key.getID(), value, coveredRanges);
          if (success) {
            if (isDeleted) {
              long originalOffset = value.getOriginalMessageOffset();
              if (originalOffset != -1) {
                if (!coveredRanges.containsKey(originalOffset)) {
                  if (startOffset.getOffset() > originalOffset) {
                    logger.trace(
                        "Put Record at {} with delete msg offset {} ignored because it is prior to startOffset {}",
                        originalOffset, value.getOffset(), startOffset);
                  } else {
                    try {
                      DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo =
                          DumpDataHelper.readSingleRecordFromLog(randomAccessFile, originalOffset, clusterMap,
                              currentTimeInMs, metrics);
                      coveredRanges.put(originalOffset, originalOffset + logBlobRecordInfo.totalRecordSize);
                      logger.trace(
                          "PUT Record {} with start offset {} and end offset {} for a delete msg {} at offset {} ",
                          logBlobRecordInfo.blobId, originalOffset,
                          (originalOffset + logBlobRecordInfo.totalRecordSize), key.getID(), value.getOffset());
                      if (!logBlobRecordInfo.blobId.getID().equals(key.getID())) {
                        logger.error("BlobId value mismatch between delete record {} and put record {}", key.getID(),
                            logBlobRecordInfo.blobId.getID());
                      }
                    } catch (IllegalArgumentException e) {
                      metrics.logDeserializationError.inc();
                      logger.error("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", "
                          + "while reading blob starting at offset " + originalOffset + " with exception: ", e);
                    } catch (MessageFormatException e) {
                      metrics.logDeserializationError.inc();
                      logger.error("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position()
                          + " while reading blob starting at offset " + originalOffset + " with exception: ", e);
                    } catch (EOFException e) {
                      metrics.endOfFileOnDumpLogError.inc();
                      logger.error("EOFException thrown at " + randomAccessFile.getChannel().position() + " ", e);
                    } catch (Exception e) {
                      metrics.unknownErrorOnDumpIndex.inc();
                      logger.error("Unknown exception thrown " + e.getMessage() + " ", e);
                    }
                  }
                }
              }
            }
          } else {
            metrics.indexToLogBlobRecordComparisonFailure.inc();
            logger.error("Failed for key {} with value {} ", key, value);
          }
        } else {
          logger.trace("Blob's {} offset {} is outside of log size {}, with a diff of {}", key,
              value.getOffset().getOffset(), logFileSize, (value.getOffset().getOffset() - logFileSize));
        }
      }
      throttler.maybeThrottle(entries.size());
      long indexEndOffset = segment.getEndOffset().getOffset();
      if (checkLogEndOffsetMatch && indexEndOffset != randomAccessFile.length()) {
        metrics.indexLogEndOffsetMisMatchError.inc();
        logger.error("Log end offset {} and index end offset {} do not match", randomAccessFile.length(),
            indexEndOffset);
      }
      logRangesNotCovered(coveredRanges, indexEndOffset);
    } finally {
      context.stop();
    }
  }

  /**
   * Dumps a single record from the log at a given offset and verifies for corresponding values in index
   * @param randomAccessFile the {@link RandomAccessFile} referring to log file that needs to be parsed
   * @param blobId the blobId which that is expected to be matched for the record present at
   *               <code>offset</code>
   * @param indexValue the {@link IndexValue} that needs to be compared against
   * @param coveredRanges a {@link Map} of startOffset to endOffset of ranges covered by records in the log
   * @throws IOException
   */
  private boolean readFromLogAndVerify(RandomAccessFile randomAccessFile, String blobId, IndexValue indexValue,
      Map<Long, Long> coveredRanges) throws Exception {
    final Timer.Context context = metrics.readFromLogAndVerifyTimeMs.time();
    long offset = indexValue.getOffset().getOffset();
    try {
      DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo =
          DumpDataHelper.readSingleRecordFromLog(randomAccessFile, offset, clusterMap, currentTimeInMs, metrics);
      if (coveredRanges != null) {
        coveredRanges.put(offset, offset + logBlobRecordInfo.totalRecordSize);
      }
      compareIndexValueToLogEntry(blobId, indexValue, logBlobRecordInfo);
      if (!logBlobRecordInfo.isDeleted) {
        logger.trace("{}", logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId.getID() + "\n"
            + logBlobRecordInfo.blobEncryptionKey + "\n" + logBlobRecordInfo.blobProperty + "\n"
            + logBlobRecordInfo.userMetadata + "\n" + logBlobRecordInfo.blobDataOutput);
      } else {
        logger.trace("{}", logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId.getID() + "\n"
            + logBlobRecordInfo.deleteMsg);
      }
      return true;
    } catch (IllegalArgumentException e) {
      metrics.logDeserializationError.inc();
      logger.error("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", "
          + "while reading blob starting at offset " + offset + " with exception: ", e);
    } catch (MessageFormatException e) {
      metrics.logDeserializationError.inc();
      logger.error("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position()
          + " while reading blob starting at offset " + offset + " with exception: ", e);
    } catch (EOFException e) {
      metrics.endOfFileOnDumpLogError.inc();
      logger.error("EOFException thrown at " + randomAccessFile.getChannel().position() + " ", e);
    } catch (Exception e) {
      metrics.unknownErrorOnDumpLog.inc();
      logger.error("Unknown exception thrown " + e.getMessage() + " ", e);
    } finally {
      context.stop();
    }
    return false;
  }

  /**
   * Compares values from index to that in the Log
   * @param blobId the blobId for which comparison is made
   * @param indexValue the {@link IndexValue} to be used in comparison
   * @param logBlobRecordInfo the {@link DumpDataHelper.LogBlobRecordInfo} to be used in comparison
   */
  private void compareIndexValueToLogEntry(String blobId, IndexValue indexValue,
      DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo) {
    boolean isDeleted = indexValue.isFlagSet(IndexValue.Flags.Delete_Index);
    boolean isExpired = DumpDataHelper.isExpired(indexValue.getExpiresAtMs(), currentTimeInMs);
    if (isDeleted != logBlobRecordInfo.isDeleted) {
      metrics.indexToLogDeleteFlagMisMatchError.inc();
      logger.error(
          "Deleted value mismatch for " + logBlobRecordInfo.blobId + " Index value " + isDeleted + ", Log value "
              + logBlobRecordInfo.isDeleted);
    } else if (!logBlobRecordInfo.isDeleted && isExpired != logBlobRecordInfo.isExpired) {
      metrics.indexToLogExpiryMisMatchError.inc();
      logger.error(
          "Expiration value mismatch for " + logBlobRecordInfo.blobId + " Index value " + isExpired + ", Log value "
              + logBlobRecordInfo.isExpired + ", index expiresAt in ms " + indexValue.getExpiresAtMs()
              + ", log expiresAt in ms " + logBlobRecordInfo.expiresAtMs);
    } else if (!blobId.equals(logBlobRecordInfo.blobId.getID())) {
      metrics.indexToLogBlobIdMisMatchError.inc();
      logger.error("BlobId value mismatch for " + logBlobRecordInfo.blobId + " Index value " + blobId + ", Log value "
          + logBlobRecordInfo.blobId);
    }
  }
}
