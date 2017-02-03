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
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.utils.SystemTime;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONException;
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
  private final String replicaRootDirecotry;

  private static final Logger logger = LoggerFactory.getLogger(DumpDataTool.class);

  public DumpDataTool(VerifiableProperties verifiableProperties) throws IOException, JSONException {
    fileToRead = verifiableProperties.getString("file.to.read");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    typeOfOperation = verifiableProperties.getString("type.of.operation");
    replicaRootDirecotry = verifiableProperties.getString("replica.root.directory");
    if (!new File(hardwareLayoutFilePath).exists() || !new File(partitionLayoutFilePath).exists()) {
      throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
    }
    clusterMap = new ClusterMapManager(hardwareLayoutFilePath, partitionLayoutFilePath,
        new ClusterMapConfig(new VerifiableProperties(new Properties())));
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = StoreToolsUtil.getVerifiableProperties(args);
    DumpDataTool dumpDataTool = new DumpDataTool(verifiableProperties);
    dumpDataTool.doOperation();
  }

  /**
   * Executes the operation with the help of properties passed
   * @throws IOException
   */
  public void doOperation() throws Exception {
    logger.info("Type of Operation " + typeOfOperation);
    if (fileToRead != null) {
      logger.info("File to read " + fileToRead);
    }
    switch (typeOfOperation) {
      case "CompareIndexToLog":
        compareIndexEntriesToLogContent(new File(fileToRead), false);
        break;
      case "CompareReplicaIndexesToLog":
        compareReplicaIndexEntriestoLogContent(replicaRootDirecotry);
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
    logger.info("Comparing Index entries to Log ");
    File[] indexFiles = new File(replicaRootDirectory).listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    if (indexFiles == null) {
      throw new IllegalStateException("Could not read index files from " + replicaRootDirectory);
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
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    StoreMetrics metrics = new StoreMetrics(indexFile.getParent(), new MetricRegistry());
    IndexSegment segment =
        new IndexSegment(indexFile, false, storeKeyFactory, config, metrics, new Journal(indexFile.getParent(), 0, 0),
            SystemTime.getInstance());
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
      boolean success = readFromLogAndVerify(randomAccessFile, key.getID(), value, coveredRanges);
      if (success) {
        if (isDeleted) {
          long originalOffset = value.getOriginalMessageOffset();
          if (originalOffset != -1) {
            if (!coveredRanges.containsKey(originalOffset)) {
              if (startOffset.getOffset() > originalOffset) {
                logger.trace("Put Record at {} with delete msg offset {} ignored because it is prior to startOffset {}",
                    originalOffset, value.getOffset(), startOffset);
              } else {
                DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo =
                    DumpDataHelper.readSingleRecordFromLog(randomAccessFile, originalOffset, clusterMap);
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
    long offset = indexValue.getOffset().getOffset();
    try {
      DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo =
          DumpDataHelper.readSingleRecordFromLog(randomAccessFile, offset, clusterMap);
      if (coveredRanges != null) {
        coveredRanges.put(offset, offset + logBlobRecordInfo.totalRecordSize);
      }
      compareIndexValueToLogEntry(blobId, indexValue, logBlobRecordInfo);
      if (!logBlobRecordInfo.isDeleted) {
        logger.trace("{}", logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId.getID() + "\n"
            + logBlobRecordInfo.blobProperty + "\n" + logBlobRecordInfo.userMetadata + "\n"
            + logBlobRecordInfo.blobDataOutput);
      } else {
        logger.trace("{}", logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId.getID() + "\n"
            + logBlobRecordInfo.deleteMsg);
      }
      return true;
    } catch (IllegalArgumentException e) {
      logger.error("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", "
          + "while reading blob starting at offset " + offset + " with exception: ", e);
    } catch (MessageFormatException e) {
      logger.error("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position()
          + " while reading blob starting at offset " + offset + " with exception: ", e);
    } catch (EOFException e) {
      logger.error("EOFException thrown at " + randomAccessFile.getChannel().position() + " ", e);
      throw (e);
    } catch (Exception e) {
      logger.error("Unknown exception thrown " + e.getMessage() + " ", e);
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
    boolean isExpired = DumpDataHelper.isExpired(indexValue.getExpiresAtMs());
    if (isDeleted != logBlobRecordInfo.isDeleted) {
      logger.error(
          "Deleted value mismatch for " + logBlobRecordInfo.blobId + " Index value " + isDeleted + ", Log value "
              + logBlobRecordInfo.isDeleted);
    } else if (!logBlobRecordInfo.isDeleted && isExpired != logBlobRecordInfo.isExpired) {
      logger.error(
          "Expiration value mismatch for " + logBlobRecordInfo.blobId + " Index value " + isExpired + ", Log value "
              + logBlobRecordInfo.isExpired + ", index TTL in ms " + indexValue.getExpiresAtMs()
              + ", log Time to live in secs " + logBlobRecordInfo.timeToLiveInSeconds + ", in ms "
              + TimeUnit.SECONDS.toMillis(logBlobRecordInfo.timeToLiveInSeconds));
    } else if (!blobId.equals(logBlobRecordInfo.blobId.getID())) {
      logger.error("BlobId value mismatch for " + logBlobRecordInfo.blobId + " Index value " + blobId + ", Log value "
          + logBlobRecordInfo.blobId);
    }
  }
}
