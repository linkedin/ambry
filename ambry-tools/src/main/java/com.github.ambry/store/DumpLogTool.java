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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping data from log file in Ambry
 */
public class DumpLogTool {
  private final ClusterMap clusterMap;
  private Throttler throttler = null;
  // Refers to log file that needs to be dumped
  private final String fileToRead;
  // File path referring to the hardware layout
  private final String hardwareLayoutFilePath;
  // File path referring to the partition layout
  private final String partitionLayoutFilePath;
  // List of blobIds (comma separated values) to filter
  private final String blobIdList;
  // The offset in the log to start dumping from
  private final long logStartOffset;
  // The offset in the log until which to dump data
  private final long logEndOffset;
  // The throttling value in bytes per sec
  private final long bytesPerSec;
  // set to true if only error logging is required
  private final boolean silent;
  private final long currentTimeInMs;
  private final StoreToolsMetrics metrics;

  private static final Logger logger = LoggerFactory.getLogger(DumpLogTool.class);

  public DumpLogTool(VerifiableProperties verifiableProperties, StoreToolsMetrics metrics) throws Exception {
    fileToRead = verifiableProperties.getString("file.to.read");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    blobIdList = verifiableProperties.getString("blobId.list", "");
    logStartOffset = verifiableProperties.getLong("log.start.offset", -1);
    logEndOffset = verifiableProperties.getLong("log.end.offset", -1);
    bytesPerSec = verifiableProperties.getLongInRange("bytes.per.sec", 10000, 0, 100000000);
    silent = verifiableProperties.getBoolean("silent", true);
    if (!new File(hardwareLayoutFilePath).exists() || !new File(partitionLayoutFilePath).exists()) {
      throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
    }
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    this.clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            hardwareLayoutFilePath, partitionLayoutFilePath)).getClusterMap();
    if (bytesPerSec > 0) {
      this.throttler = new Throttler(bytesPerSec, 1000, true, SystemTime.getInstance());
    }
    currentTimeInMs = SystemTime.getInstance().milliseconds();
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
      DumpLogTool dumpLogTool = new DumpLogTool(verifiableProperties, metrics);
      dumpLogTool.doOperation();
    } finally {
      if (reporter != null) {
        reporter.stop();
      }
    }
  }

  /**
   * Executes the operation with the help of properties passed
   * @throws IOException
   */
  public void doOperation() throws IOException {
    ArrayList<String> blobs = null;
    String[] blobArray;
    if (!blobIdList.equals("")) {
      blobArray = blobIdList.split(",");
      blobs = new ArrayList<>();
      blobs.addAll(Arrays.asList(blobArray));
      logger.info("Blobs to look out for :: " + blobs);
    }

    if (fileToRead != null && !silent) {
      logger.info("File to read " + fileToRead);
    }
    dumpLog(new File(fileToRead), logStartOffset, logEndOffset, blobs);
  }

  /**
   * Dumps all records in a given log file
   * @param logFile the log file that needs to be parsed for
   * @param startOffset the starting offset from which records needs to be dumped from. Can be {@code null}
   * @param endOffset the end offset until which records need to be dumped to. Can be {@code null}
   * @param blobs List of blobIds to be filtered for. {@code null} if no filtering required
   * @throws IOException
   */
  private void dumpLog(File logFile, long startOffset, long endOffset, ArrayList<String> blobs) throws IOException {
    Map<String, LogBlobStatus> blobIdToLogRecord = new HashMap<>();
    final Timer.Context context = metrics.dumpLogTimeMs.time();
    try {
      dumpLog(logFile, startOffset, endOffset, blobs, blobIdToLogRecord);
      long totalInConsistentBlobs = 0;
      for (String blobId : blobIdToLogRecord.keySet()) {
        LogBlobStatus logBlobStatus = blobIdToLogRecord.get(blobId);
        if (!logBlobStatus.isConsistent) {
          totalInConsistentBlobs++;
          logger.error("Inconsistent blob " + blobId + " " + logBlobStatus);
        }
      }
      logger.info("Total inconsistent blob count " + totalInConsistentBlobs);
    } finally {
      context.stop();
    }
  }

  /**
   * Dumps all blobs in a given log file
   * @param file the log file that needs to be parsed for
   * @param startOffset the starting offset from which records needs to be dumped from. Can be {@code null}
   * @param endOffset the end offset until which records need to be dumped to. Can be {@code null}
   * @param blobs List of blobIds to be filtered for. {@code null} if no filtering required
   * @param blobIdToLogRecord {@link HashMap} of blobId to {@link LogBlobStatus} to hold the information about blobs
   *                                         in the log after parsing
   * @throws IOException
   */
  private void dumpLog(File file, long startOffset, long endOffset, ArrayList<String> blobs,
      Map<String, LogBlobStatus> blobIdToLogRecord) throws IOException {
    logger.info("Dumping log file " + file.getAbsolutePath());
    long currentOffset = 0;
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    long fileSize = file.length();
    boolean lastBlobFailed = false;
    if (startOffset != -1) {
      currentOffset = startOffset;
    }
    if (endOffset == -1) {
      endOffset = fileSize;
    }
    logger.info("Starting dumping from offset " + currentOffset);
    while (currentOffset < endOffset) {
      try {
        DumpDataHelper.LogBlobRecordInfo logBlobRecordInfo =
            DumpDataHelper.readSingleRecordFromLog(randomAccessFile, currentOffset, clusterMap, currentTimeInMs,
                metrics);
        if (throttler != null) {
          throttler.maybeThrottle(logBlobRecordInfo.totalRecordSize);
        }
        if (lastBlobFailed && !silent) {
          logger.info("Successful record found at " + currentOffset + " after some failures ");
        }
        lastBlobFailed = false;
        if (!logBlobRecordInfo.isDeleted) {
          if (blobs != null) {
            if (blobs.contains(logBlobRecordInfo.blobId.getID())) {
              logger.info("{}\n{}\n{}\n{}\n{}\n{}", logBlobRecordInfo.messageHeader, logBlobRecordInfo.blobId,
                  logBlobRecordInfo.blobEncryptionKey, logBlobRecordInfo.blobProperty, logBlobRecordInfo.userMetadata,
                  logBlobRecordInfo.blobDataOutput);
              updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                  !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
            }
          } else if (!silent) {
            logger.info("{}\n{}\n{}\n{}\n{}\n{} end offset {}", logBlobRecordInfo.messageHeader,
                logBlobRecordInfo.blobId, logBlobRecordInfo.blobEncryptionKey, logBlobRecordInfo.blobProperty,
                logBlobRecordInfo.userMetadata, logBlobRecordInfo.blobDataOutput,
                (currentOffset + logBlobRecordInfo.totalRecordSize));
            updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
          }
        } else {
          if (blobs != null) {
            if (blobs.contains(logBlobRecordInfo.blobId.getID())) {
              logger.info("{}\n{}\n{}", logBlobRecordInfo.messageHeader, logBlobRecordInfo.blobId,
                  logBlobRecordInfo.deleteMsg);
              updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                  !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
            }
          } else if (!silent) {
            logger.info("{}\n{}\n{} end offset {}", logBlobRecordInfo.messageHeader, logBlobRecordInfo.blobId,
                logBlobRecordInfo.deleteMsg, (currentOffset + logBlobRecordInfo.totalRecordSize));
            updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
          }
        }
        currentOffset += (logBlobRecordInfo.totalRecordSize);
      } catch (IllegalArgumentException e) {
        if (!lastBlobFailed) {
          metrics.logDeserializationError.inc();
          logger.error("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", "
              + "while reading blob starting at offset " + currentOffset + "with exception: ", e);
        }
        currentOffset++;
        lastBlobFailed = true;
      } catch (MessageFormatException e) {
        if (!lastBlobFailed) {
          metrics.logDeserializationError.inc();
          logger.error("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position()
              + " while reading blob starting at offset " + currentOffset + " with exception: ", e);
        }
        currentOffset++;
        lastBlobFailed = true;
      } catch (EOFException e) {
        metrics.endOfFileOnDumpLogError.inc();
        logger.error("EOFException thrown at " + randomAccessFile.getChannel().position() + ", Cause :" + e.getCause()
            + ", Msg :" + e.getMessage() + ", stacktrace ", e);
        throw (e);
      } catch (Exception e) {
        if (!lastBlobFailed) {
          metrics.unknownErrorOnDumpLog.inc();
          logger.error(
              "Unknown exception thrown with Cause " + e.getCause() + ", Msg :" + e.getMessage() + ", stacktrace ", e);
          if (!silent) {
            logger.info("Trying out next offset " + (currentOffset + 1));
          }
        }
        currentOffset++;
        lastBlobFailed = true;
      }
    }
    logger.info("Dumped until offset " + currentOffset);
  }

  /**
   * Updates the {@link Map} of blobIds to {@link LogBlobStatus} with the information about the passed in
   * <code>blobId</code>
   * @param blobIdToLogRecord {@link HashMap} of blobId to {@link LogBlobStatus} that needs to be updated with the
   *                                         information about the blob
   * @param blobId the blobId of the blob that needs to be updated in the {@link Map}
   * @param offset the offset at which the blob record was parsed from in the log file
   * @param putRecord {@code true} if the record is a Put record, {@code false} otherwise (incase of a Delete record)
   * @param isExpired {@code true} if the blob has expired, {@code false} otherwise
   */
  private void updateBlobIdToLogRecordMap(Map<String, LogBlobStatus> blobIdToLogRecord, String blobId, Long offset,
      boolean putRecord, boolean isExpired) {
    if (blobIdToLogRecord != null) {
      if (blobIdToLogRecord.containsKey(blobId)) {
        if (putRecord) {
          blobIdToLogRecord.get(blobId).addPutRecord(offset, isExpired);
        } else {
          blobIdToLogRecord.get(blobId).addDeleteRecord(offset);
        }
      } else {
        blobIdToLogRecord.put(blobId, new LogBlobStatus(offset, putRecord, isExpired));
      }
    }
  }

  /**
   * Holds information about a blob in the log. If a multiple records are found for the same blob, its captured
   * in the same instance of this class.
   */
  private class LogBlobStatus {
    List<Long> putMessageOffsets = new ArrayList<>();
    List<Long> deleteMessageOffsets = new ArrayList<>();
    boolean isConsistent;
    boolean isDeleted;
    boolean isExpired;
    boolean duplicatePuts;
    boolean duplicateDeletes;
    boolean putAfterDelete;

    private LogBlobStatus(Long offset, boolean putRecord, boolean isExpired) {
      if (putRecord) {
        putMessageOffsets.add(offset);
        this.isExpired = isExpired;
        isConsistent = true;
      } else {
        isConsistent = true;
        isDeleted = true;
        deleteMessageOffsets.add(offset);
      }
    }

    /**
     * Adds information about a put record
     * @param offset the offset at which the put record was found
     * @param isExpired {@code true} if blob is expired, {@code false} otherwise
     */
    void addPutRecord(Long offset, boolean isExpired) {
      isConsistent = false;
      putMessageOffsets.add(offset);
      this.isExpired = isExpired;
      if (putMessageOffsets.size() > 1) {
        duplicatePuts = true;
      } else if (deleteMessageOffsets.size() > 0) {
        putAfterDelete = true;
      }
    }

    /**
     * Adds information about a delete record
     * @param offset the offset at which the delete record was found
     */
    void addDeleteRecord(Long offset) {
      isDeleted = true;
      deleteMessageOffsets.add(offset);
      if (putMessageOffsets.size() == 1 && deleteMessageOffsets.size() == 1) {
        isConsistent = true;
      } else if (deleteMessageOffsets.size() > 1) {
        isConsistent = false;
        duplicateDeletes = true;
      }
    }

    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("LogBlobStatus:");
      stringBuilder.append("[isConsistent=" + isConsistent + "],");
      stringBuilder.append("[isExpired=" + isExpired + "],");
      stringBuilder.append("[isDeleted=" + isDeleted + "],");
      if (putMessageOffsets.size() > 0) {
        stringBuilder.append("[PutMessageOffsets={");
        for (long putOffset : putMessageOffsets) {
          stringBuilder.append(putOffset + ",");
        }
        stringBuilder.append("}");
      }
      if (deleteMessageOffsets.size() > 0) {
        stringBuilder.append("[DeleteMessageOffsets={");
        for (long deleteOffset : deleteMessageOffsets) {
          stringBuilder.append(deleteOffset + ",");
        }
        stringBuilder.append("}");
      }
      stringBuilder.append("[DuplicatePuts=" + duplicatePuts + "],");
      stringBuilder.append("[DuplicateDeletes=" + duplicateDeletes + "],");
      stringBuilder.append("[PutAfterDeletes=" + putAfterDelete + "]");
      return stringBuilder.toString();
    }
  }
}
