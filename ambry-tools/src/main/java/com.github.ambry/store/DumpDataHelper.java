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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to assist in dumping data from index or log files
 */
class DumpDataHelper {

  private final ClusterMap _clusterMap;
  private Throttler throttler;
  private static final Logger logger = LoggerFactory.getLogger(DumpDataHelper.class);

  DumpDataHelper(ClusterMap clusterMap, int bytesPerSec) {
    this._clusterMap = clusterMap;
    if (bytesPerSec > 0) {
      this.throttler = new Throttler(bytesPerSec, 100, true, SystemTime.getInstance());
    } else if (bytesPerSec < 0) {
      throw new IllegalArgumentException("BytesPerSec " + bytesPerSec + " cannot be negative ");
    }
  }

  /**
   * Dumps all blobs in an index file
   * @param indexFileToDump the index file that needs to be parsed
   * @param blobList List of blobIds to be filtered for
   * @param blobIdToMessageMap {@link Map} of BlobId to {@link IndexRecord} to hold the information
   *                                          about blobs in the index after parsing
   * @return the total number of keys/records processed
   * @throws Exception
   */
  long dumpBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList, Map<String, IndexRecord> blobIdToMessageMap)
      throws Exception {
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", _clusterMap);
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    StoreMetrics metrics = new StoreMetrics(indexFileToDump.getParent(), new MetricRegistry());
    IndexSegment segment = new IndexSegment(indexFileToDump, false, storeKeyFactory, config, metrics,
        new Journal(indexFileToDump.getParent(), 0, 0));
    List<MessageInfo> entries = new ArrayList<>();
    segment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
    long numberOfKeysProcessed = 0;
    for (MessageInfo entry : entries) {
      StoreKey key = entry.getStoreKey();
      IndexValue value = segment.find(key);
      boolean isDeleted = value.isFlagSet(IndexValue.Flags.Delete_Index);
      numberOfKeysProcessed++;
      if (blobList == null || blobList.size() == 0 || blobList.contains(key.toString())) {
        IndexRecord indexRecord = new IndexRecord(value.toString(), isDeleted, isExpired(value.getExpiresAtMs()));
        if (blobIdToMessageMap.containsKey(key.getID())) {
          logger.error(
              "Duplicate record found for same blob " + key.getID() + ". Prev record " + blobIdToMessageMap.get(
                  key.getID()) + ", New record " + indexRecord);
        }
        blobIdToMessageMap.put(key.getID(), indexRecord);
      }
    }
    return numberOfKeysProcessed;
  }

  /**
   * Dumps all blobs in a given log file
   * @param file the log file that needs to be parsed for
   * @param startOffset the starting offset from which records needs to be dumped from. Can be {@code null}
   * @param endOffset the end offset until which records need to be dumped to. Can be {@code null}
   * @param blobs List of blobIds to be filtered for
   * @param filter {@code true} if filtering has to be done, {@code false} otherwise
   * @param blobIdToLogRecord {@link HashMap} of blobId to {@link LogBlobRecord} to hold the information about blobs
   *                                         in the log after parsing
   * @param logRecord {@code true} if log record has to be logged, {@code false} otherwise
   * @throws IOException
   */
  void dumpLog(File file, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter,
      Map<String, LogBlobRecord> blobIdToLogRecord, boolean logRecord) throws IOException {
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
        LogBlobRecordInfo logBlobRecordInfo = readSingleRecordFromLog(randomAccessFile, currentOffset);
        if (lastBlobFailed) {
          logger.info("Successful record found at " + currentOffset + " after some failures ");
        }
        lastBlobFailed = false;
        if (!logBlobRecordInfo.isDeleted) {
          if (filter) {
            if (blobs.contains(logBlobRecordInfo.blobId.getID())) {
              if (logRecord) {
                logger.info(logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId + "\n"
                    + logBlobRecordInfo.blobProperty + "\n" + logBlobRecordInfo.userMetadata + "\n"
                    + logBlobRecordInfo.blobDataOutput);
              }
              updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                  !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
            }
          } else {
            if (logRecord) {
              logger.info(logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId + "\n"
                  + logBlobRecordInfo.blobProperty + "\n" + logBlobRecordInfo.userMetadata + "\n"
                  + logBlobRecordInfo.blobDataOutput + " end offset " + (currentOffset
                  + logBlobRecordInfo.totalRecordSize));
              updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                  !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
            }
          }
        } else {
          if (filter) {
            if (blobs.contains(logBlobRecordInfo.blobId.getID())) {
              if (logRecord) {
                logger.info(logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId + "\n"
                    + logBlobRecordInfo.deleteMsg);
              }
              updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                  !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
            }
          } else {
            if (logRecord) {
              logger.info(logBlobRecordInfo.messageHeader + "\n " + logBlobRecordInfo.blobId + "\n"
                  + logBlobRecordInfo.deleteMsg + " end offset " + (currentOffset + logBlobRecordInfo.totalRecordSize));
            }
            updateBlobIdToLogRecordMap(blobIdToLogRecord, logBlobRecordInfo.blobId.getID(), currentOffset,
                !logBlobRecordInfo.isDeleted, logBlobRecordInfo.isExpired);
          }
        }
        currentOffset += (logBlobRecordInfo.totalRecordSize);
      } catch (IllegalArgumentException e) {
        if (!lastBlobFailed) {
          logger.error("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", "
              + "while reading blob starting at offset " + currentOffset + "with exception: " + e.getStackTrace());
        }
        currentOffset++;
        lastBlobFailed = true;
      } catch (MessageFormatException e) {
        if (!lastBlobFailed) {
          logger.error("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position()
              + " while reading blob starting at offset " + currentOffset + " with exception: " + e.getStackTrace());
        }
        currentOffset++;
        lastBlobFailed = true;
      } catch (EOFException e) {
        e.printStackTrace();
        logger.error("EOFException thrown at " + randomAccessFile.getChannel().position() + ", Cause :" + e.getCause()
            + ", Msg :" + e.getMessage() + ", stacktrace " + e.getStackTrace());
        throw (e);
      } catch (Exception e) {
        if (!lastBlobFailed) {
          e.printStackTrace();
          logger.error(
              "Unknown exception thrown with Cause " + e.getCause() + ", Msg :" + e.getMessage() + ", stacktrace "
                  + e.getStackTrace());
          logger.info("Trying out next offset " + (currentOffset + 1));
        }
        currentOffset++;
        lastBlobFailed = true;
      }
    }
    logger.info("Dumped until offset " + currentOffset);
  }

  /**
   * Fetches one blob record from the log
   * @param randomAccessFile {@link RandomAccessFile} referring to the log file
   * @param currentOffset the offset at which to read the record from
   * @return the {@link LogBlobRecordInfo} containing the blob record info
   * @throws IOException
   * @throws MessageFormatException
   */
  LogBlobRecordInfo readSingleRecordFromLog(RandomAccessFile randomAccessFile, long currentOffset)
      throws IOException, MessageFormatException, InterruptedException {
    String messageheader = null;
    BlobId blobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobDataOutput = null;
    String deleteMsg = null;
    boolean isDeleted = false;
    boolean isExpired = false;
    long timeToLiveInSeconds = -1;
    int totalRecordSize = 0;
    randomAccessFile.seek(currentOffset);
    short version = randomAccessFile.readShort();
    if (version == 1) {
      ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
      buffer.putShort(version);
      randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
      buffer.clear();
      MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
      messageheader =
          " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() + " currentOffset "
              + currentOffset + " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset()
              + " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() + " dataRelativeOffset "
              + header.getBlobRecordRelativeOffset() + " crc " + header.getCrc();
      totalRecordSize += header.getMessageSize() + buffer.capacity();
      // read blob id
      InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
      blobId = new BlobId(new DataInputStream(streamlog), _clusterMap);
      totalRecordSize += blobId.sizeInBytes();
      if (header.getBlobPropertiesRecordRelativeOffset()
          != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
        BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
        timeToLiveInSeconds = props.getTimeToLiveInSeconds();
        isExpired = timeToLiveInSeconds != -1 ? isExpired(TimeUnit.SECONDS.toMillis(timeToLiveInSeconds)) : false;
        blobProperty = " Blob properties - blobSize  " + props.getBlobSize() + " serviceId " + props.getServiceId()
            + ", isExpired " + isExpired;
        ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
        usermetadata = " Metadata - size " + metadata.capacity();
        BlobData blobData = MessageFormatRecord.deserializeBlob(streamlog);
        blobDataOutput = "Blob - size " + blobData.getSize();
      } else {
        boolean deleteFlag = MessageFormatRecord.deserializeDeleteRecord(streamlog);
        isDeleted = true;
        deleteMsg = "delete change " + deleteFlag;
      }
    } else {
      throw new MessageFormatException("Header version not supported " + version, MessageFormatErrorCodes.IO_Error);
    }
    if (throttler != null) {
      throttler.maybeThrottle(totalRecordSize);
    }
    return new LogBlobRecordInfo(messageheader, blobId, blobProperty, usermetadata, blobDataOutput, deleteMsg,
        isDeleted, isExpired, timeToLiveInSeconds, totalRecordSize);
  }

  /**
   * Dumps replica token file
   * @param replicaTokenFile the file that needs to be parsed for
   * @throws Exception
   */
  void dumpReplicaToken(File replicaTokenFile) throws Exception {
    logger.info("Dumping replica token");
    DataInputStream stream = new DataInputStream(new FileInputStream(replicaTokenFile));
    short version = stream.readShort();
    switch (version) {
      case 0:
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", _clusterMap);
        FindTokenFactory findTokenFactory =
            Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);
        while (stream.available() > Crc_Size) {
          // read partition id
          PartitionId partitionId = _clusterMap.getPartitionIdFromStream(stream);
          // read remote node host name
          String hostname = Utils.readIntString(stream);
          // read remote replica path
          String replicaPath = Utils.readIntString(stream);
          // read remote port
          int port = stream.readInt();
          // read total bytes read from local store
          long totalBytesReadFromLocalStore = stream.readLong();
          // read replica token
          FindToken token = findTokenFactory.getFindToken(stream);
          logger.info(
              "partitionId " + partitionId + " hostname " + hostname + " replicaPath " + replicaPath + " port " + port
                  + " totalBytesReadFromLocalStore " + totalBytesReadFromLocalStore + " token " + token);
        }
        logger.info("crc " + stream.readLong());
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
  boolean readFromLogAndVerify(RandomAccessFile randomAccessFile, String blobId, IndexValue indexValue,
      Map<Long, Long> coveredRanges) throws Exception {
    long offset = indexValue.getOffset().getOffset();
    try {
      LogBlobRecordInfo logBlobRecordInfo = readSingleRecordFromLog(randomAccessFile, offset);
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
   * @param logBlobRecordInfo the {@link LogBlobRecordInfo} to be used in comparison
   */
  private void compareIndexValueToLogEntry(String blobId, IndexValue indexValue, LogBlobRecordInfo logBlobRecordInfo) {
    boolean isDeleted = indexValue.isFlagSet(IndexValue.Flags.Delete_Index);
    boolean isExpired = isExpired(indexValue.getExpiresAtMs());
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

  /**
   * Updates the {@link Map} of blobIds to {@link LogBlobRecord} with the information about the passed in
   * <code>blobId</code>
   * @param blobIdToLogRecord {@link HashMap} of blobId to {@link LogBlobRecord} that needs to be updated with the
   *                                         information about the blob
   * @param blobId the blobId of the blob that needs to be updated in the {@link Map}
   * @param offset the offset at which the blob record was parsed from in the log file
   * @param putRecord {@code true} if the record is a Put record, {@code false} otherwise (incase of a Delete record)
   * @param isExpired {@code true} if the blob has expired, {@code false} otherwise
   */
  private void updateBlobIdToLogRecordMap(Map<String, LogBlobRecord> blobIdToLogRecord, String blobId, Long offset,
      boolean putRecord, boolean isExpired) {
    if (blobIdToLogRecord != null) {
      if (blobIdToLogRecord.containsKey(blobId)) {
        if (putRecord) {
          blobIdToLogRecord.get(blobId).addPutRecord(offset, isExpired);
        } else {
          blobIdToLogRecord.get(blobId).addDeleteRecord(offset);
        }
      } else {
        blobIdToLogRecord.put(blobId, new LogBlobRecord(offset, putRecord, isExpired));
      }
    }
  }

  /**
   * Returns if the blob has been expired or not based on the time to live value
   * @param timeToLive time in milliseconds referring to the time to live for the blob
   * @return {@code true} if blob has expired, {@code false} otherwise
   */
  private boolean isExpired(Long timeToLive) {
    return timeToLive != Utils.Infinite_Time && SystemTime.getInstance().milliseconds() > timeToLive;
  }

  /**
   * Returns the {@link ClusterMap}
   * @return the {@link ClusterMap}
   */
  ClusterMap getClusterMap() {
    return this._clusterMap;
  }

  /**
   * Holds the information about a single record in the index
   */
  class IndexRecord {
    String message;
    boolean isDeleted;
    boolean isExpired;

    IndexRecord(String msg, boolean isDeleted, boolean isExpired) {
      this.message = msg;
      this.isDeleted = isDeleted;
      this.isExpired = isExpired;
    }

    String getMessage() {
      return message;
    }

    boolean isDeleted() {
      return isDeleted;
    }

    boolean isExpired() {
      return isExpired;
    }

    public String toString() {
      return message + ", isDeleted:" + isDeleted + ", isExpired:" + isExpired;
    }
  }

  /**
   * Holds information about a blob in the log
   */
  class LogBlobRecord {
    List<Long> putMessageOffsets = new ArrayList<>();
    List<Long> deleteMessageOffsets = new ArrayList<>();
    boolean isConsistent;
    boolean isDeleted;
    boolean isExpired;
    boolean duplicatePuts;
    boolean duplicateDeletes;
    boolean putAfterDelete;

    LogBlobRecord(Long offset, boolean putRecord, boolean isExpired) {
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
      stringBuilder.append("LogBlobRecord:");
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
