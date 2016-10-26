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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.IndexValue;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Helper class to assist in dumping data from index or log files
 */
public class DumpDataHelper {

  ClusterMap _clusterMap;
  String outFile;
  FileWriter fileWriter;

  public DumpDataHelper(ClusterMap clusterMap) {
    this._clusterMap = clusterMap;
  }

  /**
   * Initializes the output file
   * @param outFile
   */
  void init(String outFile) {
    try {
      if (outFile != null) {
        this.outFile = outFile;
        fileWriter = new FileWriter(new File(outFile));
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to create File " + this.outFile);
    }
  }

  /**
   * Initializes the output file and the {@link FileWriter} for redirecting the output
   * @param outFile
   * @param fileWriter
   */
  void initializeOutFiles(String outFile, FileWriter fileWriter) {
    this.outFile = outFile;
    this.fileWriter = fileWriter;
  }

  /**
   * Dumps all blobs in an index file
   * @param indexFileToDump the index file that needs to be parsed
   * @param blobList List of blobIds to be filtered for
   * @param blobIdToMessageMap {@link HashMap} of BlobId to {@link IndexRecord} to hold the information
   *                                          about blobs in the index after parsing
   * @param avoidMiscLogging {@code true} if miscellaneous logging should be avoided, {@code false} otherwise
   * @return the total number of keys/records processed
   */
  long dumpBlobsFromIndex(File indexFileToDump, ArrayList<String> blobList,
      ConcurrentHashMap<String, IndexRecord> blobIdToMessageMap, boolean avoidMiscLogging) {
    long numberOfKeysProcessed = 0;
    try {
      DataInputStream stream = new DataInputStream(new FileInputStream(indexFileToDump));
      short version = stream.readShort();
      if (!avoidMiscLogging) {
        logOutput("version " + version);
      }
      if (version == 0) {
        int keysize = stream.readInt();
        int valueSize = stream.readInt();
        long fileEndPointer = stream.readLong();
        if (!avoidMiscLogging) {
          logOutput("key size " + keysize);
          logOutput("value size " + valueSize);
          logOutput("file end pointer " + fileEndPointer);
        }
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", _clusterMap);
        while (stream.available() > Crc_Size) {
          StoreKey key = storeKeyFactory.getStoreKey(stream);
          byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
          stream.read(value);
          IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
          String msg =
              "key " + key + " keySize(in bytes) " + key.sizeInBytes() + " value - offset " + blobValue.getOffset()
                  + " size " + blobValue.getSize() + " Original Message Offset " + blobValue.getOriginalMessageOffset()
                  + " Flag " + blobValue.isFlagSet(IndexValue.Flags.Delete_Index) + " LiveUntil "
                  + blobValue.getTimeToLiveInMs();
          boolean isDeleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
          numberOfKeysProcessed++;

          if (blobList == null || blobList.contains(key.toString())) {
            blobIdToMessageMap.put(key.toString(),
                new IndexRecord(msg, isDeleted, isExpired(blobValue.getTimeToLiveInMs())));
          }
        }
        if (!avoidMiscLogging) {
          logOutput("crc " + stream.readLong());
          logOutput("Total number of keys processed " + numberOfKeysProcessed);
        }
      }
    } catch (IOException ioException) {
      if (!avoidMiscLogging) {
        logOutput("IOException thrown " + ioException);
      }
    } catch (Exception exception) {
      if (!avoidMiscLogging) {
        logOutput("Exception thrown " + exception);
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
   * @param avoidMiscLogging {@code true} if miscellaneous logging has to be avoided. {@code false} otherwise
   * @throws IOException
   */
  public void dumpLog(File file, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter,
      ConcurrentHashMap<String, LogBlobRecord> blobIdToLogRecord, boolean avoidMiscLogging)
      throws IOException {
    logOutput("Dumping log file " + file.getAbsolutePath());
    long currentOffset = 0;
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    long fileSize = file.length();
    boolean lastBlobFailed = false;
    if (startOffset != -1) {
      currentOffset = startOffset;
      randomAccessFile.seek(currentOffset);
    }
    if (endOffset == -1) {
      endOffset = fileSize;
    }
    logOutput("Starting dumping from offset " + currentOffset);
    while (currentOffset < endOffset) {
      long tempCurrentOffset = currentOffset;

      try {
        BlobRecordInfo blobRecordInfo = readSingleRecordFromLog(randomAccessFile, currentOffset);
        if (lastBlobFailed) {
          logOutput("Successful record found at " + currentOffset + " after some failures ");
        }
        lastBlobFailed = false;
        if (!blobRecordInfo.isDeleted) {
          if (filter) {
            if (blobs.contains(blobRecordInfo.blobId.getID())) {
              logOutput(
                  blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId + "\n" + blobRecordInfo.blobProperty
                      + "\n" + blobRecordInfo.usermetadata + "\n" + blobRecordInfo.blobDataOutput);
              updateBlobIdToLogRecordMap(blobIdToLogRecord, blobRecordInfo.blobId.getID(), currentOffset,
                  !blobRecordInfo.isDeleted, blobRecordInfo.isExpired);
            }
          } else {
            logOutput(
                blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId + "\n" + blobRecordInfo.blobProperty + "\n"
                    + blobRecordInfo.usermetadata + "\n" + blobRecordInfo.blobDataOutput);
            updateBlobIdToLogRecordMap(blobIdToLogRecord, blobRecordInfo.blobId.getID(), currentOffset,
                !blobRecordInfo.isDeleted, blobRecordInfo.isExpired);
          }
        } else {
          if (filter) {
            if (blobs.contains(blobRecordInfo.blobId.getID())) {
              logOutput(blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId + "\n" + blobRecordInfo.deleteMsg);
              updateBlobIdToLogRecordMap(blobIdToLogRecord, blobRecordInfo.blobId.getID(), currentOffset,
                  !blobRecordInfo.isDeleted, blobRecordInfo.isExpired);
            }
          } else {
            logOutput(blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId + "\n" + blobRecordInfo.deleteMsg);
            updateBlobIdToLogRecordMap(blobIdToLogRecord, blobRecordInfo.blobId.getID(), currentOffset,
                !blobRecordInfo.isDeleted, blobRecordInfo.isExpired);
          }
        }
        currentOffset += (blobRecordInfo.totalRecordSize);
      } catch (IllegalArgumentException e) {
        if (!lastBlobFailed) {
          logOutput("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", " +
              "while reading blob starting at offset " + tempCurrentOffset + "with exception: " + e);
        }
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
        lastBlobFailed = true;
      } catch (MessageFormatException e) {
        if (!lastBlobFailed) {
          logOutput("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position() +
              " while reading blob starting at offset " + tempCurrentOffset + " with exception: " + e);
        }
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
        lastBlobFailed = true;
      } catch (EOFException e) {
        e.printStackTrace();
        logOutput("EOFException thrown at " + randomAccessFile.getChannel().position() + ", Cause :" + e.getCause()
            + ", Msg :" + e.getMessage());
        throw (e);
      } catch (Exception e) {
        if (!lastBlobFailed) {
          e.printStackTrace();
          logOutput(
              "Unknown exception thrown " + e.getMessage() + "\nTrying from next offset " + (tempCurrentOffset + 1)
                  + ", Cause " + e.getCause() + ", Msg :" + e.getMessage());
        }
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
        lastBlobFailed = true;
      }
    }
    logOutput("Dumped until offset " + currentOffset);
  }

  /**
   * Holds information about a blob record in the log
   */
  class BlobRecordInfo {
    String messageheader = null;
    BlobId blobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobDataOutput = null;
    String deleteMsg = null;
    boolean isDeleted;
    boolean isExpired;
    int totalRecordSize;

    public BlobRecordInfo(String messageheader, BlobId blobId, String blobProperty, String usermetadata,
        String blobDataOutput, String deleteMsg, boolean isDeleted, boolean isExpired, int totalRecordSize) {
      this.messageheader = messageheader;
      this.blobId = blobId;
      this.blobProperty = blobProperty;
      this.usermetadata = usermetadata;
      this.blobDataOutput = blobDataOutput;
      this.deleteMsg = deleteMsg;
      this.isDeleted = isDeleted;
      this.isExpired = isExpired;
      this.totalRecordSize = totalRecordSize;
    }
  }

  /**
   * Fetches one blob record from the log
   * @param randomAccessFile {@link RandomAccessFile} referring to the log file
   * @param currentOffset the offset at which to read the record from
   * @return the {@link BlobRecordInfo} containing the blob record info
   * @throws IOException
   * @throws MessageFormatException
   */
  private BlobRecordInfo readSingleRecordFromLog(RandomAccessFile randomAccessFile, long currentOffset)
      throws IOException, MessageFormatException {
    String messageheader = null;
    BlobId blobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobDataOutput = null;
    String deleteMsg = null;
    boolean isDeleted = false;
    boolean isExpired = false;
    int totalRecordSize = 0;
    short version = randomAccessFile.readShort();
    if (version == 1) {
      ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
      buffer.putShort(version);
      randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
      buffer.clear();
      MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
      messageheader = " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
          " currentOffset " + currentOffset +
          " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset() +
          " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
          " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
          " crc " + header.getCrc();
      totalRecordSize += header.getMessageSize() + buffer.capacity();
      // read blob id
      InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
      blobId = new BlobId(new DataInputStream(streamlog), _clusterMap);
      totalRecordSize += blobId.sizeInBytes();
      if (header.getBlobPropertiesRecordRelativeOffset()
          != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
        BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
        isExpired = isExpired(props.getTimeToLiveInSeconds());
        blobProperty = " Blob properties - blobSize  " + props.getBlobSize() +
            " serviceId " + props.getServiceId() + ", isExpired " + isExpired;
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
    return new BlobRecordInfo(messageheader, blobId, blobProperty, usermetadata, blobDataOutput, deleteMsg, isDeleted,
        isExpired, totalRecordSize);
  }

  /**
   * Dumps replica token file
   * @param replicaTokenFile the file that needs to be parsed for
   * @throws Exception
   */
  public void dumpReplicaToken(File replicaTokenFile)
      throws Exception {
    logOutput("Dumping replica token");
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
          logOutput(
              "partitionId " + partitionId + " hostname " + hostname + " replicaPath " + replicaPath + " port " + port
                  + " totalBytesReadFromLocalStore " + totalBytesReadFromLocalStore + " token " + token);
        }
        logOutput("crc " + stream.readLong());
    }
  }

  /**
   * Dumps a single record from the log at a given offset
   * @param randomAccessFile the {@link RandomAccessFile} referring to log file that needs to be parsed
   * @param offset the offset at which the record needs to be parsed for
   * @param blobId the blobId which that is expected to be matched for the record present at
   *               <code>offset</code>
   * @param avoidMiscLogging {@code true} if miscellaneous logging, {@code false} otherwise
   * @throws IOException
   */
  public boolean readFromLog(RandomAccessFile randomAccessFile, long offset, String blobId, boolean avoidMiscLogging)
      throws Exception {
    try {
      randomAccessFile.seek(offset);
      BlobRecordInfo blobRecordInfo = readSingleRecordFromLog(randomAccessFile, offset);
      if (blobRecordInfo.blobId.getID().compareTo(blobId) != 0) {
        logOutput("BlobId did not match the index value. BlodId from index " + blobId + ", blobid in log "
            + blobRecordInfo.blobId.getID());
      }
      if (!avoidMiscLogging) {
        if (!blobRecordInfo.isDeleted) {
          logOutput(
              blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId.getID() + "\n" + blobRecordInfo.blobProperty
                  + "\n" + blobRecordInfo.usermetadata + "\n" + blobRecordInfo.blobDataOutput);
        } else {
          logOutput(
              blobRecordInfo.messageheader + "\n " + blobRecordInfo.blobId.getID() + "\n" + blobRecordInfo.deleteMsg);
        }
      }
      return true;
    } catch (IllegalArgumentException e) {
      logOutput("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", " +
          "while reading blob starting at offset " + offset + " with exception: " + e);
    } catch (MessageFormatException e) {
      logOutput("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position() +
          " while reading blob starting at offset " + offset + " with exception: " + e);
    } catch (EOFException e) {
      e.printStackTrace();
      logOutput("EOFException thrown at " + randomAccessFile.getChannel().position());
      throw (e);
    } catch (Exception e) {
      e.printStackTrace();
      logOutput("Unknown exception thrown " + e.getMessage());
    }
    return false;
  }

  /**
   * Updates the {@link ConcurrentHashMap} of blobIds to {@link LogBlobRecord} with the information about the passed in <code>blobId</code>
   * @param blobIdToLogRecord {@link HashMap} of blobId to {@link LogBlobRecord} that needs to be updated with the information about the blob
   * @param blobId the blobId of the blob that needs to be updated in the {@link ConcurrentHashMap}
   * @param offset the offset at which the blob record was parsed from in the log file
   * @param putRecord {@code true} if the record is a Put record, {@code false} otherwise (incase of a Delete record)
   * @param isExpired {@code true} if the blob has expired, {@code false} otherwise
   */
  private void updateBlobIdToLogRecordMap(ConcurrentHashMap<String, LogBlobRecord> blobIdToLogRecord, String blobId,
      Long offset, boolean putRecord, boolean isExpired) {
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
   * @param timeToLive time in nano seconds refering to the time to live for the blob
   * @return {@code true} if blob has expired, {@code false} otherwise
   */
  private boolean isExpired(Long timeToLive) {
    return timeToLive != Utils.Infinite_Time && SystemTime.getInstance().milliseconds() > timeToLive;
  }

  /**
   * Log a message to the standard out or to the file as per configs
   * @param msg the message that needs to be logged
   */
  synchronized void logOutput(String msg) {
    try {
      if (fileWriter == null) {
        System.out.println(msg);
      } else {
        fileWriter.write(msg + "\n");
      }
    } catch (IOException e) {
      System.out.println("IOException while trying to write to File");
    }
  }

  /**
   * Returns the {@link ClusterMap}
   * @return the {@link ClusterMap}
   */
  ClusterMap getClusterMap() {
    return this._clusterMap;
  }

  /**
   * Flushes and closes the outfile if need be
   */
  void shutdown() {
    try {
      if (fileWriter != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to close File " + outFile);
    }
  }

  /**
   * Holds the information about a single record in the index
   */
  class IndexRecord {
    String message;
    boolean isDeleted;
    boolean isExpired;

    public IndexRecord(String msg, boolean isDeleted, boolean isExpired) {
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

    public LogBlobRecord(Long offset, boolean putRecord, boolean isExpired) {
      if (putRecord) {
        putMessageOffsets.add(offset);
        this.isExpired = isExpired;
        isConsistent = true;
      } else {
        isConsistent = false;
        isDeleted = true;
        deleteMessageOffsets.add(offset);
      }
    }

    /**
     * Adds information about a put record
     * @param offset the offset at which the put record was found
     * @param isExpired {@code true} if blob is expired, {@code false} otherwise
     */
    public void addPutRecord(Long offset, boolean isExpired) {
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
    public void addDeleteRecord(Long offset) {
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
