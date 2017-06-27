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

import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;


/**
 * Helper class to assist in dumping a single blob record from a log file
 */
class DumpDataHelper {

  /**
   * Fetches one blob record from the log
   * @param randomAccessFile {@link RandomAccessFile} referring to the log file
   * @param currentOffset the offset at which to read the record from
   * @param clusterMap the {@link ClusterMap} object to use to generate BlobId
   * @param currentTimeInMs current time in ms to determine expiration
   * @param metrics {@link StoreToolsMetrics} instance
   * @return the {@link LogBlobRecordInfo} containing the blob record info
   * @throws IOException
   * @throws MessageFormatException
   */
  static LogBlobRecordInfo readSingleRecordFromLog(RandomAccessFile randomAccessFile, long currentOffset,
      ClusterMap clusterMap, long currentTimeInMs, StoreToolsMetrics metrics)
      throws IOException, MessageFormatException {
    String messageheader = null;
    BlobId blobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobDataOutput = null;
    String deleteMsg = null;
    boolean isDeleted = false;
    boolean isExpired = false;
    long expiresAtMs = -1;
    int totalRecordSize = 0;
    final Timer.Context context = metrics.readSingleBlobRecordFromLogTimeMs.time();
    try {
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
        blobId = new BlobId(new DataInputStream(streamlog), clusterMap);
        totalRecordSize += blobId.sizeInBytes();
        if (header.getBlobPropertiesRecordRelativeOffset()
            != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
          BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
          expiresAtMs = Utils.addSecondsToEpochTime(props.getCreationTimeInMs(), props.getTimeToLiveInSeconds());
          isExpired = isExpired(expiresAtMs, currentTimeInMs);
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
      return new LogBlobRecordInfo(messageheader, blobId, blobProperty, usermetadata, blobDataOutput, deleteMsg,
          isDeleted, isExpired, expiresAtMs, totalRecordSize);
    } finally {
      context.stop();
    }
  }

  /**
   * Holds information about a single blob record in the log
   */
  static class LogBlobRecordInfo {
    final String messageHeader;
    final BlobId blobId;
    final String blobProperty;
    final String userMetadata;
    final String blobDataOutput;
    final String deleteMsg;
    final boolean isDeleted;
    final boolean isExpired;
    final long expiresAtMs;
    final int totalRecordSize;

    LogBlobRecordInfo(String messageHeader, BlobId blobId, String blobProperty, String userMetadata,
        String blobDataOutput, String deleteMsg, boolean isDeleted, boolean isExpired, long expiresAtMs,
        int totalRecordSize) {
      this.messageHeader = messageHeader;
      this.blobId = blobId;
      this.blobProperty = blobProperty;
      this.userMetadata = userMetadata;
      this.blobDataOutput = blobDataOutput;
      this.deleteMsg = deleteMsg;
      this.isDeleted = isDeleted;
      this.isExpired = isExpired;
      this.expiresAtMs = expiresAtMs;
      this.totalRecordSize = totalRecordSize;
    }
  }

  /**
   * Returns if the blob has been expired or not, based on {@code expiresAtMs}.
   * @param expiresAtMs time in milliseconds referring to the time at which the blob expires.
   * @param currentTimeInMs current time in ms to determine expiration
   * @return {@code true} if blob has expired, {@code false} otherwise
   */
  static boolean isExpired(Long expiresAtMs, long currentTimeInMs) {
    return expiresAtMs != Utils.Infinite_Time && currentTimeInMs > expiresAtMs;
  }
}
