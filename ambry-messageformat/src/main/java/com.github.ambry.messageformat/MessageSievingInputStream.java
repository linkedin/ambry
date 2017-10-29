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
package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * InputStream that skips invalid blobs based on some validation criteria.
 * For now, the check only supports detection of message corruption
 */
public class MessageSievingInputStream extends InputStream {
  private int validSize;
  private final Logger logger;
  private ByteBuffer byteBuffer;
  private boolean hasInvalidMessages;
  private List<MessageInfo> validMessageInfoList;

  //metrics
  public Histogram messageFormatValidationTime;
  public Histogram messageFormatBatchValidationTime;

  /**
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param metricRegistry Metric register to register metrics
   * @throws java.io.IOException
   */
  public MessageSievingInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, MetricRegistry metricRegistry) throws IOException {
    this.logger = LoggerFactory.getLogger(getClass());
    messageFormatValidationTime =
        metricRegistry.histogram(MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatValidationTime"));
    messageFormatBatchValidationTime = metricRegistry.histogram(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatBatchValidationTime"));
    validSize = 0;
    hasInvalidMessages = false;
    validMessageInfoList = new ArrayList<MessageInfo>();

    // check for empty list
    if (messageInfoList.size() == 0) {
      byteBuffer = ByteBuffer.allocate(0);
      return;
    }

    int totalMessageListSize = 0;
    for (MessageInfo info : messageInfoList) {
      totalMessageListSize += info.getSize();
    }

    int bytesRead = 0;
    byte[] data = new byte[totalMessageListSize];
    long startTime = SystemTime.getInstance().milliseconds();
    logger.trace("Starting to validate message stream ");
    int offset = 0;
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      Utils.readBytesFromStream(stream, data, offset, msgSize);
      logger.trace("Read stream for message info " + msgInfo + "  into memory");
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, offset, msgSize);
      if (checkForMessageValidity(byteArrayInputStream, offset, msgSize, storeKeyFactory, msgInfo)) {
        offset += msgSize;
        validMessageInfoList.add(msgInfo);
      } else {
        logger.error("Error reading the message at " + bytesRead + " with messageInfo " + msgInfo
            + " and hence skipping the message");
        hasInvalidMessages = true;
      }
      bytesRead += msgSize;
    }
    if (bytesRead != totalMessageListSize) {
      logger.error(
          "Failed to read intended size from stream. Expected " + totalMessageListSize + ", actual " + bytesRead);
    }
    if (validMessageInfoList.size() == 0) {
      logger.error("All messages are invalidated in this message stream ");
    }
    messageFormatBatchValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    this.validSize = offset;
    byteBuffer = ByteBuffer.wrap(data, 0, validSize);
    logger.trace("Completed validation of message stream ");
  }

  /**
   * Returns the total size of all valid messages that could be read from the stream
   * @return validSize
   */
  public int getSize() {
    return validSize;
  }

  @Override
  public int read() throws IOException {
    if (!byteBuffer.hasRemaining()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Byte array cannot be null");
    } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) {
      return -1;
    }
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  /**
   * Whether the stream has invalid messages or not
   * @return
   */
  public boolean hasInvalidMessages() {
    return hasInvalidMessages;
  }

  public List<MessageInfo> getValidMessageInfoList() {
    return validMessageInfoList;
  }

  /**
   * Ensures blob validity in the given input stream. For now, blobs are checked for message corruption
   * @param byteArrayInputStream stream against which validation has to be done
   * @param size total size of the message expected
   * @param currentOffset Current offset at which the data has to be read from the given byte array
   * @param storeKeyFactory StoreKeyFactory used to get store key
   * @return true if message is valid and false otherwise
   * @throws IOException
   */
  private boolean checkForMessageValidity(ByteArrayInputStream byteArrayInputStream, int currentOffset, long size,
      StoreKeyFactory storeKeyFactory, MessageInfo msgInfo) throws IOException {
    boolean isValid = false;
    ByteBuffer encryptionKey;
    BlobProperties props;
    ByteBuffer metadata;
    BlobData blobData;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      int availableBeforeParsing = byteArrayInputStream.available();
      byte[] headerVersionInBytes = new byte[Version_Field_Size_In_Bytes];
      byteArrayInputStream.read(headerVersionInBytes, 0, Version_Field_Size_In_Bytes);
      ByteBuffer headerVersion = ByteBuffer.wrap(headerVersionInBytes);
      short version = headerVersion.getShort();
      if (!isValidHeaderVersion(version)) {
        throw new MessageFormatException("Header version not supported " + version,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      ByteBuffer headerBuffer = ByteBuffer.allocate(getHeaderSizeForVersion(version));
      headerBuffer.putShort(version);
      byteArrayInputStream.read(headerBuffer.array(), 2, headerBuffer.capacity() - 2);
      headerBuffer.position(headerBuffer.capacity());
      headerBuffer.flip();
      MessageHeader_Format header = getMessageHeader(version, headerBuffer);
      StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(byteArrayInputStream));
      if (header.isPutRecord()) {
        encryptionKey = header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(byteArrayInputStream) : null;
        props = deserializeBlobProperties(byteArrayInputStream);
        metadata = deserializeUserMetadata(byteArrayInputStream);
        blobData = deserializeBlob(byteArrayInputStream);
      } else {
        throw new IllegalStateException("Message cannot be a deleted record ");
      }
      if (byteArrayInputStream.available() != 0) {
        logger.error("Parsed message size {} is not equivalent to the size in message info {}",
            (availableBeforeParsing + byteArrayInputStream.available()), availableBeforeParsing);
      } else {
        logger.trace("Message Successfully read");
        logger.trace(
            "Header - version {} Message Size {} Starting offset of the blob {} BlobEncryptionKeyRecord {} BlobPropertiesRelativeOffset {}"
                + " UserMetadataRelativeOffset {} DataRelativeOffset {} DeleteRecordRelativeOffset {} Crc {}",
            header.getVersion(), header.getMessageSize(), currentOffset,
            header.getBlobEncryptionKeyRecordRelativeOffset(), header.getBlobPropertiesRecordRelativeOffset(),
            header.getUserMetadataRecordRelativeOffset(), header.getBlobRecordRelativeOffset(),
            header.getDeleteRecordRelativeOffset(), header.getCrc());
        logger.trace("Id {} Encryption Key -size {} Blob Properties - blobSize {} Metadata - size {} Blob - size {} ",
            storeKey.getID(), encryptionKey == null ? 0 : encryptionKey.capacity(), props.getBlobSize(),
            metadata.capacity(), blobData.getSize());
        if (msgInfo.getStoreKey().equals(storeKey)) {
          isValid = true;
        } else {
          logger.error(
              "StoreKey in log " + storeKey + " failed to match store key from Index " + msgInfo.getStoreKey());
        }
      }
    } catch (MessageFormatException e) {
      logger.error(
          "MessageFormat exception thrown for a blob starting at offset " + currentOffset + " with exception: ", e);
    } finally {
      messageFormatValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    return isValid;
  }
}

