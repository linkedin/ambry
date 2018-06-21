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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * InputStream that skips invalid blobs based on some validation criteria.
 * For now, the check only supports detection of message corruption
 */
public class MessageSievingInputStream extends InputStream {
  private int sievedStreamSize;
  private final Logger logger;
  private ByteBuffer byteBuffer;
  private boolean hasInvalidMessages;
  private List<MessageInfo> sievedMessageInfoList;

  //metrics
  public final Histogram messageFormatValidationTime;
  public final Histogram messageFormatBatchValidationTime;
  public final Counter messageSievingCorruptMessagesDiscardedCount;
  public final Counter messageSievingDeprecatedMessagesDiscardedCount;
  public final Counter messageSievingConvertedMessagesCount;

  /**
   * @param inStream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param storeKeyConverter the converter that, if non-null, is used to transform messages.
   * @param metricRegistry Metric register to register metrics
   * @throws java.io.IOException
   */
  public MessageSievingInputStream(InputStream inStream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, StoreKeyConverter storeKeyConverter, MetricRegistry metricRegistry)
      throws IOException {
    this.logger = LoggerFactory.getLogger(getClass());
    messageFormatValidationTime =
        metricRegistry.histogram(MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatValidationTime"));
    messageFormatBatchValidationTime = metricRegistry.histogram(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatBatchValidationTime"));
    messageSievingCorruptMessagesDiscardedCount = metricRegistry.counter(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageSievingCorruptMessagesDiscardedCount"));
    messageSievingDeprecatedMessagesDiscardedCount = metricRegistry.counter(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageSievingDeprecatedMessagesDiscardedCount"));
    messageSievingConvertedMessagesCount = metricRegistry.counter(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageSievingConvertedMessagesCount"));
    sievedStreamSize = 0;
    hasInvalidMessages = false;
    sievedMessageInfoList = new ArrayList<>();

    // check for empty list
    if (messageInfoList.size() == 0) {
      byteBuffer = ByteBuffer.allocate(0);
      return;
    }

    List<StoreKey> keyList = new ArrayList<>();
    Map<StoreKey, StoreKey> convertedMap = null;
    int totalMessageListSize = 0;
    for (MessageInfo info : messageInfoList) {
      totalMessageListSize += info.getSize();
      keyList.add(info.getStoreKey());
    }

    if (storeKeyConverter != null) {
      try {
        convertedMap = storeKeyConverter.convert(keyList);
      } catch (Exception e) {
        throw new IOException("StoreKey conversion encountered an exception", e);
      }
    }

    int bytesRead = 0;
    ByteArrayOutputStream sievedStream = new ByteArrayOutputStream(totalMessageListSize);
    long startTime = SystemTime.getInstance().milliseconds();
    logger.trace("Starting to validate message stream ");
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      byte[] msg = Utils.readBytesFromStream(inStream, msgSize);
      logger.trace("Read stream for message info " + msgInfo + "  into memory");
      StoreKey transformedKey =
          storeKeyConverter != null ? convertedMap.get(msgInfo.getStoreKey()) : msgInfo.getStoreKey();
      if (transformedKey != null) {
        PutMessageFormatInputStream transformedStream =
            validateAndTransform(msg, msgInfo, storeKeyFactory, transformedKey);
        if (transformedStream != null) {
          sievedMessageInfoList.add(
              new MessageInfo(transformedKey, transformedStream.getSize(), msgInfo.isDeleted(), msgInfo.isTtlUpdated(),
                  msgInfo.getExpirationTimeInMs(), msgInfo.getCrc(), msgInfo.getAccountId(), msgInfo.getContainerId(),
                  msgInfo.getOperationTimeMs()));
          Utils.transferBytes(transformedStream, sievedStream, (int) transformedStream.getSize());
          logger.trace("Original message length {}, transformed bytes read {}", msgSize,
              (int) transformedStream.getSize());
        } else {
          logger.error("Error reading the message at " + bytesRead + " with messageInfo " + msgInfo
              + " and hence skipping the message");
          hasInvalidMessages = true;
        }
      } else {
        logger.trace("Transformation is on, and the given message with id {} does not have a replacement. Discarding.",
            msgInfo.getStoreKey());
        messageSievingDeprecatedMessagesDiscardedCount.inc();
        hasInvalidMessages = true;
      }
      bytesRead += msgSize;
    }
    if (bytesRead != totalMessageListSize) {
      logger.error(
          "Failed to read intended size from stream. Expected " + totalMessageListSize + ", actual " + bytesRead);
    }
    if (sievedMessageInfoList.size() == 0) {
      logger.error("All messages are invalidated in this message stream ");
    }
    messageFormatBatchValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    byteBuffer = ByteBuffer.wrap(sievedStream.toByteArray());
    this.sievedStreamSize = byteBuffer.remaining();
    logger.trace("Completed validation of message stream ");
  }

  /**
   * Returns the total size of all valid messages that could be read from the stream
   * @return sievedStreamSize
   */
  public int getSize() {
    return sievedStreamSize;
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
    return sievedMessageInfoList;
  }

  /**
   * Validates and potentially transforms the given input stream consisting of message data. Blobs are validated for
   * message corruption and acceptable formats.
   * @param msg byte array containing the message that needs to be validated and possibly transformed.
   * @param msgInfo the {@link MessageInfo} associated with the message.
   * @param storeKeyFactory StoreKeyFactory used to get store key
   * @param replacementKey the converted {@link StoreKey} to replace the one in msgInfo with. This can also be the same
   *                       key as the original (if no transformation is required).
   * @return {@link PutMessageFormatInputStream} a validated and possibly transformed message stream, if message is
   *         valid and successfully transformed; null otherwise
   * @throws IOException
   */
  private PutMessageFormatInputStream validateAndTransform(byte[] msg, MessageInfo msgInfo,
      StoreKeyFactory storeKeyFactory, StoreKey replacementKey) throws IOException {
    PutMessageFormatInputStream transformedStream = null;
    ByteBuffer encryptionKey;
    BlobProperties props;
    ByteBuffer metadata;
    BlobData blobData;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      // Read header
      ByteBuffer headerVersion = ByteBuffer.wrap(msg, 0, Version_Field_Size_In_Bytes);
      short version = headerVersion.getShort();
      if (!isValidHeaderVersion(version)) {
        throw new MessageFormatException("Header version not supported " + version,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      int headerSize = getHeaderSizeForVersion(version);
      ByteBuffer headerBuffer = ByteBuffer.wrap(msg, 0, headerSize);
      MessageHeader_Format header = getMessageHeader(version, headerBuffer);
      ByteArrayInputStream msgStream = new ByteArrayInputStream(msg, headerSize, msg.length - headerSize);
      StoreKey originalKey = storeKeyFactory.getStoreKey(new DataInputStream(msgStream));
      if (header.isPutRecord()) {
        encryptionKey = header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(msgStream) : null;
        props = deserializeBlobProperties(msgStream);
        metadata = deserializeUserMetadata(msgStream);
        blobData = deserializeBlob(msgStream);
      } else {
        throw new IllegalStateException("Message cannot be a deleted record ");
      }
      if (msgStream.available() != 0) {
        logger.error("{} bytes remaining after parsing message, the size in message info is {}", msgStream.available(),
            msg.length);
      } else {
        logger.trace("Message Successfully read");
        logger.trace("Header - version {} Message Size {} BlobEncryptionKeyRecord {} BlobPropertiesRelativeOffset {}"
                + " UserMetadataRelativeOffset {} DataRelativeOffset {} UpdateRecordRelativeOffset {} Crc {}",
            header.getVersion(), header.getMessageSize(), header.getBlobEncryptionKeyRecordRelativeOffset(),
            header.getBlobPropertiesRecordRelativeOffset(), header.getUserMetadataRecordRelativeOffset(),
            header.getBlobRecordRelativeOffset(), header.getUpdateRecordRelativeOffset(), header.getCrc());
        logger.trace(
            "Original Id {} Replacement Id {} Encryption Key -size {} Blob Properties - blobSize {} Metadata - size {} Blob - size {} ",
            originalKey.getID(), replacementKey.getID(), encryptionKey == null ? 0 : encryptionKey.capacity(),
            props.getBlobSize(), metadata.capacity(), blobData.getSize());
        if (msgInfo.getStoreKey().equals(originalKey)) {
          if (!originalKey.equals(replacementKey)) {
            logger.trace("Replacing original id {} with replacement id {}", originalKey.getID(),
                replacementKey.getID());
            messageSievingConvertedMessagesCount.inc();
          }
          transformedStream =
              new PutMessageFormatInputStream(replacementKey, encryptionKey, props, metadata, blobData.getStream(),
                  blobData.getSize(), blobData.getBlobType());
        } else {
          logger.error("StoreKey in log {} failed to match store key from Index {}", originalKey,
              msgInfo.getStoreKey());
        }
      }
    } catch (MessageFormatException e) {
      logger.error("MessageFormat exception thrown for blob {} with exception {}", msgInfo.getStoreKey().getID(), e);
      messageSievingCorruptMessagesDiscardedCount.inc();
    } finally {
      messageFormatValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    return transformedStream;
  }
}

