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
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
  private boolean hasDeprecatedMessages;
  private List<MessageInfo> sievedMessageInfoList;
  private final List<Transformer> transformers = new ArrayList<>();

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
    hasDeprecatedMessages = false;
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
      transformers.add(new ValidatingKeyConvertingTransformer(storeKeyFactory, true, convertedMap));
    } else {
      transformers.add(new ValidatingKeyConvertingTransformer(storeKeyFactory, false, null));
    }

    int bytesRead = 0;
    ByteArrayOutputStream sievedOutputStream = new ByteArrayOutputStream(totalMessageListSize);
    long startTime = SystemTime.getInstance().milliseconds();
    logger.trace("Starting to validate message stream ");
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      Message msg = new Message(msgInfo, Utils.readBytesFromStream(inStream, msgSize));
      logger.trace("Read stream for message info " + msgInfo + "  into memory");

      try {
        sieve(msg, sievedOutputStream, bytesRead);
      } catch (Exception e) {
        throw new IllegalStateException("Caught exception sieving messages", e);
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
    byteBuffer = ByteBuffer.wrap(sievedOutputStream.toByteArray());
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

  /**
   * @return Whether this stream had messages that were deprecated as part of the sieving.
   */
  boolean hasDeprecatedMessages() {
    return hasDeprecatedMessages;
  }

  public List<MessageInfo> getValidMessageInfoList() {
    return sievedMessageInfoList;
  }

  /**
   * Validates and potentially transforms the given input stream consisting of message data. It does so using the list
   * of {@link Transformer}s associated with this instance.
   * message corruption and acceptable formats.
   * @param inMsg the original {@link Message} that needs to be validated and possibly transformed.
   * @param sievedOutputStream the output {@link OutputStream}
   * @param msgOffset the offset of the message in the stream.
   * @throws Exception if an exception was encountered by any of the transformers.
   */
  private void sieve(Message inMsg, OutputStream sievedOutputStream, int msgOffset) throws Exception {
    if (transformers.isEmpty()) {
      return;
    }
    Message msg = inMsg;
    TransformationOutput output = null;
    for (Transformer transformer : transformers) {
      output = transformer.transform(msg);
      if (output.getException() != null || output.getMsg() == null) {
        break;
      } else {
        msg = output.getMsg();
      }
    }
    if (output.getException() != null) {
      logger.error("Error validating/transforming the message at {} with messageInfo {} and hence skipping the message",
          msgOffset, inMsg.getMessageInfo());
      hasInvalidMessages = true;
      messageSievingCorruptMessagesDiscardedCount.inc();
    } else if (output.getMsg() == null) {
      logger.trace("Transformation is on, and the message with id {} does not have a replacement and was discarded.",
          inMsg.getMessageInfo().getStoreKey());
      hasDeprecatedMessages = true;
      messageSievingDeprecatedMessagesDiscardedCount.inc();
    } else {
      MessageInfo tfmMsgInfo = output.getMsg().getMessageInfo();
      byte[] tfmMsgBytes = output.getMsg().getBytes();
      sievedMessageInfoList.add(tfmMsgInfo);
      sievedOutputStream.write(tfmMsgBytes);
      logger.trace("Original message length {}, transformed bytes read {}", inMsg.getMessageInfo().getSize(),
          (int) tfmMsgInfo.getSize());
    }
  }

  class ValidatingKeyConvertingTransformer implements Transformer {
    private final StoreKeyFactory storeKeyFactory;
    private final boolean conversionOn;
    private final Map<StoreKey, StoreKey> conversionMap;

    public ValidatingKeyConvertingTransformer(StoreKeyFactory storeKeyFactory, boolean conversionOn,
        Map<StoreKey, StoreKey> conversionMap) {
      this.storeKeyFactory = storeKeyFactory;
      this.conversionOn = conversionOn;
      this.conversionMap = conversionMap;
    }

    @Override
    public TransformationOutput transform(Message message) throws Exception {
      ByteBuffer encryptionKey;
      BlobProperties props;
      ByteBuffer metadata;
      BlobData blobData;
      MessageInfo msgInfo = message.getMessageInfo();
      byte[] msg = message.getBytes();
      TransformationOutput transformationOutput = null;
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
          logger.error("{} bytes remaining after parsing message, the size in message info is {}",
              msgStream.available(), msg.length);
        } else {
          logger.trace("Message Successfully read");
          logger.trace("Header - version {} Message Size {} BlobEncryptionKeyRecord {} BlobPropertiesRelativeOffset {}"
                  + " UserMetadataRelativeOffset {} DataRelativeOffset {} UpdateRecordRelativeOffset {} Crc {}",
              header.getVersion(), header.getMessageSize(), header.getBlobEncryptionKeyRecordRelativeOffset(),
              header.getBlobPropertiesRecordRelativeOffset(), header.getUserMetadataRecordRelativeOffset(),
              header.getBlobRecordRelativeOffset(), header.getUpdateRecordRelativeOffset(), header.getCrc());
          if (msgInfo.getStoreKey().equals(originalKey)) {
            StoreKey newKey = conversionOn ? conversionMap.get(originalKey) : originalKey;
            if (newKey == null) {
              logger.trace("No mapping for the given key, transformed message will be null");
              transformationOutput = new TransformationOutput(null, null);
            } else {
              MessageInfo transformedMsgInfo;
              PutMessageFormatInputStream transformedStream =
                  new PutMessageFormatInputStream(newKey, encryptionKey, props, metadata, blobData.getStream(),
                      blobData.getSize(), blobData.getBlobType());
              logger.trace(
                  "Original Id {} Replacement Id {} Encryption Key -size {} Blob Properties - blobSize {} Metadata - size {} Blob - size {} ",
                  originalKey.getID(), newKey.getID(), encryptionKey == null ? 0 : encryptionKey.capacity(),
                  props.getBlobSize(), metadata.capacity(), blobData.getSize());
              if (!originalKey.equals(newKey)) {
                logger.trace("Replacing original id {} with replacement id {}", originalKey.getID(), newKey.getID());
                messageSievingConvertedMessagesCount.inc();
                transformedMsgInfo =
                    new MessageInfo(newKey, transformedStream.getSize(), msgInfo.isDeleted(), msgInfo.isTtlUpdated(),
                        msgInfo.getExpirationTimeInMs(), msgInfo.getCrc(), msgInfo.getAccountId(),
                        msgInfo.getContainerId(), msgInfo.getOperationTimeMs());
              } else {
                transformedMsgInfo = msgInfo;
              }
              byte[] transformedBytes = Utils.readBytesFromStream(transformedStream, (int) transformedStream.getSize());
              transformationOutput = new TransformationOutput(null, new Message(transformedMsgInfo, transformedBytes));
            }
          } else {
            throw new IllegalStateException(
                "StoreKey in log " + originalKey + " failed to match store key from Index " + msgInfo.getStoreKey());
          }
        }
      } catch (MessageFormatException e) {
        logger.error("Exception thrown for blob {} with exception {}", msgInfo.getStoreKey().getID(), e);
        transformationOutput = new TransformationOutput(e, null);
      } finally {
        messageFormatValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
      }
      return transformationOutput;
    }
  }
}

