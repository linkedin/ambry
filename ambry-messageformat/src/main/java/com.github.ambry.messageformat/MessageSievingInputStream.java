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
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private final List<Transformer> transformers;

  //metrics
  public final Histogram singleMessageSieveTime;
  public final Histogram batchMessageSieveTime;
  public final Counter messageSievingCorruptMessagesDiscardedCount;
  public final Counter messageSievingDeprecatedMessagesDiscardedCount;

  /**
   * @param inStream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param metricRegistry Metric register to register metrics
   * @throws java.io.IOException
   */
  public MessageSievingInputStream(InputStream inStream, List<MessageInfo> messageInfoList,
      List<Transformer> transformers, MetricRegistry metricRegistry) throws IOException {
    this.logger = LoggerFactory.getLogger(getClass());
    this.transformers = transformers;
    singleMessageSieveTime =
        metricRegistry.histogram(MetricRegistry.name(MessageSievingInputStream.class, "SingleMessageSieveTime"));
    batchMessageSieveTime =
        metricRegistry.histogram(MetricRegistry.name(MessageSievingInputStream.class, "BatchMessageSieveTime"));
    messageSievingCorruptMessagesDiscardedCount = metricRegistry.counter(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageSievingCorruptMessagesDiscardedCount"));
    messageSievingDeprecatedMessagesDiscardedCount = metricRegistry.counter(
        MetricRegistry.name(MessageSievingInputStream.class, "MessageSievingDeprecatedMessagesDiscardedCount"));
    sievedStreamSize = 0;
    hasInvalidMessages = false;
    hasDeprecatedMessages = false;
    sievedMessageInfoList = new ArrayList<>();

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
    ByteArrayOutputStream sievedOutputStream = new ByteArrayOutputStream(totalMessageListSize);
    long batchStartTime = SystemTime.getInstance().milliseconds();
    logger.trace("Starting to validate message stream ");
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      // Read the entire message to create an InputStream for just this message. This is to isolate the message
      // from the batched stream, as well as to ensure that subsequent messages can be correctly processed even if there
      // was an error during the sieving for this message.
      Message msg = new Message(msgInfo, new ByteArrayInputStream(Utils.readBytesFromStream(inStream, msgSize)));
      logger.trace("Read stream for message info " + msgInfo + "  into memory");
      sieve(msg, sievedOutputStream, bytesRead);
      bytesRead += msgSize;
    }
    if (bytesRead != totalMessageListSize) {
      logger.error(
          "Failed to read intended size from stream. Expected " + totalMessageListSize + ", actual " + bytesRead);
    }
    if (sievedMessageInfoList.size() == 0) {
      logger.error("All messages are invalidated in this message stream ");
    }
    batchMessageSieveTime.update(SystemTime.getInstance().milliseconds() - batchStartTime);
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
   * @throws IOException if an exception was encountered reading or writing bytes to/from streams.
   */
  private void sieve(Message inMsg, OutputStream sievedOutputStream, int msgOffset) throws IOException {
    if (transformers == null || transformers.isEmpty()) {
      // Write the message without any transformations.
      sievedMessageInfoList.add(inMsg.getMessageInfo());
      Utils.transferBytes(inMsg.getStream(), sievedOutputStream, inMsg.getMessageInfo().getSize());
    } else {
      long sieveStartTime = SystemTime.getInstance().milliseconds();
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
        if (output.getException() instanceof MessageFormatException) {
          logger.error(
              "Error validating/transforming the message at {} with messageInfo {} and hence skipping the message",
              msgOffset, inMsg.getMessageInfo());
          hasInvalidMessages = true;
          messageSievingCorruptMessagesDiscardedCount.inc();
        } else {
          throw new IOException("Encountered exception during transformation", output.getException());
        }
      } else if (output.getMsg() == null) {
        logger.trace("Transformation is on, and the message with id {} does not have a replacement and was discarded.",
            inMsg.getMessageInfo().getStoreKey());
        hasDeprecatedMessages = true;
        messageSievingDeprecatedMessagesDiscardedCount.inc();
      } else {
        MessageInfo tfmMsgInfo = output.getMsg().getMessageInfo();
        sievedMessageInfoList.add(tfmMsgInfo);
        Utils.transferBytes(output.getMsg().getStream(), sievedOutputStream, tfmMsgInfo.getSize());
        logger.trace("Original message length {}, transformed bytes read {}", inMsg.getMessageInfo().getSize(),
            tfmMsgInfo.getSize());
      }
      singleMessageSieveTime.update(SystemTime.getInstance().milliseconds() - sieveStartTime);
    }
  }
}

