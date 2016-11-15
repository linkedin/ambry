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
package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Transmission used to speak plain text to the underlying channel.
 */
public class PlainTextTransmission extends Transmission {
  private static final Logger logger = LoggerFactory.getLogger(PlainTextTransmission.class);

  public PlainTextTransmission(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
      NetworkMetrics metrics) {
    super(connectionId, socketChannel, key, time, metrics);
  }

  /**
   * Prepare is a no op for Plaintext
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
  }

  /**
   * Plain text channel is always ready to accept read and write calls
   * @return true
   */
  @Override
  public boolean ready() {
    return true;
  }

  /**
   * Reads a sequence of bytes from the channel into the {@link NetworkReceive}
   *
   * @return true if the read is complete, false otherwise
   * @throws IOException if some other I/O error occurs
   */
  @Override
  public boolean read() throws IOException {
    if (!hasReceive()) {
      networkReceive = new NetworkReceive(getConnectionId(), new BoundedByteBufferReceive(), time);
    }
    long startTimeMs = SystemTime.getInstance().milliseconds();
    long bytesRead = networkReceive.getReceivedBytes().readFrom(socketChannel);
    long readTimeMs = SystemTime.getInstance().milliseconds() - startTimeMs;
    logger.trace("Bytes read " + bytesRead + " from {} using key {} Time: {}",
        socketChannel.socket().getRemoteSocketAddress(), getConnectionId(), readTimeMs);
    if (bytesRead > 0) {
      metrics.plaintextReceiveTimePerKB.update(readTimeMs * 1024 / bytesRead);
    }
    return networkReceive.getReceivedBytes().isReadComplete();
  }

  /**
   * Writes a sequence of bytes to the channel from the payload in {@link NetworkSend}
   *
   * @return true if {@link Send} in {@link NetworkSend} is completely written to the channel, false otherwise
   * @throws IOException If some other I/O error occurs
   */
  @Override
  public boolean write() throws IOException {
    Send send = networkSend.getPayload();
    if (send == null) {
      throw new IllegalStateException("Registered for write interest but no response attached to key.");
    }
    long startTimeMs = SystemTime.getInstance().milliseconds();
    long bytesWritten = send.writeTo(socketChannel);
    long writeTimeMs = SystemTime.getInstance().milliseconds() - startTimeMs;
    logger.trace("Bytes written {} to {} using key {} Time: {}", bytesWritten,
        socketChannel.socket().getRemoteSocketAddress(), getConnectionId(), writeTimeMs);
    if (bytesWritten > 0) {
      metrics.plaintextSendTimePerKB.update(writeTimeMs * 1024 / bytesWritten);
    }
    return send.isSendComplete();
  }

  /**
   * Close the connection for the socket channel
   */
  @Override
  public void close() {
    clearReceive();
    clearSend();
    key.attach(null);
    key.cancel();
    try {
      socketChannel.socket().close();
      socketChannel.close();
    } catch (IOException e) {
      metrics.selectorCloseSocketErrorCount.inc();
      logger.error("Exception closing connection to node {}:", getConnectionId(), e);
    }
  }

  /**
   * Actions to be taken on completion of {@link Send} in {@link NetworkSend}
   */
  @Override
  public void onSendComplete() {
    long sendTimeMs = SystemTime.getInstance().milliseconds() - networkSend.getSendStartTimeInMs();
    networkSend.onSendComplete();
    double sendBytesRate = networkSend.getPayload().sizeInBytes() / ((double) sendTimeMs / SystemTime.MsPerSec);
    metrics.plaintextSendBytesRate.mark((long) sendBytesRate);
  }

  /**
   * Actions to be taken on completion of {@link BoundedByteBufferReceive} in {@link NetworkReceive}
   */
  @Override
  public void onReceiveComplete() {
    long receiveTimeMs = SystemTime.getInstance().milliseconds() - networkReceive.getReceiveStartTimeInMs();
    double receiveBytesRate =
        networkReceive.getReceivedBytes().sizeRead() / ((double) receiveTimeMs / SystemTime.MsPerSec);
    metrics.plaintextReceiveBytesRate.mark((long) receiveBytesRate);
  }
}
