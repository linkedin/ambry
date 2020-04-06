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

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;


/**
 * Defines the interface for channel interactions. Once the connection is established, {@link Selector} assigns
 * a {@link Transmission} object as attachment for a key. All operations to the channel like read, write, close, etc
 * happens via this class. This class is also responsible for exposing the characteristics of the underlying channel
 * like ready, isConnected and so on.
 */
public abstract class Transmission {

  private String connectionId;
  protected NetworkSend networkSend = null;
  protected NetworkReceive networkReceive = null;
  protected SocketChannel socketChannel = null;
  protected SelectionKey key = null;
  protected final Time time;
  protected final NetworkMetrics metrics;
  protected long sendCompleteTime;
  protected NetworkConfig config;

  public Transmission(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
      NetworkConfig config, NetworkMetrics metrics) {
    this.connectionId = connectionId;
    this.socketChannel = socketChannel;
    this.key = key;
    this.time = time;
    this.config = config;
    this.metrics = metrics;
  }

  /**
   * Actions taken as part of finishing a connection initiation. This will be called by client when server accepting
   * its connection request.
   * @throws IOException
   */
  public void finishConnect() throws IOException {
    socketChannel.finishConnect();
    key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
  }

  /**
   * Setting network send to be written to the underlying channel asynchronously
   * @param networkSend
   */
  public void setNetworkSend(NetworkSend networkSend) {
    if (hasSend()) {
      throw new IllegalStateException(
          "Attempt to begin a networkSend operation with prior networkSend operation still in progress.");
    }
    this.networkSend = networkSend;
    metrics.sendInFlight.inc();
    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
  }

  protected void initializeNetworkReceive() {
    networkReceive = new NetworkReceive(getConnectionId(), new BoundedNettyByteBufReceive(), time);
  }

  /**
   * Prepare the channel to accept read and write calls
   * @throws IOException
   */
  public abstract void prepare() throws IOException;

  /**
   * To check if the channel is ready to accept read and write calls
   */
  public abstract boolean ready();

  /**
   * Reads a sequence of bytes from the channel into the {@link NetworkReceive}
   *
   * @return true if read is complete, false otherwise
   * @throws IOException if some other I/O error occurs
   */
  public abstract boolean read() throws IOException;

  /**
   * Writes a sequence of bytes to the channel from the payload in {@link NetworkSend}
   *
   * @return true if {@link Send} in {@link NetworkSend} is complete (by writing all bytes to the channel),
   * false otherwise
   * @throws IOException If some other I/O error occurs
   */
  public abstract boolean write() throws IOException;

  /**
   * Actions to be taken on completion of {@link Send} in {@link NetworkSend}
   */
  public void onSendComplete() {
    networkSend.updateServerResponseMetrics();
    sendCompleteTime = time.milliseconds();
    long sendTimeMs = sendCompleteTime - networkSend.getSendStartTimeInMs();
    metrics.transmissionSendAllTime.update(sendTimeMs);
    double sendBytesRate = networkSend.getPayload().sizeInBytes() / ((double) sendTimeMs / SystemTime.MsPerSec);
    metrics.transmissionSendBytesRate.mark((long) sendBytesRate);
  }

  /**
   * Actions to be taken on completion of {@link BoundedNettyByteBufReceive} in {@link NetworkReceive}
   */
  public void onReceiveComplete() {
    long receiveTimeMs = time.milliseconds() - networkReceive.getReceiveStartTimeInMs();
    metrics.transmissionReceiveAllTime.update(receiveTimeMs);
    double receiveBytesRate =
        networkReceive.getReceivedBytes().sizeRead() / ((double) receiveTimeMs / SystemTime.MsPerSec);
    metrics.transmissionReceiveBytesRate.mark((long) receiveBytesRate);
  }

  /**
   * Returns true if {@link NetworkReceive} is read completely
   * @return true if {@link NetworkReceive} is read completely, false otherwise
   */
  public boolean isReadComplete() {
    if (networkReceive != null) {
      return networkReceive.getReceivedBytes().isReadComplete();
    }
    return false;
  }

  /**
   * Returns the remote socket address of the underlying socket channel
   */
  public SocketAddress getRemoteSocketAddress() {
    return socketChannel.socket().getRemoteSocketAddress();
  }

  /**
   * Close the connection for the socket channel
   */
  public abstract void close() throws IOException;

  public String getConnectionId() {
    return connectionId;
  }

  public SocketChannel getSocketChannel() {
    return this.socketChannel;
  }

  public boolean hasSend() {
    return networkSend != null;
  }

  public void clearSend() {
    networkSend = null;
  }

  public boolean hasReceive() {
    return networkReceive != null;
  }

  public void clearReceive() {
    networkReceive = null;
  }

  protected void release() {
    if (networkSend != null) {
      networkSend.getPayload().release();
    }
    if (networkReceive != null) {
      networkReceive.getReceivedBytes().release();
    }
  }

  public NetworkReceive getNetworkReceive() {
    return this.networkReceive;
  }

  public NetworkSend getNetworkSend() {
    return this.networkSend;
  }

  public boolean isConnected() {
    return socketChannel.isConnected();
  }
}
