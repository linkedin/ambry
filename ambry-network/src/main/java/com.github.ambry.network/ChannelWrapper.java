package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;


/**
 * ChannelWrapper to interact with a given socketChannel in plain text
 */
public class ChannelWrapper {
  private String connectionId;
  private NetworkSend networkSend = null;
  private NetworkReceive networkReceive = null;
  protected SocketChannel socketChannel = null;
  protected SelectionKey key = null;
  protected final Time time;
  protected final NetworkMetrics metrics;
  private Logger logger;

  public ChannelWrapper(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
      NetworkMetrics metrics, Logger logger) {
    this.connectionId = connectionId;
    this.socketChannel = socketChannel;
    this.key = key;
    this.time = time;
    this.metrics = metrics;
    this.logger = logger;
  }

  public void finishConnect()
      throws IOException {
    socketChannel.finishConnect();
    key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
  }

  void setNetworkSend(NetworkSend networkSend) {
    if (hasSend()) {
      throw new IllegalStateException(
          "Attempt to begin a networkSend operation with prior networkSend operation still in progress.");
    }
    this.networkSend = networkSend;
    metrics.sendInFlight.inc();
    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
  }

  /**
   * Reads data from the socketChannel
   * @return total bytes read from the socket channel
   */
  long read()
      throws IOException {
    if (!hasReceive()) {
      networkReceive = new NetworkReceive(getConnectionId(), new BoundedByteBufferReceive(), time);
    }

    long bytesRead = networkReceive.getReceivedBytes().readFrom(socketChannel);
    return bytesRead;
  }

  /**
   * Writes the payload to the socket channel
   * @return true if send is complete, false otherwise
   */
  boolean write()
      throws IOException {
    Send send = networkSend.getPayload();
    if (send == null) {
      throw new IllegalStateException("Registered for write interest but no response attached to key.");
    }
    send.writeTo(socketChannel);
    logger
        .trace("Bytes written to {} using key {}", socketChannel.socket().getRemoteSocketAddress(), connectionId);
    return send.isSendComplete();
  }

  /**
   * Close the connection for the socket channel
   */
  void close() {
    clearReceive();
    clearSend();
    key.attach(null);
    key.cancel();
    try {
      socketChannel.socket().close();
      socketChannel.close();
    } catch (IOException e) {
      metrics.selectorCloseSocketErrorCount.inc();
      logger.error("Exception closing connection to node {}:", connectionId, e);
    }
  }

  String getConnectionId() {
    return connectionId;
  }

  boolean hasSend() {
    return networkSend != null;
  }

  void clearSend() {
    networkSend = null;
  }

  boolean hasReceive() {
    return networkReceive != null;
  }

  void clearReceive() {
    networkReceive = null;
  }

  SocketChannel getSocketChannel() {
    return this.socketChannel;
  }

  NetworkReceive getNetworkReceive() {
    return this.networkReceive;
  }

  NetworkSend getNetworkSend() {
    return this.networkSend;
  }
}
