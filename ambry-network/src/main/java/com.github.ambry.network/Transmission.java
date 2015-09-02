package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import org.slf4j.Logger;


public abstract class Transmission{

  private String connectionId;
  protected NetworkSend networkSend = null;
  protected NetworkReceive networkReceive = null;
  protected SocketChannel socketChannel = null;
  protected SelectionKey key = null;
  protected final Time time;
  protected final NetworkMetrics metrics;
  protected Logger logger;

  public Transmission(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
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

  public boolean isOpen(){
    return socketChannel.isOpen();
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

  abstract void prepare() throws IOException;

  abstract  boolean ready();

  /**
   * Reads data from the socketChannel
   * @return total bytes read from the socket channel
   */
  abstract long read() throws IOException ;

  /**
   * Writes the payload to the socket channel
   * @return true if send is complete, false otherwise
   */
  abstract boolean write() throws IOException;

  /**
   * Close the connection for the socket channel
   */
  public abstract void close() throws IOException;

  String getConnectionId() {
    return connectionId;
  }

  SocketChannel getSocketChannel() {
    return this.socketChannel;
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

  NetworkReceive getNetworkReceive() {
    return this.networkReceive;
  }

  NetworkSend getNetworkSend() {
    return this.networkSend;
  }

  boolean isConnected() {
    return socketChannel.isConnected();
  }
}
