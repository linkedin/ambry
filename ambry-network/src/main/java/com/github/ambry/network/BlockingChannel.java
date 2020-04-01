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

import com.github.ambry.config.ConnectionPoolConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking channel that is used to communicate with a server
 */
public class BlockingChannel implements ConnectedChannel {
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final String host;
  protected final int port;
  protected final int readBufferSize;
  protected final int writeBufferSize;
  protected final int readTimeoutMs;
  protected final int connectTimeoutMs;
  protected boolean connected = false;
  protected InputStream readChannel = null;
  protected WritableByteChannel writeChannel = null;
  protected boolean enableTcpNoDelay;
  private Socket socket = null;

  public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs, boolean enableTcpNoDelay) {
    this.host = host;
    this.port = port;
    this.readBufferSize = readBufferSize;
    this.writeBufferSize = writeBufferSize;
    this.readTimeoutMs = readTimeoutMs;
    this.connectTimeoutMs = connectTimeoutMs;
    this.enableTcpNoDelay = enableTcpNoDelay;
  }

  public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs) {
    this(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs, true);
  }

  public BlockingChannel(String host, int port, ConnectionPoolConfig config) {
    this(host, port, config.connectionPoolReadBufferSizeBytes, config.connectionPoolWriteBufferSizeBytes,
        config.connectionPoolReadTimeoutMs, config.connectionPoolConnectTimeoutMs,
        config.connectionPoolSocketEnableTcpNoDelay);
  }

  /**
   * Connect a socket and create writeChannel (WriteByteChannel) and readChannel (InputStream)
   *
   * @throws IOException
   */
  @Override
  public void connect() throws IOException {
    synchronized (this) {
      if (!connected) {

        socket = createSocket();

        writeChannel = Channels.newChannel(socket.getOutputStream());
        readChannel = socket.getInputStream();

        connected = true;

        logger.debug("Connection established to {}({}):{}", host, socket.getRemoteSocketAddress(), socket.getPort());

      }
    }
  }

  /**
   * Creates and connects a Socket
   * @return Socket
   * @throws IOException
   */
  public Socket createSocket() throws IOException {
    Socket tcpSocket = new Socket();

    if (readBufferSize > 0) {
      tcpSocket.setReceiveBufferSize(readBufferSize);
    }
    if (writeBufferSize > 0) {
      tcpSocket.setSendBufferSize(writeBufferSize);
    }
    tcpSocket.setSoTimeout(readTimeoutMs);
    tcpSocket.setKeepAlive(true);
    tcpSocket.setTcpNoDelay(enableTcpNoDelay);
    tcpSocket.connect(new InetSocketAddress(host, port), connectTimeoutMs);

    logger.debug("Created socket with SO_TIMEOUT = {} (requested {}), "
            + "SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})", tcpSocket.getSoTimeout(), readTimeoutMs,
        tcpSocket.getReceiveBufferSize(), readBufferSize, tcpSocket.getSendBufferSize(), writeBufferSize);

    return tcpSocket;
  }

    /**
     * Disconnect readChannel, writeChannel and close underlying Socket
     */
    @Override
    public void disconnect() {
    synchronized (this) {
      try {
        if (connected || socket != null) {
          if (readChannel != null) {
            readChannel.close();
            readChannel = null;
          }
          if (writeChannel != null) {
            writeChannel.close();
            writeChannel = null;
          }
          socket.close();
          socket = null;
          connected = false;
        }
      } catch (Exception e) {
        logger.error("error while disconnecting {}", e);
      }
    }
  }

  /**
   * Sets socket options to close immediately
   * (subsequent Socket.close() will cause connection closure by sending TCP RST instead of FIN
   */
  public void reset() {
    synchronized (this) {
      try {
        if (connected || socket != null) {
          // Setting SO_LINGER to true and time to 0 to send TCP RST instead of TCP FIN
          socket.setSoLinger(true, 0);
          disconnect();
        }
      } catch (Exception e) {
        logger.error("Error while setting socket linger option {}", e);
      }
    }
  }

  public boolean isConnected() {
    return connected;
  }

  @Override
  public void send(Send request) throws IOException {
    if (!connected) {
      throw new ClosedChannelException();
    }
    while (!request.isSendComplete()) {
      request.writeTo(writeChannel);
    }
  }

  @Override
  public ChannelOutput receive() throws IOException {
    if (!connected) {
      throw new ClosedChannelException();
    }

    // consume the size header and return the remaining response.
    ByteBuffer streamSizeBuffer = ByteBuffer.allocate(8);
    while (streamSizeBuffer.position() < streamSizeBuffer.capacity()) {
      int read = readChannel.read();
      if (read == -1) {
        throw new IOException("Could not read complete size from readChannel ");
      }
      streamSizeBuffer.put((byte) read);
    }
    streamSizeBuffer.flip();
    return new ChannelOutput(readChannel, streamSizeBuffer.getLong() - 8);
  }

  @Override
  public String getRemoteHost() {
    return host;
  }

  @Override
  public int getRemotePort() {
    return port;
  }
}
