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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking channel that is used to communicate with a server
 */
public class BlockingChannel implements ConnectedChannel {
  protected final String host;
  protected final int port;
  protected final int readBufferSize;
  protected final int writeBufferSize;
  protected final int readTimeoutMs;
  protected final int connectTimeoutMs;
  protected boolean connected = false;
  protected InputStream readChannel = null;
  protected WritableByteChannel writeChannel = null;
  protected Object lock = new Object();
  protected Logger logger = LoggerFactory.getLogger(getClass());
  private SocketChannel channel = null;

  public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs) {
    this.host = host;
    this.port = port;
    this.readBufferSize = readBufferSize;
    this.writeBufferSize = writeBufferSize;
    this.readTimeoutMs = readTimeoutMs;
    this.connectTimeoutMs = connectTimeoutMs;
  }

  public void connect() throws IOException {
    synchronized (lock) {
      if (!connected) {
        channel = SocketChannel.open();
        if (readBufferSize > 0) {
          channel.socket().setReceiveBufferSize(readBufferSize);
        }
        if (writeBufferSize > 0) {
          channel.socket().setSendBufferSize(writeBufferSize);
        }
        channel.configureBlocking(true);
        channel.socket().setSoTimeout(readTimeoutMs);
        channel.socket().setKeepAlive(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().connect(new InetSocketAddress(host, port), connectTimeoutMs);
        writeChannel = channel;
        readChannel = channel.socket().getInputStream();
        connected = true;
        logger.debug("Created socket with SO_TIMEOUT = {} (requested {}), "
                + "SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})", channel.socket().getSoTimeout(),
            readTimeoutMs, channel.socket().getReceiveBufferSize(), readBufferSize,
            channel.socket().getSendBufferSize(), writeBufferSize);
      }
    }
  }

  public void disconnect() {
    synchronized (lock) {
      try {
        if (connected || channel != null) {
          // closing the main socket channel *should* close the read channel
          // but let's do it to be sure.
          channel.close();
          channel.socket().close();
          if (readChannel != null) {
            readChannel.close();
            readChannel = null;
          }
          if (writeChannel != null) {
            writeChannel.close();
            writeChannel = null;
          }
          channel = null;
          connected = false;
        }
      } catch (Exception e) {
        logger.error("error while disconnecting {}", e);
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