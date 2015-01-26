package com.github.ambry.network;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;


/**
 * A blocking channel that is used to communicate with a server
 */
public class BlockingChannel implements ConnectedChannel {
  private final String host;
  private final int port;
  private final int readBufferSize;
  private final int writeBufferSize;
  private final int readTimeoutMs;
  private final int connectTimeoutMs;
  private boolean connected = false;
  private SocketChannel channel = null;
  public InputStream readChannel = null;
  private GatheringByteChannel writeChannel = null;
  private Object lock = new Object();
  private Logger logger = LoggerFactory.getLogger(getClass());

  public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs) {
    this.host = host;
    this.port = port;
    this.readBufferSize = readBufferSize;
    this.writeBufferSize = writeBufferSize;
    this.readTimeoutMs = readTimeoutMs;
    this.connectTimeoutMs = connectTimeoutMs;
  }

  public void connect()
      throws SocketException, IOException {
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
  public void send(Send request)
      throws IOException {
    if (!connected) {
      throw new ClosedChannelException();
    }
    while (!request.isSendComplete()) {
      request.writeTo(writeChannel);
    }
  }

  @Override
  public ChannelOutput receive()
      throws IOException {
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