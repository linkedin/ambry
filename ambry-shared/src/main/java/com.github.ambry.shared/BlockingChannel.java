package com.github.ambry.shared;

import com.github.ambry.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.*;


/**
 * A blocking channel that is used to communicate with a server
 */
public class BlockingChannel implements ConnectedChannel {
  private final String host;
  private final int port;
  private final int readBufferSize;
  private final int writeBufferSize;
  private final int readTimeoutMs;
  private boolean connected = false;
  private SocketChannel channel = null;
  public InputStream readChannel = null;
  private GatheringByteChannel writeChannel = null;
  private Object lock = new Object();
  private Logger logger = LoggerFactory.getLogger(getClass());

  public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs) {
    this.host = host;
    this.port = port;
    this.readBufferSize = readBufferSize;
    this.writeBufferSize = writeBufferSize;
    this.readTimeoutMs = readTimeoutMs;
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
        channel.connect(new InetSocketAddress(host, port));
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
  public InputStream receive()
      throws IOException {
    if (!connected) {
      throw new ClosedChannelException();
    }

    // consume the size header and return the remaining response. Need to be done by network receive?
    long toRead = 8;
    long read = 0;
    while (read < toRead) {
      readChannel.read();
      read++;
    }
    return readChannel;
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