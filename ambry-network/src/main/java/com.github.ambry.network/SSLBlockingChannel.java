package com.github.ambry.network;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.Channels;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking channel that is used to communicate with a server using SSL
 */
public class SSLBlockingChannel extends BlockingChannel {
  private SSLSocket sslSocket = null;
  private final SSLSocketFactory sslSocketFactory;

  public SSLBlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs, SSLSocketFactory sslSocketFactory) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);
    if (sslSocketFactory == null) {
      throw new IllegalArgumentException("sslSocketFactory is null when creating SSLBlockingChannel");
    }
    this.sslSocketFactory = sslSocketFactory;
  }

  @Override
  public void connect()
      throws IOException {
    synchronized (lock) {
      if (!connected) {
        sslSocket = (SSLSocket) sslSocketFactory.createSocket(host, port);
        if (readBufferSize > 0) {
          sslSocket.setReceiveBufferSize(readBufferSize);
        }
        if (writeBufferSize > 0) {
          sslSocket.setSendBufferSize(writeBufferSize);
        }
        sslSocket.setSoTimeout(readTimeoutMs);
        sslSocket.setKeepAlive(true);
        sslSocket.setTcpNoDelay(true);

        // handshake in a blocking way
        sslSocket.startHandshake();

        writeChannel = Channels.newChannel(sslSocket.getOutputStream());
        readChannel = sslSocket.getInputStream();
        connected = true;
        logger.debug(
            "Created socket with SO_TIMEOUT = {} (requested {}), SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})",
            sslSocket.getSoTimeout(), readTimeoutMs, sslSocket.getReceiveBufferSize(), readBufferSize,
            sslSocket.getSendBufferSize(), writeBufferSize);
      }
    }
  }

  @Override
  public void disconnect() {
    synchronized (lock) {
      try {
        if (connected || sslSocket != null) {
          // closing the main socket channel *should* close the read channel
          // but let's do it to be sure.
          sslSocket.close();
          if (readChannel != null) {
            readChannel.close();
            readChannel = null;
          }
          if (writeChannel != null) {
            writeChannel.close();
            writeChannel = null;
          }
          sslSocket = null;
          connected = false;
        }
      } catch (Exception e) {
        logger.error("error while disconnecting {}", e);
      }
    }
  }
}
