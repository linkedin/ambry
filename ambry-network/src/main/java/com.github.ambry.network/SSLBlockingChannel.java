package com.github.ambry.network;

/**
 * A blocking channel that is used to communicate with a server using SSL
 */
public class SSLBlockingChannel extends BlockingChannel {

  public SSLBlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);
  }
}
