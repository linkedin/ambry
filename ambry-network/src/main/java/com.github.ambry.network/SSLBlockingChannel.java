package com.github.ambry.network;

public class SSLBlockingChannel extends BlockingChannel {

  public SSLBlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
      int connectTimeoutMs) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);
  }
}
