package com.github.ambry.coordinator;


public class MockSSLBlockingChannel extends MockBlockingChannel{

  public MockSSLBlockingChannel(MockDataNode mockDataNode, String host, int port, int readBufferSize, int writeBufferSize,
      int readTimeoutMs, int connectTimeoutMs){
    super(mockDataNode, host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);
  }
}
