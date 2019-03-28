package com.github.ambry.server;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static org.mockito.Mockito.*;


/** Mock impl for {@link SSLFactory} */
public class MockSSLFactory implements SSLFactory {

  private final SSLEngine mockEngine;
  private final SSLContext mockContext;

  public MockSSLFactory(SSLConfig sslConfig) {
     mockEngine = mock(SSLEngine.class);
     mockContext = mock(SSLContext.class);
  }

  @Override
  public SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode) {
    return mockEngine;
  }

  @Override
  public SSLContext getSSLContext() {
    return mockContext;
  }
}
