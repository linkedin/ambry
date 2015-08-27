package com.github.ambry.network;

import com.github.ambry.config.SSLConfig;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SSLFactoryTest {

  @Before
  public void setup()
      throws Exception {
  }

  @After
  public void teardown()
      throws Exception {
  }

  @Test
  public void testSSLFactory()
      throws Exception {
    SSLConfig sslConfig = TestSSLUtils.createSSLConfig("DC1,DC2,DC3");
    SSLFactory sslFactory = new SSLFactory(sslConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);
    SSLEngine clientSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    SSLEngine serverSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);

    TestSSLUtils.verifySSLConfig(sslContext, clientSSLEngine, true);
    TestSSLUtils.verifySSLConfig(sslContext, serverSSLEngine, false);
  }
}
