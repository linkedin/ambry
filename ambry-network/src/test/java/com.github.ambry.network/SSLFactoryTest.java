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
    SSLConfig sslConfig = TestSSLUtils.createSSLConfig();
    SSLFactory sslFactory = new SSLFactory(sslConfig);

    // SSLContext test
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);

    // client side SSLEngine test
    SSLEngine clientSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    Assert.assertEquals(sslContext.getProtocol(), "TLS");
    Assert.assertEquals(sslContext.getProvider().getName(), "SunJSSE");
    String[] enabledProtocols = clientSSLEngine.getEnabledProtocols();
    Assert.assertEquals(enabledProtocols.length, 1);
    Assert.assertEquals(enabledProtocols[0], "TLSv1.2");
    Assert.assertEquals(clientSSLEngine.getSSLParameters().getEndpointIdentificationAlgorithm(), "HTTPS");
    Assert.assertEquals(clientSSLEngine.getNeedClientAuth(), false);
    Assert.assertEquals(clientSSLEngine.getUseClientMode(), true);
    Assert.assertEquals(clientSSLEngine.getWantClientAuth(), false);
    String[] enabledCipherSuites = clientSSLEngine.getEnabledCipherSuites();
    Assert.assertEquals(enabledCipherSuites.length, 1);
    Assert.assertEquals(enabledCipherSuites[0], "TLS_RSA_WITH_AES_128_CBC_SHA256");

    // server side SSLEngine test
    SSLEngine serverSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);
    enabledProtocols = serverSSLEngine.getEnabledProtocols();
    Assert.assertEquals(enabledProtocols.length, 1);
    Assert.assertEquals(enabledProtocols[0], "TLSv1.2");
    // endpoint indentification algorithm only effect in client side SSLEngine
    Assert.assertEquals(serverSSLEngine.getSSLParameters().getEndpointIdentificationAlgorithm(), null);
    Assert.assertEquals(serverSSLEngine.getNeedClientAuth(), true);
    Assert.assertEquals(serverSSLEngine.getUseClientMode(), false);
    Assert.assertEquals(serverSSLEngine.getWantClientAuth(), false);
    enabledCipherSuites = serverSSLEngine.getEnabledCipherSuites();
    Assert.assertEquals(enabledCipherSuites.length, 1);
    Assert.assertEquals(enabledCipherSuites[0], "TLS_RSA_WITH_AES_128_CBC_SHA256");
  }
}
