package com.github.ambry.network;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;


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
  public void testSSLFactory() {
    try {
      SSLFactory sslFactory = TestUtils.createSSLFactory();
      SSLContext sslContext = sslFactory.createSSLContext();
      SSLSocketFactory socketFactory = sslContext.getSocketFactory();
      SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
      SSLEngine engine = sslFactory.createSSLEngine(sslContext, "localhost", 9095, true);
      System.out.println(socketFactory.toString());
      System.out.println(serverSocketFactory.toString());
      System.out.println(engine.toString());
    } catch (Exception e) {
      fail("Unexpected error in SSLFactory testing");
      e.printStackTrace();
    }
  }
}
