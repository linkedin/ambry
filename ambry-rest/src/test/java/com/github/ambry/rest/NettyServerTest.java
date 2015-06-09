package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class NettyServerTest {

  @Test
  public void startShutdownTest() throws Exception {
    RestServer restServer = getNettyServer();
    restServer.start();
    restServer.shutdown();
  }

  @Test
  public void startWithBadInputTest() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(NettyConfig.PORT_KEY, "abcd"); // should be int. So will throw at instantiation
    RestServer restServer = null;
    try {
      restServer = getNettyServer(properties);
      restServer.start();
      fail("Netty server startup should have failed because of bad port value");
    } catch (Exception e) {
      // nothing to do. expected.
    } finally {
      if(restServer != null) {
        restServer.shutdown();
      }
    }

    properties.setProperty(NettyConfig.PORT_KEY, "-1"); // should be > 0. So will throw at start
    restServer = getNettyServer(properties);
    try {
      restServer.start();
      fail("Netty server startup should have failed because of bad port value");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } finally {
      if(restServer != null) {
        restServer.shutdown();
      }
    }
  }


  // helpers
  // general
  private NettyServer getNettyServer() throws InstantiationException {
    // dud properties. should pick up defaults
    Properties properties = new Properties();
    return getNettyServer(properties);
  }

  private NettyServer getNettyServer(Properties properties) throws InstantiationException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    RestRequestDelegator requestDelegator = new MockRestRequestDelegator();
    return new NettyServer(verifiableProperties, new MetricRegistry(), requestDelegator);
  }
}
