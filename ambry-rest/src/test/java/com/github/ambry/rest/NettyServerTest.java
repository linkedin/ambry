package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.NioServer;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Tests netty server basic function
 */
public class NettyServerTest {

  /**
   * Tests basic startup/shutdown given good input
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, IOException {
    NioServer nioServer = getNettyServer(null);
    nioServer.start();
    nioServer.shutdown();
  }

  /**
   * Tests to see that correct exceptions are thrown on instantiation/start with bad input
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithBadInputTest()
      throws InstantiationException, IOException {
    Properties properties = new Properties();
    properties.setProperty(NettyConfig.PORT_KEY, "abcd"); // should be int. So will throw at instantiation
    NioServer nioServer = null;
    try {
      nioServer = getNettyServer(properties);
      fail("Netty server startup should have failed because of bad port value");
    } catch (NumberFormatException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
      }
    }

    properties.setProperty(NettyConfig.PORT_KEY, "-1"); // should be > 0. So will throw at start
    nioServer = getNettyServer(properties);
    try {
      nioServer.start();
      fail("Netty server startup should have failed because of bad port value");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
      }
    }
  }

  // helpers
  // general
  private RestRequestDelegator getRestRequestDelegator(Properties properties)
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = getBlobStorageService(properties);
    return new RestRequestDelegator(1, restServerMetrics, blobStorageService);
  }

  private BlobStorageService getBlobStorageService(Properties properties)
      throws IOException {
    if (properties == null) {
      // dud properties. should pick up defaults
      properties = new Properties();
    }
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new MockBlobStorageService(verifiableProperties, new MockClusterMap(), new MetricRegistry());
  }

  private NettyServer getNettyServer(Properties properties)
      throws InstantiationException, IOException {
    if (properties == null) {
      // dud properties. should pick up defaults
      properties = new Properties();
    }
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    RestRequestDelegator requestDelegator = getRestRequestDelegator(properties);
    return new NettyServer(verifiableProperties, new MetricRegistry(), requestDelegator);
  }
}
