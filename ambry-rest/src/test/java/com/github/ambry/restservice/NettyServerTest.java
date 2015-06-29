package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Tests basic functionality of {@link NettyServer}.
 */
public class NettyServerTest {

  /**
   * Tests {@link NettyServer#start()} and {@link NettyServer#shutdown()} given good input.
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
   * Tests for {@link NettyServer#shutdown()} when {@link NettyServer#start()} has not been called previously.
   * This test is for cases where {@link NettyServer#start()} has failed and {@link NettyServer#shutdown()} needs to be
   * run.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws InstantiationException, IOException {
    NioServer nioServer = getNettyServer(null);
    nioServer.shutdown();
  }

  /**
   * Tests for correct exceptions are thrown on {@link NettyServer} instantiation/{@link NettyServer#start()} with bad
   * input.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithBadInputTest()
      throws InstantiationException, IOException {
    Properties properties = new Properties();
    // Should be int. So will throw at instantiation.
    properties.setProperty("netty.server.port", "abcd");
    NioServer nioServer = null;
    try {
      nioServer = getNettyServer(properties);
      fail("NettyServer instantiation should have failed because of bad nettyServerPort value");
    } catch (NumberFormatException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
      }
    }

    // Should be > 0. So will throw at start().
    properties.setProperty("netty.server.port", "-1");
    nioServer = getNettyServer(properties);
    try {
      nioServer.start();
      fail("NettyServer start() should have failed because of bad nettyServerPort value");
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
  private RestRequestHandlerController getRestRequestHandlerController(Properties properties)
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = new MockBlobStorageService(new MockClusterMap());
    return new RequestHandlerController(1, restServerMetrics, blobStorageService);
  }

  private NettyServer getNettyServer(Properties properties)
      throws InstantiationException, IOException {
    if (properties == null) {
      // dud properties. should pick up defaults
      properties = new Properties();
    }
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    RestRequestHandlerController requestHandlerController = getRestRequestHandlerController(properties);
    return new NettyServer(nettyConfig, nettyMetrics, requestHandlerController);
  }
}
