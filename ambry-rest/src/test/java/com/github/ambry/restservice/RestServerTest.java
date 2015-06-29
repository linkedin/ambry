package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Test functionality of {@link RestServer}.
 */
public class RestServerTest {

  /**
   * Tests {@link RestServer#start()} and {@link RestServer#shutdown()}.
   * @throws Exception
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, InterruptedException, IOException {
    Properties properties = new Properties();
    // no defaults defined for BlobStorageServiceFactory so need to define it here.
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for {@link RestServer#shutdown()} when {@link RestServer#start()} had not been called previously. This test
   * is for cases where {@link RestServer#start()} has failed and {@link RestServer#shutdown()} needs to be run.
   * @throws InstantiationException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws InstantiationException, InterruptedException, IOException {
    Properties properties = new Properties();
    // no defaults defined for BlobStorageServiceFactory so need to define it here.
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer} instantiation/{@link RestServer#start()} with bad input.
   * @throws IOException
   */
  @Test
  public void serverCreationWithBadInputTest()
      throws IOException {
    badArgumentsTest();
    badNioServerClassTest();
    badBlobStorageServiceClassTest();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer#start()}/{@link RestServer#shutdown()} with bad
   * components.
   * @throws Exception
   */
  @Test
  public void startShutdownTestWithBadComponent()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.nio.server.factory", "com.github.ambry.restservice.MockNioServerFactory");
    // makes MockNioServer throw exceptions.
    properties.setProperty(MockNioServerFactory.IS_FAULTY_KEY, "true");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();
    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    try {
      server.start();
      fail("start() should not be successful. MockNioServer::start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } finally {
      try {
        server.shutdown();
        fail("RestServer shutdown should have failed.");
      } catch (RuntimeException e) {
        // nothing to do. expected.
      }
    }
  }

  // helpers
  // serverCreationWithBadInputTest() helpers

  /**
   * Tests {@link RestServer} instantiation attempts with bad input.
   * @throws IOException
   */
  private void badArgumentsTest()
      throws IOException {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    try {
      // no props.
      new RestServer(null, metricRegistry, clusterMap);
      fail("Properties missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      // no MetricRegistry.
      new RestServer(verifiableProperties, null, clusterMap);
      fail("MetricsRegistry missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      // no ClusterMap.
      new RestServer(verifiableProperties, metricRegistry, null);
      fail("ClusterMap missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad {@link com.github.ambry.restservice.NioServerFactory} class.
   * @throws IOException
   */
  private void badNioServerClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("rest.nio.server.factory", "non.existent.nio.server.factory");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained non existent NioServerFactory, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    properties = new Properties();
    // invalid NioServerFactory.
    properties.setProperty("rest.nio.server.factory", "com.github.ambry.restservice.RestServer");
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid NioServerFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad {@link com.github.ambry.restservice.BlobStorageServiceFactory} class.
   * @throws IOException
   */
  private void badBlobStorageServiceClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("rest.blob.storage.service.factory", "non.existent.blob.storage.service.factory");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained non existent BlobStorageServiceFactory, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    properties = new Properties();
    // invalid BlobStorageServiceFactory.
    properties.setProperty("rest.blob.storage.service.factory", "com.github.ambry.restservice.RestServer");
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid BlobStorageServiceFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }
}
