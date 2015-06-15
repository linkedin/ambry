package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.MockNioServer;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Test functionality of RestServer
 */
public class RestServerTest {

  /**
   * Tests basic start shutdown of RestServer
   * @throws Exception
   */
  @Test
  public void startShutdownTest()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY,
        MockBlobStorageService.class.getCanonicalName()); // no defaults defined for blob storage service
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for correct exceptions thrown on bad input during instantiation/start of RestServer
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
   * Tests that right exceptions are thrown on start/shutdown of RestServer with a bad component.
   * @throws Exception
   */
  @Test
  public void startShutdownTestWithBadComponent()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY,
        "com.github.ambry.restservice.MockBlobStorageService");
    properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, "com.github.ambry.restservice.MockNioServer");
    properties.setProperty(MockNioServer.IS_FAULTY_KEY, "true"); // makes MockNioServer throw exceptions.
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    try {
      server.start();
      fail("Start should not be successful. MockNioServer::start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } catch (Exception e) {
      fail("RestServer::start() threw unknown exception - " + e);
    } finally {
      server.shutdown();
    }
  }

  // helpers
  // serverCreationWithBadInputTest() helpers

  /**
   * Tests instantiation attempts with bad input.
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
      // no props
      new RestServer(null, metricRegistry, clusterMap);
      fail("Properties missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      // not metric registry
      new RestServer(verifiableProperties, null, clusterMap);
      fail("Metrics registry missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      // no cluster map
      new RestServer(verifiableProperties, metricRegistry, null);
      fail("Cluster map missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad NioServer class.
   * @throws IOException
   */
  private void badNioServerClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, "non.existent.server");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained non existent class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    properties = new Properties();
    properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, "com.github.ambry.rest.RestServer");
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid NioServer class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad BlobStorageService class.
   * @throws IOException
   */
  private void badBlobStorageServiceClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY, "non.existent.blobstorage.service");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained non existent class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY, "com.github.ambry.rest.RestServer");
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid BlobStorageService class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }
}
