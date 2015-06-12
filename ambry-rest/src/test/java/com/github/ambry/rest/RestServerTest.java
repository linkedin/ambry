package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.MockNIOServer;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class RestServerTest {

  @Test
  public void startShutdownTest()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY,
        MockBlobStorageService.class.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  @Test
  public void serverCreationWithBadInputTest()
      throws IOException {
    badArgumentsTest();
    badNioServerClassTest();
    badBlobStorageServiceClassTest();
  }

  @Test(expected = Exception.class)
  public void faultyServerStartShutdownTest()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(NIOServerFactory.NIO_SERVER_CLASS_KEY, "com.github.ambry.restservice.MockNIOServer");
    properties.setProperty(MockNIOServer.IS_FAULTY_KEY, "true");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    RestServer server = new RestServer(verifiableProperties, metricRegistry, clusterMap);
    try {
      server.start();
      fail("Faulty server start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } catch (Exception e) {
      fail("Faulty server start() threw unknown exception - " + e);
    } finally {
      server.shutdown();
      fail("Faulty server shutdown() would have thrown Exception");
    }
  }

  // helpers
  // serverCreationWithBadInputTest() helpers
  private void badArgumentsTest()
      throws IOException {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    try {
      new RestServer(null, metricRegistry, clusterMap);
      fail("Properties missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      new RestServer(verifiableProperties, null, clusterMap);
      fail("Metrics registry missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      new RestServer(verifiableProperties, metricRegistry, null);
      fail("Cluster map missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  private void badNioServerClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(NIOServerFactory.NIO_SERVER_CLASS_KEY, "non.existent.server");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid server class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  private void badBlobStorageServiceClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(BlobStorageServiceFactory.BLOBSTORAGE_SERVICE_CLASS_KEY, "non.existent.blobstorage.service");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MetricRegistry(), new MockClusterMap());
      fail("Properties file contained invalid server class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }
}
