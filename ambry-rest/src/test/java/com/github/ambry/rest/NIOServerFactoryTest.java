package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.MockNioServer;
import com.github.ambry.restservice.NioServer;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class NioServerFactoryTest {

  @Test
  public void getRestServerDefaultTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

    NioServer nioServer = NioServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNotNull("No rest server returned", nioServer);
  }

  @Test
  public void getRestServerNonDefaultTest()
      throws Exception {
    Properties properties = new Properties();
    Class restServerClass = MockNioServer.class;
    properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, restServerClass.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();

    NioServer nioServer = NioServerFactory.getNIOServer(verifiableProperties, metricRegistry, null);
    assertEquals("Did not return rest server specified in properties", restServerClass, nioServer.getClass());
  }

  @Test
  public void getRestServerWithBadInputTest()
      throws Exception {
    try {
      Properties properties = new Properties();
      properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, "not.a.valid.class");
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      MetricRegistry metricRegistry = new MetricRegistry();
      RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

      NioServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
      fail("Test did not fail even though a non existent class was provided as input for rest server class");
    } catch (ClassNotFoundException e) {
      //nothing to do. expected.
    }

    Properties properties = new Properties();
    // not a valid rest server
    properties.setProperty(NioServerFactory.NIO_SERVER_CLASS_KEY, "com.github.ambry.restservice.MockRestContent");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

    NioServer nioServer = NioServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNull("No rest server should be returned since provided class in not an implementation of nioServer",
        nioServer);
  }

  // helpers
  // general
  private RestRequestDelegator getRestRequestDelegator()
      throws IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = getBlobStorageService();
    return new RestRequestDelegator(1, restServerMetrics, blobStorageService);
  }

  private BlobStorageService getBlobStorageService()
      throws IOException {
    // dud properties. should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new MockBlobStorageService(verifiableProperties, new MockClusterMap(), new MetricRegistry());
  }
}
