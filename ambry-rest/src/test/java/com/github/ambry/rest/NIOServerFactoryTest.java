package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.MockNIOServer;
import com.github.ambry.restservice.NIOServer;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class NIOServerFactoryTest {

  @Test
  public void getRestServerDefaultTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

    NIOServer nioServer = NIOServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNotNull("No rest server returned", nioServer);
  }

  @Test
  public void getRestServerNonDefaultTest()
      throws Exception {
    Properties properties = new Properties();
    Class restServerClass = MockNIOServer.class;
    properties.setProperty(NIOServerFactory.NIO_SERVER_CLASS_KEY, restServerClass.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();

    NIOServer NIOServer = NIOServerFactory.getNIOServer(verifiableProperties, metricRegistry, null);
    assertEquals("Did not return rest server specified in properties", restServerClass, NIOServer.getClass());
  }

  @Test
  public void getRestServerWithBadInputTest()
      throws Exception {
    try {
      Properties properties = new Properties();
      properties.setProperty(NIOServerFactory.NIO_SERVER_CLASS_KEY, "not.a.valid.class");
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      MetricRegistry metricRegistry = new MetricRegistry();
      RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

      NIOServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
      fail("Test did not fail even though a non existent class was provided as input for rest server class");
    } catch (ClassNotFoundException e) {
      //nothing to do. expected.
    }

    Properties properties = new Properties();
    // not a valid rest server
    properties.setProperty(NIOServerFactory.NIO_SERVER_CLASS_KEY, "com.github.ambry.restservice.MockRestContent");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = getRestRequestDelegator();

    NIOServer NIOServer = NIOServerFactory.getNIOServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNull("No rest server should be returned since provided class in not an implementation of NIOServer",
        NIOServer);
  }

  // helpers
  // general
  private RestRequestDelegator getRestRequestDelegator() {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = new MockBlobStorageService();
    return new RestRequestDelegator(1, restServerMetrics, blobStorageService);
  }
}
