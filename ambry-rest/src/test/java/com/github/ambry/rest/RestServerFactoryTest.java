package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class RestServerFactoryTest {

  @Test
  public void getRestServerDefaultTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = new MockRestRequestDelegator();

    RestServer restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNotNull("No rest server returned", restServer);
  }

  @Test
  public void getRestServerNonDefaultTest()
      throws Exception {
    Properties properties = new Properties();
    Class restServerClass = MockRestServer.class;
    properties.setProperty(RestServerFactory.SERVER_CLASS_KEY, restServerClass.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = new MockRestRequestDelegator();

    RestServer restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertEquals("Did not return rest server specified in properties", restServerClass, restServer.getClass());
  }

  @Test
  public void getRestServerWithBadInputTest()
      throws Exception {
    try {
      Properties properties = new Properties();
      properties.setProperty(RestServerFactory.SERVER_CLASS_KEY, "not.a.valid.class");
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      MetricRegistry metricRegistry = new MetricRegistry();
      RestRequestDelegator restRequestDelegator = new MockRestRequestDelegator();

      RestServerFactory.getRestServer(verifiableProperties, metricRegistry, restRequestDelegator);
      fail("Test did not fail even though a non existent class was provided as input for rest server class");
    } catch (ClassNotFoundException e) {
      //nothing to do. expected.
    }

    Properties properties = new Properties();
    // not a valid rest server
    properties.setProperty(RestServerFactory.SERVER_CLASS_KEY, "com.github.ambry.rest.MockRestContent");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestDelegator restRequestDelegator = new MockRestRequestDelegator();

    RestServer restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, restRequestDelegator);
    assertNull("No rest server should be returned since provided class in not an implementation of RestServer",
        restServer);
  }
}
