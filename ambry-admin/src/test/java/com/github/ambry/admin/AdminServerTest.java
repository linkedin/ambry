package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestServer;
import com.github.ambry.rest.RestServerFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class AdminServerTest {

  @Test
  public void startShutdownTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    AdminServer server = new AdminServer(verifiableProperties, metricRegistry, clusterMap);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  @Test
  public void serverCreationWithBadInputTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    properties.setProperty(RestServerFactory.SERVER_CLASS_KEY, "non.existent.server");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    try {
      AdminServer server = new AdminServer(null, metricRegistry, clusterMap);
      fail("Properties missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      AdminServer server = new AdminServer(verifiableProperties, null, clusterMap);
      fail("Metrics registry missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      AdminServer server = new AdminServer(verifiableProperties, metricRegistry, null);
      fail("Cluster map missing, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    try {
      AdminServer server = new AdminServer(verifiableProperties, metricRegistry, clusterMap);
      fail("Properties file contained invalid server class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  @Test (expected = Exception.class)
  public void faultyServerStartShutdownTest()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(RestServerFactory.SERVER_CLASS_KEY, "com.github.ambry.rest.MockRestServer");
    properties.setProperty(MockRestServer.IS_FAULTY_KEY, "true");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    AdminServer server = new AdminServer(verifiableProperties, metricRegistry, clusterMap);
    try {
      server.start();
      fail("Faulty server start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } catch (Exception e) {
      fail("Faulty server start() threw unknown exception - " + e);
    } finally {
      server.shutdown();
    }
  }

  // helpers
  // startShutdownTest() helpers
  private class AdminServerStarter implements Runnable {

    private final AdminServer server;
    private AtomicReference<Exception> exception;

    public AdminServerStarter(AdminServer server, AtomicReference<Exception> exception) {
      this.server = server;
      this.exception = exception;
    }

    public void run() {
      try {
        server.start();
      } catch (Exception e) {
        exception.set(e);
      }
    }
  }
}
