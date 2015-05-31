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
    try {
      AtomicReference<Exception> e = new AtomicReference<Exception>();
      Thread serverStarter = new Thread(new AdminServerStarter(server, e));
      serverStarter.start();
      awaitServerStart(server, e);
    } catch (Exception e) {
      throw e;
    } finally {
      server.shutdown();
      if (!server.awaitShutdown(1, TimeUnit.MINUTES)) {
        throw new Exception("Shutdown did not complete within timeout");
      }
      assertEquals("isTerminated is not true", true, server.isTerminated());
    }
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

  @Test
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
    }

    assertFalse("Faulty server awaitShutdown() would have returned false", server.awaitShutdown(1, TimeUnit.MINUTES));
  }

  // helpers
  // startShutdownTest() helpers
  private void awaitServerStart(AdminServer server, AtomicReference<Exception> exception)
      throws Exception {
    // wait for 5 seconds. After each period of wait, check if server is up
    int singleWaitTime = 5; //seconds
    int maxIterations = 6;
    for (int i = 0; i < maxIterations; i++) {
      try {
        Thread.sleep(singleWaitTime * 1000);
        if (exception.get() != null) {
          throw exception.get();
        }
        if (server.isUp()) {
          return;
        }
      } catch (InterruptedException ie) {
        // just move on
      }
    }
    if (exception.get() != null) {
      throw exception.get();
    } else {
      throw new InstantiationException("Server did not startup after " + singleWaitTime * maxIterations + " seconds");
    }
  }

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
