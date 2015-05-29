package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;


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
      if (!server.awaitShutdown(5, TimeUnit.MINUTES)) {
        throw new Exception("Shutdown did not complete within 5 minutes");
      }
    }
  }

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
