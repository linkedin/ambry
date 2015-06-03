package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMessageHandler;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * TODO: write description
 */

public class AdminRequestDelegatorTest {

  @Test
  public void startShutdownTest()
      throws Exception {
    AdminRequestDelegator adminRequestDelegator = createDelegator(1);
    adminRequestDelegator.start();
    adminRequestDelegator.shutdown();
  }

  @Test(expected = InstantiationException.class)
  public void startWithHandlerCountZeroTest()
      throws Exception {
    AdminRequestDelegator adminRequestDelegator = createDelegator(0);
    adminRequestDelegator.start();
  }

  @Test(expected = RestException.class)
  public void messageHandlerGetWithoutStartTest()
      throws IOException, RestException { //to test the exception path
    AdminRequestDelegator adminRequestDelegator = createDelegator(5);
    adminRequestDelegator.getMessageHandler();
  }

  @Test
  public void messageHandlerGetTest()
      throws Exception {
    AdminRequestDelegator adminRequestDelegator = createDelegator(5);
    adminRequestDelegator.start();
    try {
      RestMessageHandler messageHandler = adminRequestDelegator.getMessageHandler();
      assertNotNull("Message handler is null", messageHandler);
    } catch (RestException e) {
      throw e;
    } finally {
      adminRequestDelegator.shutdown();
    }
  }

  //helpers
  //general
  private AdminRequestDelegator createDelegator(int handlerCount)
      throws IOException {
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(new MockClusterMap());
    return new AdminRequestDelegator(handlerCount, adminMetrics, adminBlobStorageService);
  }
}
