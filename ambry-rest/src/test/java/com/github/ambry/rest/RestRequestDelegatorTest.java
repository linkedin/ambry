package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */

public class RestRequestDelegatorTest {

  @Test
  public void startShutdownTest()
      throws Exception {
    RestRequestDelegator restRequestDelegator = createDelegator(1);
    restRequestDelegator.start();
    restRequestDelegator.shutdown();
  }

  @Test(expected = InstantiationException.class)
  public void startWithHandlerCountZeroTest()
      throws Exception {
    RestRequestDelegator restRequestDelegator = createDelegator(0);
    restRequestDelegator.start();
  }

  @Test
  public void messageHandlerGetWithoutStartTest()
      throws IOException, RestServiceException { //to test the exception path
    RestRequestDelegator restRequestDelegator = createDelegator(5);
    try {
      restRequestDelegator.getMessageHandler();
      fail("getMessageHandler() should have failed because delegator has not been started");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.HandlerSelectionError, e.getErrorCode());
    }
  }

  @Test
  public void messageHandlerGetTest()
      throws Exception {
    RestRequestDelegator restRequestDelegator = createDelegator(5);
    restRequestDelegator.start();
    try {
      RestMessageHandler messageHandler = restRequestDelegator.getMessageHandler();
      assertNotNull("Message handler is null", messageHandler);
    } catch (RestServiceException e) {
      throw e;
    } finally {
      restRequestDelegator.shutdown();
    }
  }

  //helpers
  //general
  private RestRequestDelegator createDelegator(int handlerCount)
      throws IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = new MockBlobStorageService();
    return new RestRequestDelegator(handlerCount, restServerMetrics, blobStorageService);
  }
}
