package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Tests functionality of RestRequestDelegator
 */

public class RestRequestDelegatorTest {

  /**
   * Tests basic start/shutdown
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, IOException {
    RestRequestDelegator restRequestDelegator = createDelegator(1);
    restRequestDelegator.start();
    restRequestDelegator.shutdown();
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a RestRequestDelegator with 0 handlers
   * @throws Exception
   */
  @Test(expected = InstantiationException.class)
  public void startWithHandlerCountZeroTest()
      throws Exception {
    createDelegator(0);
  }

  /**
   * Tests exception and error code when trying to get a message handler before starting the RestRequestDelegator
   * @throws InstantiationException
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void messageHandlerGetWithoutStartTest()
      throws InstantiationException, IOException, RestServiceException {
    RestRequestDelegator restRequestDelegator = createDelegator(5);
    try {
      // did not start yet.
      restRequestDelegator.getMessageHandler();
      fail("getMessageHandler() should have failed because delegator has not been started");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.HandlerSelectionError, e.getErrorCode());
    }
  }

  /**
   * Tests normal getting of message handler.
   * @throws InstantiationException
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void messageHandlerGetTest()
      throws InstantiationException, IOException, RestServiceException {
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
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = getBlobStorageService();
    return new RestRequestDelegator(handlerCount, restServerMetrics, blobStorageService);
  }

  private BlobStorageService getBlobStorageService()
      throws IOException {
    // dud properties. should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new MockBlobStorageService(verifiableProperties, new MockClusterMap(), new MetricRegistry());
  }
}
