package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.InMemoryRouter;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

// TODO: need setBlobStorageService tests. Here and in Async...

/**
 * Tests functionality of {@link RequestResponseHandlerController}.
 */

public class RequestResponseHandlerControllerTest {

  /**
   * Tests {@link RequestResponseHandlerController#start()} and {@link RequestResponseHandlerController#shutdown()}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, IOException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1);
    requestResponseHandlerController.start();
    requestResponseHandlerController.shutdown();
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a {@link RequestResponseHandlerController} with 0
   * handlers.
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void startWithHandlerCountZeroTest()
      throws Exception {
    createHandlerController(0);
  }

  /**
   * Tests for {@link RequestResponseHandlerController#shutdown()} when {@link RequestResponseHandlerController#start()}
   * has not been called previously. This test is for cases where {@link RequestResponseHandlerController#start()} has
   * failed and {@link RequestResponseHandlerController#shutdown()} needs to be run.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws InstantiationException, IOException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1);
    requestResponseHandlerController.shutdown();
  }

  /**
   * This tests for exceptions thrown when a {@link RequestResponseHandlerController} is used without calling
   * {@link RequestResponseHandlerController#start()} first.
   * @throws InstantiationException
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void useServiceWithoutStartTest()
      throws InstantiationException, IOException, RestServiceException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1);
    try {
      // fine to use without start.
      assertNotNull("Request handler is null", requestResponseHandlerController.getHandler());
    } finally {
      requestResponseHandlerController.shutdown();
    }
  }

  /**
   * Tests getting of a {@link AsyncRequestResponseHandler} instance.
   * @throws InstantiationException
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void requestHandlerGetTest()
      throws InstantiationException, IOException, RestServiceException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(5);
    requestResponseHandlerController.start();
    try {
      for (int i = 0; i < 1000; i++) {
        assertNotNull("Obtained AsyncRequestResponseHandler is null", requestResponseHandlerController.getHandler());
      }
    } finally {
      requestResponseHandlerController.shutdown();
    }
  }

  //helpers
  //general
  private RequestResponseHandlerController createHandlerController(int handlerCount)
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    BlobStorageService blobStorageService =
        new MockBlobStorageService(verifiableProperties, new InMemoryRouter(verifiableProperties));
    RequestResponseHandlerController controller = new RequestResponseHandlerController(handlerCount, restServerMetrics);
    controller.setBlobStorageService(blobStorageService);
    return controller;
  }
}
