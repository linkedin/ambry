package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.InMemoryRouter;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


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
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1, true);
    requestResponseHandlerController.start();
    requestResponseHandlerController.shutdown();
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a {@link RequestResponseHandlerController} with 0
   * handlers.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithHandlerCountZeroTest()
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    try {
      new RequestResponseHandlerController(0, restServerMetrics);
      fail("RequestResponseHandlerController instantiation should have failed because handlerCount is 0");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
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
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1, true);
    requestResponseHandlerController.shutdown();
  }

  /**
   * This tests for exceptions thrown when a {@link RequestResponseHandlerController} is used without calling
   * {@link RequestResponseHandlerController#start()} first.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void useServiceWithoutStartTest()
      throws InstantiationException, IOException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1, true);
    try {
      // fine to use without start.
      assertNotNull("Request handler is null", requestResponseHandlerController.getHandler());
    } finally {
      requestResponseHandlerController.shutdown();
    }
  }

  /**
   * This tests for exceptions thrown when a {@link RequestResponseHandlerController} is started without setting a
   * {@link BlobStorageService}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithoutBlobStorageServiceTest()
      throws InstantiationException, IOException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(1, false);
    try {
      requestResponseHandlerController.start();
      fail("Start should have failed because no BlobStorageService was set.");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    } finally {
      requestResponseHandlerController.shutdown();
    }
  }

  /**
   * Tests getting of a {@link AsyncRequestResponseHandler} instance.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void requestHandlerGetTest()
      throws InstantiationException, IOException {
    RequestResponseHandlerController requestResponseHandlerController = createHandlerController(5, true);
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

  /**
   * Creates and gets an instance of {@link RequestResponseHandlerController}.
   *
   * @param handlerCount the number of scaling units required inside the {@link RequestResponseHandlerController}.
   * @param setBlobStorageService whether to set the {@link BlobStorageService} or not.
   * @return an instance of {@link RequestResponseHandlerController}.
   * @throws InstantiationException
   * @throws IOException
   */
  private RequestResponseHandlerController createHandlerController(int handlerCount, boolean setBlobStorageService)
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    RequestResponseHandlerController controller = new RequestResponseHandlerController(handlerCount, restServerMetrics);
    if (setBlobStorageService) {
      BlobStorageService blobStorageService =
          new MockBlobStorageService(verifiableProperties, new InMemoryRouter(verifiableProperties));
      controller.setBlobStorageService(blobStorageService);
    }
    return controller;
  }
}
