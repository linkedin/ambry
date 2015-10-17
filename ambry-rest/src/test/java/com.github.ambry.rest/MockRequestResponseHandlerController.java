package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Mock of {@link RequestResponseHandlerController} that can be used in tests.
 * <p/>
 * Starts a fixed (but configurable) number of {@link MockRestRequestResponseHandler} instances and hands them out when
 * requested.
 */
public class MockRequestResponseHandlerController extends RequestResponseHandlerController {
  public static String RETURN_NULL_ON_GET_REQUEST_HANDLER = "return.null.on.get.request.handler";

  private final AtomicInteger currIndex = new AtomicInteger(0);
  private final List<MockRestRequestResponseHandler> requestResponseHandlers =
      new ArrayList<MockRestRequestResponseHandler>();
  private boolean isFaulty = false;
  private VerifiableProperties failureProperties = null;

  public MockRequestResponseHandlerController(int handlerCount)
      throws InstantiationException {
    // just going to call the constructor of the super class. Not starting it, so we should be good.
    super(1, new RestServerMetrics(new MetricRegistry()));
    if (handlerCount > 0) {
      createRequestHandlers(handlerCount);
    } else {
      throw new InstantiationException("Handlers to be created has to be > 0 - (is " + handlerCount + ")");
    }
  }

  /**
   * Sets the {@link BlobStorageService} that will be used in {@link MockRestRequestResponseHandler}.
   * @param blobStorageService the {@link BlobStorageService} instance to be used in the
   *                           {@link MockRestRequestResponseHandler}.
   */
  @Override
  public void setBlobStorageService(BlobStorageService blobStorageService) {
    for (AsyncRequestResponseHandler requestResponseHandler : requestResponseHandlers) {
      requestResponseHandler.setBlobStorageService(blobStorageService);
    }
  }

  @Override
  public void start()
      throws InstantiationException {
    if (!isFaulty) {
      for (int i = 0; i < requestResponseHandlers.size(); i++) {
        requestResponseHandlers.get(i).start();
      }
    } else {
      throw new InstantiationException("This MockRequestResponseHandlerController is faulty");
    }
  }

  @Override
  public void shutdown() {
    if (!isFaulty && requestResponseHandlers.size() > 0) {
      for (int i = 0; i < requestResponseHandlers.size(); i++) {
        requestResponseHandlers.get(i).shutdown();
        requestResponseHandlers.remove(i);
      }
    }
    // just in case
    super.shutdown();
  }

  @Override
  public AsyncRequestResponseHandler getHandler() {
    if (!isFaulty) {
      int index = currIndex.getAndIncrement();
      return requestResponseHandlers.get(index % requestResponseHandlers.size());
    } else {
      if (failureProperties != null && failureProperties.containsKey(RETURN_NULL_ON_GET_REQUEST_HANDLER)
          && failureProperties.getBoolean(RETURN_NULL_ON_GET_REQUEST_HANDLER)) {
        return null;
      }
      throw new RuntimeException("Requested handler error");
    }
  }

  /**
   * Makes the MockRequestResponseHandlerController faulty.
   * @param props failure properties. Defines the faulty behaviour. Can be null.
   */
  public void breakdown(VerifiableProperties props) {
    isFaulty = true;
    failureProperties = props;
  }

  /**
   * Fixes the MockRequestResponseHandlerController (not faulty anymore).
   */
  public void fix() {
    isFaulty = false;
  }

  /**
   * Creates handlerCount instances of {@link MockRestRequestResponseHandler}.
   * @param handlerCount the number of instances of {@link MockRestRequestResponseHandler} to be created.
   */
  private void createRequestHandlers(int handlerCount) {
    for (int i = 0; i < handlerCount; i++) {
      requestResponseHandlers.add(new MockRestRequestResponseHandler());
    }
  }
}
