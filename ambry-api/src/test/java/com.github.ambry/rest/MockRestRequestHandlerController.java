package com.github.ambry.rest;

import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Implementation of {@link RestRequestHandlerController} that can be used in tests.
 * <p/>
 * Starts a fixed (but configurable) number of {@link MockRestRequestResponseHandler} instances and hands them out when
 * requested.
 */
public class MockRestRequestHandlerController implements RestRequestHandlerController {
  public static String RETURN_NULL_ON_GET_REQUEST_HANDLER = "return.null.on.get.request.handler";

  private final AtomicInteger currIndex = new AtomicInteger(0);
  private final List<RestRequestHandler> restRequestResponseHandlers = new ArrayList<RestRequestHandler>();
  private boolean isFaulty = false;
  private VerifiableProperties failureProperties = null;

  public MockRestRequestHandlerController(int handlerCount, BlobStorageService blobStorageService)
      throws InstantiationException {
    if (handlerCount > 0) {
      createRequestHandlers(handlerCount, blobStorageService);
    } else {
      throw new InstantiationException("Handlers to be created has to be > 0 - (is " + handlerCount + ")");
    }
  }

  @Override
  public void start()
      throws InstantiationException {
    if (!isFaulty) {
      for (int i = 0; i < restRequestResponseHandlers.size(); i++) {
        restRequestResponseHandlers.get(i).start();
      }
    } else {
      throw new InstantiationException("This MockRequestHandlerController is faulty");
    }
  }

  @Override
  public void shutdown() {
    if (!isFaulty && restRequestResponseHandlers.size() > 0) {
      for (int i = 0; i < restRequestResponseHandlers.size(); i++) {
        restRequestResponseHandlers.get(i).shutdown();
        restRequestResponseHandlers.remove(i);
      }
    }
  }

  @Override
  public RestRequestHandler getRequestHandler()
      throws RestServiceException {
    if (!isFaulty) {
      try {
        int index = currIndex.getAndIncrement();
        return restRequestResponseHandlers.get(index % restRequestResponseHandlers.size());
      } catch (Exception e) {
        throw new RestServiceException("Error while trying to pick a handler to return", e,
            RestServiceErrorCode.RequestResponseHandlerSelectionError);
      }
    } else {
      if (failureProperties != null && failureProperties.containsKey(RETURN_NULL_ON_GET_REQUEST_HANDLER)
          && failureProperties.getBoolean(RETURN_NULL_ON_GET_REQUEST_HANDLER)) {
        return null;
      }
      throw new RestServiceException("Requested handler error",
          RestServiceErrorCode.RequestResponseHandlerSelectionError);
    }
  }

  /**
   * Makes the MockRestRequestHandlerController faulty.
   * @param props failure properties. Defines the faulty behaviour. Can be null.
   */
  public void breakdown(VerifiableProperties props) {
    isFaulty = true;
    failureProperties = props;
  }

  /**
   * Fixes the MockRestRequestHandlerController (not faulty anymore).
   */
  public void fix() {
    isFaulty = false;
  }

  /**
   * Creates handlerCount instances of {@link MockRestRequestResponseHandler}.
   * @param handlerCount the number of instances of {@link MockRestRequestResponseHandler} to be created.
   * @param blobStorageService the {@link BlobStorageService} implementation to be used.
   */
  private void createRequestHandlers(int handlerCount, BlobStorageService blobStorageService) {
    for (int i = 0; i < handlerCount; i++) {
      // This can change if there is ever a RestRequestHandlerFactory.
      restRequestResponseHandlers.add(new MockRestRequestResponseHandler(blobStorageService));
    }
  }
}
