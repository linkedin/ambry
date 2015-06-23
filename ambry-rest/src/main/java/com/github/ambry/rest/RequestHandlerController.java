package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.RestRequestHandler;
import com.github.ambry.restservice.RestRequestHandlerController;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Starts a fixed number of {@link AsyncRequestHandler} instances and hands them out as required.
 * <p/>
 * Only one instance of this is expected to be alive and that instance lives through the lifetime of the
 * {@link RestServer}.
 */
class RequestHandlerController implements RestRequestHandlerController {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerMetrics restServerMetrics;
  private final List<RestRequestHandler> requestHandlers = new ArrayList<RestRequestHandler>();
  // index that is needed to track the handing out of AsyncRequestHandler instances.
  private final AtomicInteger currIndex = new AtomicInteger(0);

  public RequestHandlerController(int handlerCount, RestServerMetrics restServerMetrics,
      BlobStorageService blobStorageService)
      throws InstantiationException {
    if (handlerCount > 0) {
      this.restServerMetrics = restServerMetrics;
      createRequestHandlers(handlerCount, blobStorageService);
    } else {
      throw new InstantiationException("Handlers to be created has to be > 0 - (is " + handlerCount + ")");
    }
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("Starting RequestHandlerController..");
    for (int i = 0; i < requestHandlers.size(); i++) {
      requestHandlers.get(i).start();
    }
    logger.info("RequestHandlerController has started");
  }

  @Override
  public RestRequestHandler getRequestHandler()
      throws RestServiceException {
    try {
      // Alternative: Can have an implementation where we check queue sizes and then return the one with the least
      // occupancy.
      //
      // This function is thread-safe.
      int index = currIndex.getAndIncrement();
      RestRequestHandler requestHandler = requestHandlers.get(index % requestHandlers.size());
      return requestHandler;
    } catch (Exception e) {
      restServerMetrics.handlerControllerHandlerSelectionErrorCount.inc();
      throw new RestServiceException("Error while trying to pick a handler to return", e,
          RestServiceErrorCode.RequestHandlerSelectionError);
    }
  }

  @Override
  public void shutdown() {
    if (requestHandlers.size() > 0) {
      logger.info("Shutting down RequestHandlerController..");
      Iterator<RestRequestHandler> asyncRequestHandlerIterator = requestHandlers.iterator();
      while (asyncRequestHandlerIterator.hasNext()) {
        RestRequestHandler requestHandler = asyncRequestHandlerIterator.next();
        requestHandler.shutdown();
        asyncRequestHandlerIterator.remove();
      }
      logger.info("RequestHandlerController shutdown complete");
    }
  }

  /**
   * Creates handlerCount instances of {@link AsyncRequestHandler}. They are not started.
   * @param handlerCount - The number of instances of {@link AsyncRequestHandler} to be created.
   * @param blobStorageService - The {@link BlobStorageService} to be used by the {@link AsyncRequestHandler} instances.
   */
  private void createRequestHandlers(int handlerCount, BlobStorageService blobStorageService) {
    for (int i = 0; i < handlerCount; i++) {
      // This can change if there is ever a RequestHandlerFactory.
      requestHandlers.add(new AsyncRequestHandler(blobStorageService, restServerMetrics));
    }
  }
}
