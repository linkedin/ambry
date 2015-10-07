package com.github.ambry.rest;

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
  private final AtomicInteger currIndex = new AtomicInteger(0);

  public RequestHandlerController(int handlerCount, RestServerMetrics restServerMetrics,
      BlobStorageService blobStorageService) {
    if (handlerCount > 0) {
      this.restServerMetrics = restServerMetrics;
      createRequestHandlers(handlerCount, blobStorageService);
      restServerMetrics.trackRequestHandlerHealth(requestHandlers);
    } else {
      restServerMetrics.requestHandlerControllerInstantiationError.inc();
      throw new IllegalArgumentException("Handlers to be created has to be > 0. Is " + handlerCount);
    }
    logger.trace("Instantiated RequestHandlerController");
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("Starting RequestHandlerController with {} request handler(s)", requestHandlers.size());
    for (int i = 0; i < requestHandlers.size(); i++) {
      requestHandlers.get(i).start();
    }
    logger.info("RequestHandlerController has started");
  }

  @Override
  public RestRequestHandler getRequestHandler()
      throws RestServiceException {
    RestRequestHandler requestHandler;
    int index = currIndex.getAndIncrement();
    try {
      requestHandler = requestHandlers.get(index % requestHandlers.size());
      logger.trace("Monotonically increasing value {} was used to pick request handler at index {}", index,
          index % requestHandlers.size());
    } catch (Exception e) {
      restServerMetrics.requestHandlerSelectionError.inc();
      throw new RestServiceException("Exception during selection of a RestRequestHandler to return", e,
          RestServiceErrorCode.RequestHandlerSelectionError);
    }
    return requestHandler;
  }

  @Override
  public void shutdown() {
    if (requestHandlers.size() > 0) {
      logger.info("Shutting down RequestHandlerController");
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
   * @param handlerCount The number of instances of {@link AsyncRequestHandler} to be created.
   * @param blobStorageService The {@link BlobStorageService} to be used by the {@link AsyncRequestHandler} instances.
   */
  private void createRequestHandlers(int handlerCount, BlobStorageService blobStorageService) {
    logger.trace("Creating {} instances of AsyncRequestHandler", handlerCount);
    for (int i = 0; i < handlerCount; i++) {
      // This can change if there is ever a RequestHandlerFactory.
      requestHandlers.add(new AsyncRequestHandler(blobStorageService, restServerMetrics));
    }
  }
}
