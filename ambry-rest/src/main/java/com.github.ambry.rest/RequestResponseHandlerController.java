package com.github.ambry.rest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Starts a fixed number of {@link AsyncRequestResponseHandler} instances and hands them out as required.
 * <p/>
 * It cannot be expected to return a specific instance of {@link AsyncRequestResponseHandler} at any point of time and
 * may choose any instance from its pool.
 * <p/>
 * Only one instance of this is expected to be alive and that instance lives through the lifetime of the
 * {@link RestServer}.
 * <p/>
 * This class is thread-safe.
 */
public class RequestResponseHandlerController {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerMetrics restServerMetrics;
  private final List<AsyncRequestResponseHandler> requestResponseHandlers =
      new ArrayList<AsyncRequestResponseHandler>();
  private final AtomicInteger currIndex = new AtomicInteger(0);

  /**
   * Creates a new instance of {@link RequestResponseHandlerController}.
   * @param handlerCount the number of {@link AsyncRequestResponseHandler} instance required. Has to be > 0.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use.
   * @throws IllegalArgumentException if {@code handlerCount} <= 0.
   */
  public RequestResponseHandlerController(int handlerCount, RestServerMetrics restServerMetrics) {
    if (handlerCount > 0) {
      this.restServerMetrics = restServerMetrics;
      createRequestHandlers(handlerCount);
      restServerMetrics.trackRequestHandlerHealth(requestResponseHandlers);
    } else {
      throw new IllegalArgumentException("Handlers to be created has to be > 0. Is " + handlerCount);
    }
    logger.trace("Instantiated RequestResponseHandlerController");
  }

  /**
   * Returns a {@link AsyncRequestResponseHandler} that can be used to handle incoming requests or send outgoing
   * responses.
   * <p/>
   * If {@link RequestResponseHandlerController#start()} was not called before a call to this function, it is not
   * guaranteed that the {@link AsyncRequestResponseHandler} is started and available for use.
   * <p/>
   * Multiple calls to this function (even by the same thread) can return different instances of
   * {@link AsyncRequestResponseHandler} and no order/pattern can be expected.
   * @return a {@link AsyncRequestResponseHandler} that can be used to handle requests/responses.
   */
  public AsyncRequestResponseHandler getHandler() {
    int index = currIndex.getAndIncrement();
    logger.trace("Monotonically increasing value {} was used to pick request handler at index {}", index,
        index % requestResponseHandlers.size());
    return requestResponseHandlers.get(index % requestResponseHandlers.size());
  }

  /**
   * Sets the {@link BlobStorageService} that will be used in {@link AsyncRequestResponseHandler}.
   * @param blobStorageService the {@link BlobStorageService} instance to be used in the
   *                           {@link AsyncRequestResponseHandler}.
   */
  protected void setBlobStorageService(BlobStorageService blobStorageService) {
    for (AsyncRequestResponseHandler requestResponseHandler : requestResponseHandlers) {
      requestResponseHandler.setBlobStorageService(blobStorageService);
    }
  }

  /**
   * Does startup tasks for the RequestResponseHandlerController. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the RequestResponseHandlerController is unable to start.
   */
  protected void start()
      throws InstantiationException {
    logger.info("Starting RequestResponseHandlerController with {} request handler(s)", requestResponseHandlers.size());
    for (AsyncRequestResponseHandler requestResponseHandler : requestResponseHandlers) {
      requestResponseHandler.start();
    }
    logger.info("RequestResponseHandlerController has started");
  }

  /**
   * Does shutdown tasks for the RequestResponseHandlerController. When the function returns, shutdown is FULLY complete.
   */
  protected void shutdown() {
    if (requestResponseHandlers.size() > 0) {
      logger.info("Shutting down RequestResponseHandlerController");
      Iterator<AsyncRequestResponseHandler> asyncRequestHandlerIterator = requestResponseHandlers.iterator();
      while (asyncRequestHandlerIterator.hasNext()) {
        AsyncRequestResponseHandler requestHandler = asyncRequestHandlerIterator.next();
        requestHandler.shutdown();
        asyncRequestHandlerIterator.remove();
      }
      logger.info("RequestResponseHandlerController shutdown complete");
    }
  }

  /**
   * Creates handlerCount instances of {@link AsyncRequestResponseHandler}. They are not started.
   * @param handlerCount The number of instances of {@link AsyncRequestResponseHandler} to be created.
   */
  private void createRequestHandlers(int handlerCount) {
    logger.trace("Creating {} instances of AsyncRequestResponseHandler", handlerCount);
    for (int i = 0; i < handlerCount; i++) {
      requestResponseHandlers.add(new AsyncRequestResponseHandler(restServerMetrics));
    }
  }
}
