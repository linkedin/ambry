package com.github.ambry.rest;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link AsyncRequestResponseHandler} specific implementation of {@link RestRequestHandlerFactory} and
 * {@link RestResponseHandlerFactory}.
 * <p/>
 * Sets up all the supporting cast required for {@link AsyncRequestResponseHandler}. Maintains a single instance of
 * {@link AsyncRequestResponseHandler} and returns the same instance on any call to {@link #getRestRequestHandler()} or
 * {@link #getRestResponseHandler()}.
 */
public class AsyncRequestResponseHandlerFactory implements RestRequestHandlerFactory, RestResponseHandlerFactory {

  private static final AtomicBoolean instantiated = new AtomicBoolean(false);
  private static AsyncRequestResponseHandler instance;
  private static RestServerMetrics restServerMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Constructor for {@link RestResponseHandlerFactory}.
   * @param handlerCount the number of response scaling units required.
   * @param restServerMetrics the {@link RestServerMetrics} instance that should be used for metrics.
   * @throws IllegalArgumentException if {@code handlerCount} <= 0 or if {@code restServerMetrics} is null.
   */
  public AsyncRequestResponseHandlerFactory(Integer handlerCount, RestServerMetrics restServerMetrics) {
    if (restServerMetrics == null) {
      throw new IllegalArgumentException("RestServerMetrics instance provided is null");
    } else if (handlerCount <= 0) {
      throw new IllegalArgumentException("Response handler scaling unit count has to be > 0. Is " + handlerCount);
    } else {
      AsyncRequestResponseHandler responseHandler = getInstance(restServerMetrics);
      responseHandler.setResponseWorkersCount(handlerCount);
    }
    logger.trace("Instantiated AsyncRequestResponseHandlerFactory as RestResponseHandler");
  }

  /**
   * Constructor for {@link RestRequestHandlerFactory}.
   * @param handlerCount the number of request scaling units required.
   * @param restServerMetrics the {@link RestServerMetrics} instance that should be used for metrics.
   * @param blobStorageService the {@link BlobStorageService} to use for handling requests.
   * @throws IllegalArgumentException if {@code handlerCount} <= 0 or if {@code restServerMetrics} or
   * {@code blobStorageService} is null.
   */
  public AsyncRequestResponseHandlerFactory(Integer handlerCount, RestServerMetrics restServerMetrics,
      BlobStorageService blobStorageService) {
    if (restServerMetrics == null || blobStorageService == null) {
      throw new IllegalArgumentException("One or more arguments received is null");
    } else if (handlerCount <= 0) {
      throw new IllegalArgumentException("Request handler scaling unit count has to be > 0. Is " + handlerCount);
    } else {
      AsyncRequestResponseHandler requestHandler = getInstance(restServerMetrics);
      requestHandler.setRequestWorkersCount(handlerCount);
      requestHandler.setBlobStorageService(blobStorageService);
    }
    logger.trace("Instantiated AsyncRequestResponseHandlerFactory as RestRequestHandler");
  }

  /**
   * Returns an instance of {@link AsyncRequestResponseHandler}.
   * @return an instance of {@link AsyncRequestResponseHandler}.
   */
  @Override
  public RestRequestHandler getRestRequestHandler() {
    return instance;
  }

  /**
   * Returns an instance of {@link AsyncRequestResponseHandler}.
   * @return an instance of {@link AsyncRequestResponseHandler}.
   */
  @Override
  public RestResponseHandler getRestResponseHandler() {
    return instance;
  }

  /**
   * Returns the singleton {@link AsyncRequestResponseHandler} instance being maintained. Creates it if it hasn't been
   * created already.
   * @param restServerMetrics the {@link RestServerMetrics} instance that should be used for metrics.
   * @return an instance of {@link AsyncRequestResponseHandler}.
   */
  private static AsyncRequestResponseHandler getInstance(RestServerMetrics restServerMetrics) {
    if (instantiated.compareAndSet(false, true)) {
      AsyncRequestResponseHandlerFactory.restServerMetrics = restServerMetrics;
      instance = new AsyncRequestResponseHandler(restServerMetrics);
    }
    // check if same instance of RestServerMetrics - otherwise it is a problem.
    if (AsyncRequestResponseHandlerFactory.restServerMetrics != restServerMetrics) {
      throw new IllegalStateException("RestServerMetrics instance provided during construction of "
          + "AsyncRequestResponseHandler differs from the one currently received");
    }
    return instance;
  }
}
