package com.github.ambry.rest;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Implementation of {@link RestRequestHandlerFactory} and {@link RestResponseHandlerFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for {@link MockRequestResponseHandler}. Maintains a single instance of
 * {@link MockRequestResponseHandler} and returns the same instance on any call to {@link #getRestRequestHandler()} or
 * {@link #getRestResponseHandler()}.
 */
public class MockRequestResponseHandlerFactory implements RestRequestHandlerFactory, RestResponseHandlerFactory {
  private static final AtomicBoolean instantiated = new AtomicBoolean(false);
  private static MockRequestResponseHandler instance;

  public MockRequestResponseHandlerFactory(Object handlerCount, Object restServerMetrics) {
  }

  public MockRequestResponseHandlerFactory(Object handlerCount, Object restServerMetrics,
      BlobStorageService blobStorageService) {
    MockRequestResponseHandler requestHandler = getInstance();
    requestHandler.setBlobStorageService(blobStorageService);
  }

  /**
   * Returns an instance of {@link MockRequestResponseHandler}.
   * @return an instance of {@link MockRequestResponseHandler}.
   */
  @Override
  public RestRequestHandler getRestRequestHandler()
      throws InstantiationException {
    return getInstance();
  }

  /**
   * Returns an instance of {@link MockRequestResponseHandler}.
   * @return an instance of {@link MockRequestResponseHandler}.
   */
  @Override
  public RestResponseHandler getRestResponseHandler()
      throws InstantiationException {
    return getInstance();
  }

  /**
   * Returns the singleton {@link MockRequestResponseHandler} instance being maintained. Creates it if it hasn't been
   * created already.
   * @return an instance of {@link MockRequestResponseHandler}.
   */
  private static MockRequestResponseHandler getInstance() {
    if (instantiated.compareAndSet(false, true)) {
      instance = new MockRequestResponseHandler();
    }
    return instance;
  }
}
