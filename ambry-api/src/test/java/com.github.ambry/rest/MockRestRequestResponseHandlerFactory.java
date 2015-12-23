package com.github.ambry.rest;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Implementation of {@link RestRequestHandlerFactory} and {@link RestResponseHandlerFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for {@link MockRestRequestResponseHandler}. Maintains a single instance of
 * {@link MockRestRequestResponseHandler} and returns the same instance on any call to {@link #getRestRequestHandler()} or
 * {@link #getRestResponseHandler()}.
 */
public class MockRestRequestResponseHandlerFactory implements RestRequestHandlerFactory, RestResponseHandlerFactory {
  private static final AtomicBoolean instantiated = new AtomicBoolean(false);
  private static MockRestRequestResponseHandler instance;

  public MockRestRequestResponseHandlerFactory(Object handlerCount, Object restServerMetrics) {
  }

  public MockRestRequestResponseHandlerFactory(Object handlerCount, Object restServerMetrics,
      BlobStorageService blobStorageService) {
    MockRestRequestResponseHandler requestHandler = getInstance();
    requestHandler.setBlobStorageService(blobStorageService);
  }

  /**
   * Returns an instance of {@link MockRestRequestResponseHandler}.
   * @return an instance of {@link MockRestRequestResponseHandler}.
   */
  @Override
  public RestRequestHandler getRestRequestHandler()
      throws InstantiationException {
    return getInstance();
  }

  /**
   * Returns an instance of {@link MockRestRequestResponseHandler}.
   * @return an instance of {@link MockRestRequestResponseHandler}.
   */
  @Override
  public RestResponseHandler getRestResponseHandler()
      throws InstantiationException {
    return getInstance();
  }

  /**
   * Returns the singleton {@link MockRestRequestResponseHandler} instance being maintained. Creates it if it hasn't been
   * created already.
   * @return an instance of {@link MockRestRequestResponseHandler}.
   */
  private static MockRestRequestResponseHandler getInstance() {
    if (instantiated.compareAndSet(false, true)) {
      instance = new MockRestRequestResponseHandler();
    }
    return instance;
  }
}
