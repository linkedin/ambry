package com.github.ambry.restservice;

/**
 * A RestRequestHandlerController is expected to start up a fixed number of {@link RestRequestHandler} instances and
 * hand them out when requested.
 * <p/>
 * It cannot expected to return a specific instance of {@link RestRequestHandler} at any point of time and may choose
 * any instance from its pool.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestHandlerController {

  /**
   * Does startup tasks for the RestRequestHandlerController. When the function returns, startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the RestRequestHandlerController. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();

  /**
   * Returns a {@link RestRequestHandler} that can be used to handle incoming requests.
   * <p/>
   * If {@link RestRequestHandlerController#start()} was not called before a call to this function, it is not guaranteed
   * that the {@link RestRequestHandler} is started and available for use.
   * <p/>
   * Multiple calls to this function (even by the same thread) can return different instances of
   * {@link RestRequestHandler} and no order/pattern can be expected.
   * @return - a {@link RestRequestHandler} that can be used to handle requests.
   * @throws RestServiceException
   */
  public RestRequestHandler getRequestHandler()
      throws RestServiceException;
}
