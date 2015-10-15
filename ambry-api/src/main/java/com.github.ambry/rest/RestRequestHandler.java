package com.github.ambry.rest;

/**
 * Meant to be a scaling unit that handles all incoming requests.
 * <p/>
 * The requests are usually submitted by a {@link NioServer} and handled by a {@link BlobStorageService} with the
 * RestRequestHandler forming a bridge between them to provide scaling capabilities and non-blocking behaviour.
 * <p/>
 * There might be multiple instances of RestRequestHandler (as a part of a {@link RestRequestHandlerController})
 * and the expectation is that their number can be scaled up and down independently of any other component in the
 * handling pipeline.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestHandler {

  /**
   * Does startup tasks for the RestRequestHandler. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the RestRequestHandler is unable to start.
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the RestRequestHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any requests queued after shutdown is called might be dropped.
   * <p/>
   * The {@link NioServer} is expected to have stopped queueing new requests before this function is called.
   */
  public void shutdown();

  /**
   * Submit a request for handling along with a channel on which a response to the request may be sent.
   * <p/>
   * It is possible that the {@code restRequest} is not immediately handled but en-queued to be handled at a later time.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws RestServiceException if there is any error while processing the request.
   */
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException;

  /**
   * Used to query whether the RestRequestHandler is currently in a state to handle submitted requests.
   * <p/>
   * This function should be considered only as a sanity check since there is no guarantee that the
   * RestRequestHandler is still running at the time of request submission.
   * @return {@code true} if in a state to handle submitted requests. {@code false} otherwise.
   */
  public boolean isRunning();
}
