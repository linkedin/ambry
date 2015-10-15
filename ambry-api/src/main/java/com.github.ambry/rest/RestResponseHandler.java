package com.github.ambry.rest;

import com.github.ambry.router.ReadableStreamChannel;


/**
 * Meant to be a scaling unit that handles all outgoing responses.
 * <p/>
 * The responses are submitted from the {@link BlobStorageService} or from any layers that sit behind it.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestResponseHandler {
  /**
   * Does startup tasks for the RestResponseHandler. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the RestResponseHandler is unable to start.
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the RestResponseHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any responses queued after shutdown is called might be dropped.
   */
  public void shutdown();

  /**
   * Submit a response for a request along with a channel over which the response can be sent. If the response building
   * was unsuccessful for any reason, the details are included in the {@code exception}.
   * <p/>
   * The bytes consumed from the {@code response} are streamed out (unmodified) through the
   * {@code restResponseChannel}.
   * <p/>
   * Assumed that at least one of {@code response} or {@code exception} is null.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the
   *                                {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws RestServiceException if there is any error while processing the response.
   */
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException;

  /**
   * Used to query whether the RestResponseHandler is currently in a state to handle submitted responses.
   * <p/>
   * This function should be considered only as a sanity check since there is no guarantee that the RestResponseHandler
   * is still running at the time of response submission.
   * @return {@code true} if in a state to handle submitted responses. {@code false} otherwise.
   */
  public boolean isRunning();
}
