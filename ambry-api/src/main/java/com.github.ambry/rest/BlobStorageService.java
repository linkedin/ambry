package com.github.ambry.rest;

/**
 * BlobStorageService defines a service that forms the bridge b/w a RESTful frontend and a storage backend (or something
 * that communicates with a storage backend).
 * <p/>
 * Typically, a BlobStorageService is expected to receive requests from the RESTful frontend, handle them as required
 * and either send a response (if immediately available) or pass control to another component that do further handling
 * and generate a response. The information received from the scaling layer should be enough to perform these functions.
 * <p/>
 * Most operations are performed async and responses are therefore queued asynchronously instead of being available at
 * the end of the function call.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface BlobStorageService {

  /**
   * Does startup tasks for the BlobStorageService. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if BlobStorageService is unable to start.
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the BlobStorageService. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();

  /**
   * Handles a GET operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest  the {@link .RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a POST operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest  the {@link .RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a DELETE operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest  the {@link .RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a HEAD operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest  the {@link .RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel);
}
