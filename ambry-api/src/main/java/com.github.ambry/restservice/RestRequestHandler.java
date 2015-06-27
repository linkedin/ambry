package com.github.ambry.restservice;

/**
 * Meant to be a scaling unit that handles all incoming requests.
 * <p/>
 * The requests are usually submitted by a {@link NioServer} (maybe in parts) and handled by a
 * {@link BlobStorageService} with the RestRequestHandler forming a bridge between them to provide scaling capabilities
 * and non-blocking behaviour.
 * <p/>
 * There might be multiple instances of RestRequestHandler (as a part of a {@link RestRequestHandlerController}) and the
 * expectation is that their number can be scaled up and down independently of any other component in the handling
 * pipeline.
 * <p/>
 * All parts of the same request need to be handled by the same RestRequestHandler since ordering of content is
 * important in requests. Since the {@link RestRequestHandlerController} is not guaranteed to return the same instance
 * of RestRequestHandler at any point of time, it is the responsibility of the {@link NioServer} to use the same
 * RestRequestHandler for all parts of the same request.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestHandler {

  /**
   * Does startup tasks for the RestRequestHandler. When the function returns, startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the RestRequestHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * The {@link NioServer} is expected to have stopped queueing new requests before this function is called.
   */
  public void shutdown();

  /**
   * Submit a request for handling.
   * <p/>
   * Every submitted {@link RestRequestInfo} may only be a part of a full request. All parts of the same request will be
   * expected to have the same {@link RestRequestMetadata} in the {@link RestRequestInfo}s that are submitted.
   * <p/>
   * It is possible that the {@link RestRequestInfo} is not immediately handled but en-queued to be handled at a later
   * time. If this is the case, then the implementation will (is expected to) notify listeners that are registered to
   * listen to events on the {@link RestRequestInfo} of handling completion.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be handled.
   * @throws RestServiceException
   */
  public void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException;

  /**
   * Notifies that request handling is complete and tasks that need to be done after handling of a request is complete
   * can proceed.
   * <p/>
   * Any cleanup code for destroying state maintained per request or for objects that need to be released
   * is expected to go here.
   * <p/>
   * A request is considered to be complete when all the {@link RestRequestInfo}s associated with the request have been
   * acknowledged as handled and no more {@link RestRequestInfo}s associated with the same request are expected.
   * <p/>
   * This is (has to be) called regardless of the request being concluded successfully or unsuccessfully
   * (e.g. connection interruption).
   * <p/>
   * This operation has to be idempotent.
   * @param restRequestMetadata - the metadata of the request that just completed.
   * @throws Exception
   */
  public void onRequestComplete(RestRequestMetadata restRequestMetadata);
}
