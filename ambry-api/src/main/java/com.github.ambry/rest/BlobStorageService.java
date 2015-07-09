package com.github.ambry.rest;

/**
 * BlobStorageService defines a service that forms the bridge b/w a RESTful frontend and a storage backend.
 * <p/>
 * Typically, a BlobStorageService is expected to receive requests (maybe in parts) from the RESTful frontend,
 * process them as required and write the response back to the client. The information received from the frontend
 * should be enough to perform these functions.
 * <p/>
 * It is possible that some operations are performed async and therefore a return from any of the functions does not
 * guarantee operation completion but the response to the client will be sent as soon as the operation is complete.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface BlobStorageService {

  /**
   * Does startup tasks for the BlobStorageService. When the function returns, startup is FULLY complete.
   * @throws InstantiationException
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
   * The {@link RestRequestInfo} provided will have {@link RestRequestMetadata} that provides metadata about the
   * request, optionally some content as {@link RestRequestContent} and a {@link RestResponseHandler} to send a response
   * back to the client.
   * <p/>
   * The received {@link RestRequestInfo} may only be a part of the whole request.
   * <p/>
   * It is expected that the request will be handled and an appropriate response returned to the client via the provided
   * {@link RestResponseHandler}. It is possible that multiple parts have to be accumulated before a response can be
   * sent. This is OK as long as a response is eventually sent to the client.
   * <p/>
   * Any exceptions should be bubbled up with the appropriate {@link RestServiceErrorCode}. Error messages are not
   * expected to be sent to the client as soon as an exception is detected since they have to be translated into
   * protocol specific codes that the client can understand (like HTTP error codes). This is the responsibility of the
   * {@link RestResponseHandler}.
   * @param restRequestInfo - a {@link RestRequestInfo} object representing a part of the request.
   * @throws RestServiceException
   */
  public void handleGet(RestRequestInfo restRequestInfo)
      throws RestServiceException;

  /**
   * Handles a POST operation.
   * <p/>
   * The {@link RestRequestInfo} provided will have {@link RestRequestMetadata} that provides metadata about the
   * request, optionally some content as {@link RestRequestContent} and a {@link RestResponseHandler} to send a response
   * back to the client.
   * <p/>
   * The received {@link RestRequestInfo} may only be a part of the whole request.
   * <p/>
   * It is expected that the request will be handled and an appropriate response returned to the client via the provided
   * {@link RestResponseHandler}. It is possible that multiple parts have to be accumulated before a response can be
   * sent. This is OK as long as a response is eventually sent to the client.
   * <p/>
   * Any exceptions should be bubbled up with the appropriate {@link RestServiceErrorCode}. Error messages are not
   * expected to be sent to the client as soon as an exception is detected since they have to be translated into
   * protocol specific codes that the client can understand (like HTTP error codes). This is the responsibility of the
   * {@link RestResponseHandler}.
   * @param restRequestInfo - a {@link RestRequestInfo} object representing a part of the request.
   * @throws RestServiceException
   */
  public void handlePost(RestRequestInfo restRequestInfo)
      throws RestServiceException;

  /**
   * Handles a DELETE operation.
   * <p/>
   * The {@link RestRequestInfo} provided will have {@link RestRequestMetadata} that provides metadata about the
   * request, optionally some content as {@link RestRequestContent} and a {@link RestResponseHandler} to send a response
   * back to the client.
   * <p/>
   * The received {@link RestRequestInfo} may only be a part of the whole request.
   * <p/>
   * It is expected that the request will be handled and an appropriate response returned to the client via the provided
   * {@link RestResponseHandler}. It is possible that multiple parts have to be accumulated before a response can be
   * sent. This is OK as long as a response is eventually sent to the client.
   * <p/>
   * Any exceptions should be bubbled up with the appropriate {@link RestServiceErrorCode}. Error messages are not
   * expected to be sent to the client as soon as an exception is detected since they have to be translated into
   * protocol specific codes that the client can understand (like HTTP error codes). This is the responsibility of the
   * {@link RestResponseHandler}.
   * @param restRequestInfo - a {@link RestRequestInfo} object representing a part of the request.
   * @throws RestServiceException
   */
  public void handleDelete(RestRequestInfo restRequestInfo)
      throws RestServiceException;

  /**
   * Handles a HEAD operation.
   * <p/>
   * The {@link RestRequestInfo} provided will have {@link RestRequestMetadata} that provides metadata about the
   * request, optionally some content as {@link RestRequestContent} and a {@link RestResponseHandler} to send a response
   * back to the client.
   * <p/>
   * The received {@link RestRequestInfo} may only be a part of the whole request.
   * <p/>
   * It is expected that the request will be handled and an appropriate response returned to the client via the provided
   * {@link RestResponseHandler}. It is possible that multiple parts have to be accumulated before a response can be
   * sent. This is OK as long as a response is eventually sent to the client.
   * <p/>
   * Any exceptions should be bubbled up with the appropriate {@link RestServiceErrorCode}. Error messages are not
   * expected to be sent to the client as soon as an exception is detected since they have to be translated into
   * protocol specific codes that the client can understand (like HTTP error codes). This is the responsibility of the
   * {@link RestResponseHandler}.
   * @param restRequestInfo - a {@link RestRequestInfo} object representing a part of the request.
   * @throws RestServiceException
   */
  public void handleHead(RestRequestInfo restRequestInfo)
      throws RestServiceException;
}
