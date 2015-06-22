package com.github.ambry.restservice;

/**
 * The RestResponseHandler is a meant to provide a {@link NioServer} implementation independent way to return responses
 * to the client. It deals with data in terms of bytes only and is not concerned with different types of data that might
 * need to be returned from the {@link BlobStorageService}.
 * <p/>
 * This functionality is mostly required by implementations of {@link BlobStorageService} since they are agnostic to
 * both the REST protocol being used and the framework used for the implementation of {@link NioServer}.
 * <p/>
 * Typically, the RestResponseHandler wraps the APIs provided by the framework used for implementation of
 * {@link NioServer}  to return responses to clients.
 * <p/>
 * Implementations are expected to be thread-safe but use with care across different threads since there are neither
 * ordering guarantees nor operation success guarantees (e.g. if an external thread closes the channel while a write
 * attempt is in progress).
 */
public interface RestResponseHandler {
  /**
   * Adds data to the body of the response. Requests a write to the underlying channel before returning.
   * <p/>
   * This method might not have sent data to the wire upon return. The byte array provided as argument might still be in
   * use.
   * <p/>
   * Be sure to call {@link RestResponseHandler#flush()} once you want to send all pending data to the actual transport.
   * @param data - the bytes of data that need to be written.
   * @param isLast - whether this is the last piece of the response.
   * @throws RestServiceException
   */
  public void addToResponseBody(byte[] data, boolean isLast)
      throws RestServiceException;

  /**
   * Flushes all pending messages in the channel to transport. A response of OK is returned if no response body was
   * constructed (i.e if there were no {@link RestResponseHandler#addToResponseBody(byte[], boolean)} calls).
   * @throws RestServiceException
   */
  public void flush()
      throws RestServiceException;

  /**
   * Closes the channel. No further communication will be possible.
   * <p/>
   * No response will be sent if neither a response body was constructed (i.e if there were no
   * {@link RestResponseHandler#addToResponseBody(byte[], boolean)} calls) nor a {@link RestResponseHandler#flush()} was
   * called.
   * <p/>
   * Any pending writes (that are not already flushed) might be discarded.
   * @throws RestServiceException
   */
  public void close()
      throws RestServiceException;

  /**
   * Notifies the RestResponseHandler that an error has occurred in handling the request.
   * <p/>
   * The expectation is that an appropriate error response will be constructed, returned to the client and the
   * connection closed (if required).
   * @param restRequestMetadata - the metadata of the request that failed.
   * @param cause - the cause of the error.
   */
  public void onError(RestRequestMetadata restRequestMetadata, Throwable cause);

  /**
   * Notifies that request handling is complete and tasks that need to be done after handling of a request is complete
   * can proceed.
   * <p/>
   * Any cleanup code is expected to go here but no data can be sent to the client since the connection might be
   * closed/severed before this is called.
   * <p/>
   * A request is considered to be complete when all the {@link RestRequestInfo}s associated with the request have been
   * acknowledged as handled and no more {@link RestRequestInfo}s associated with the same request are expected.
   * <p/>
   * This is (has to be) called regardless of the request being concluded successfully or unsuccessfully
   * (e.g. connection interruption).
   */
  public void onRequestComplete(RestRequestMetadata restRequestMetadata);

  // Header helper functions.
  //
  // We will add more as we discover uses for them.
  // 1. Haven't added one for http version because that is going to be 1.1 for now.
  // 2. Haven't added for status since status is OK unless there is an exception.
  //
  // For the exception case, we directly convert the exception error code to a response code.
  //
  // May need to add later - content length, keep alive
  // If we discover other use cases, can add here.

  /**
   * Sets the content-type of the response. Expected to be MIME types.
   * @param type - the content-type of the data in the response.
   * @throws RestServiceException
   */
  public void setContentType(String type)
      throws RestServiceException;
}
