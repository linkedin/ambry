package com.github.ambry.rest;

/**
 * The RestResponseHandler is meant to provide a {@link NioServer} implementation independent way to return responses
 * to the client. It deals with data in terms of bytes only and is not concerned with different types of data that might
 * need to be returned from the {@link BlobStorageService}.
 * <p/>
 * This functionality is mostly required by implementations of {@link BlobStorageService} since they are agnostic to
 * both the REST protocol being used and the framework used for the implementation of {@link NioServer}.
 * <p/>
 * Typically, the RestResponseHandler wraps the APIs provided by the framework used for an implementation of
 * {@link NioServer} to return responses to clients.
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
   * If the write fails sometime in the future, the channel may be closed.
   * <p/>
   * Be sure to call {@link RestResponseHandler#flush()} once you want to send all pending data to the actual transport.
   * @param data the bytes of data that need to be written.
   * @param isLast whether this is the last piece of the response.
   * @throws RestServiceException if there is an error adding to response body or with writing to the channel.
   */
  public void addToResponseBody(byte[] data, boolean isLast)
      throws RestServiceException;

  /**
   * Flushes all pending messages in the channel to transport. A response of OK is returned if no response body was
   * constructed (i.e if there were no {@link RestResponseHandler#addToResponseBody(byte[], boolean)} calls).
   * @throws RestServiceException if there is an error while flushing to channel.
   */
  public void flush()
      throws RestServiceException;

  /**
   * Notifies that request handling is complete (successfully or unsuccessfully) and tasks that need to be done after
   * handling of a request is complete can proceed (e.g. cleanup code + closing of connection if not keepalive).
   * <p/>
   * If cause is not not null, then it indicates that there was an error while handling the request and cause defines
   * the error that occurred. The expectation is that an appropriate error response will be constructed, returned to the
   * client if possible and the connection closed (if required).
   * <p/>
   * It is possible that the connection might be closed/severed before this is called. Therefore this function needs to
   * always check if the channel of communication with the client is still open if it wants to send data.
   * <p/>
   * A request is considered to be complete when no more {@link RestRequestInfo}s associated with the same request are
   * expected at the {@link NioServer} (they might still be in the other layers) or if there was an error while
   * handling the request or if the client timed out.
   * <p/>
   * This is (has to be) called regardless of the request being concluded successfully or unsuccessfully
   * (e.g. connection interruption).
   * <p/>
   * This operation has to be idempotent.
   * @param cause if an error occurred, the cause of the error. Otherwise null.
   * @param forceClose whether the connection needs to be forcibly closed.
   */
  public void onRequestComplete(Throwable cause, boolean forceClose);

  /**
   * Returns completion status of the request represented by the given {@link RestRequestMetadata}.
   * @return {@code true} if there has been a call to {@link #onRequestComplete(Throwable, boolean)} and the request has
   * been marked complete. {@code false} otherwise.
   */
  public boolean isRequestComplete();

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
   * @param type the content-type of the data in the response.
   * @throws RestServiceException if there is an error while setting the content-type.
   */
  public void setContentType(String type)
      throws RestServiceException;
}
