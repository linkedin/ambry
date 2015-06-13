package com.github.ambry.restservice;

/**
 * Interface for the RestResponseHandler. Provides a way to return responses to the client.
 */
public interface RestResponseHandler {
  /**
   * Add to body of the rest data we are returning and write to the channel.
   *
   * @param data
   * @param isLast isLastChunk
   * @throws RestServiceException
   */
  public void addToBody(byte[] data, boolean isLast)
      throws RestServiceException;

  /**
   * Add to body of the rest data we are returning and flush the write to the channel.
   *
   * @param data
   * @param isLast isLastChunk
   * @throws RestServiceException
   */
  public void addToBodyAndFlush(byte[] data, boolean isLast)
      throws RestServiceException;

  /**
   * Write the response to the channel.
   *
   * @throws RestServiceException
   */
  public void finalizeResponse()
      throws RestServiceException;

  /**
   * Write the response to the channel and flush.
   *
   * @throws RestServiceException
   */
  public void finalizeResponseAndFlush()
      throws RestServiceException;

  /**
   * Flush all data in the channel.
   *
   * @throws RestServiceException
   */
  public void flush()
      throws RestServiceException;

  /**
   * Close the channel.
   *
   * @throws RestServiceException
   */
  public void close()
      throws RestServiceException;

  /**
   * Used to notify the response handler that an error has occurred.
   */
  public void onError(Throwable cause);

  /**
   * Called when the request is complete.
   *
   * @throws Exception
   */
  public void onRequestComplete()
      throws Exception;

  // header helper functions. We will add more as we discover uses for them
  /*
      1. Haven't added one for version because that is going to be 1.1 for now
      2. Haven't added for status since status is OK unless there is an exception.
          For the exception case, we directly convert the exception error code to a response code.
          If we discover other use cases, can add here.

      May need to add later - content length, keep alive
   */

  /**
   * set the content type of the response
   *
   * @param type
   * @throws RestServiceException
   */
  public void setContentType(String type)
      throws RestServiceException;
}
