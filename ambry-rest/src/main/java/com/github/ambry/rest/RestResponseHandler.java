package com.github.ambry.rest;

/**
 * Interface for the RestResponseHandler. Provides a way for Ambry to return responses through the RestServer
 */
public interface RestResponseHandler {
  /**
   * Add to body of the rest data we are returning and write to the channel
   *
   * @param data
   * @param isLast isLastChunk
   * @throws RestException
   */
  public void addToBody(byte[] data, boolean isLast)
      throws RestException;

  /**
   * Add to body of the rest data we are returning and flush the write to the channel
   *
   * @param data
   * @param isLast isLastChunk
   * @throws RestException
   */
  public void addToBodyAndFlush(byte[] data, boolean isLast)
      throws RestException;

  /**
   * Write the response to the channel
   *
   * @throws RestException
   */
  public void finalizeResponse()
      throws RestException;

  /**
   * Write the response to the channel and flush
   *
   * @throws RestException
   */
  public void finalizeResponseAndFlush()
      throws RestException;

  /**
   * Flush all data in the channel
   *
   * @throws RestException
   */
  public void flush()
      throws RestException;

  /**
   * Close the channel
   *
   * @throws RestException
   */
  public void close()
      throws RestException;

  /**
   * Called by the RestMessageHandler when it detects/catches an error
   */
  public void onError(Throwable cause);

  /**
   * Called by the rest server when the request is complete and the connection is inactive
   *
   * @throws Exception
   */
  public void onRequestComplete()
      throws Exception;

  // header helper functions. We will add more as we discover uses for them
  /*
      1. Haven't added one for version because this is going to be 1.1 for now
      2. Haven't added for status since status is OK unless there is an exception.
          For the exception case, we directly convert the exception error code to a response code.
          If we discover other use case, can add here.

      May need to add later - content length, keep alive
   */

  /**
   * set the content type of the response
   *
   * @param type
   * @throws RestException
   */
  public void setContentType(String type)
      throws RestException;
}
