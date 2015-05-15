package com.github.ambry.rest;

/**
 * Interface for the RestResponseHandler. Provides a way for Ambry to return responses through the RestServer
 */
public interface RestResponseHandler {
  /**
   * Add to body of the rest data we are returning and write to the channel
   * @param data
   * @param isLast isLastChunk
   * @throws Exception
   */
  public void addToBody(byte[] data, boolean isLast)
      throws Exception;

  /**
   * Add to body of the rest data we are returning and flush the write to the channel
   * @param data
   * @param isLast isLastChunk
   * @throws Exception
   */
  public void addToBodyAndFlush(byte[] data, boolean isLast)
      throws Exception;

  /**
   * Write the response to the channel
   * @throws Exception
   */
  public void finalizeResponse()
      throws Exception;

  /**
   * Write the response to the channel and flush
   * @throws Exception
   */
  public void finalizeResponseAndFlush()
      throws Exception;

  /**
   * Flush all data in the channel
   * @throws Exception
   */
  public void flush()
      throws Exception;

  /**
   * Close the channel
   * @throws Exception
   */
  public void close()
      throws Exception;

  /**
   * Called by the RestMessageHandler when it detects/catches an error
   * @throws Exception
   */
  public void onError(Throwable cause);

  /**
   * Called by the rest server when the request is complete and the connection is inactive
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
   * @param type
   * @throws Exception
   */
  public void setContentType(String type)
      throws Exception;
}
