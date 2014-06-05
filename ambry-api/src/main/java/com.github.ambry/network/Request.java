package com.github.ambry.network;

import java.io.InputStream;


/**
 * Simple request
 */
public interface Request {
  /**
   * The request as an input stream is returned to the caller
   * @return The inputstream that represents the request
   */
  InputStream getInputStream();

  /**
   * Gets the start time in ms when this request started
   * @return The start time in ms when the request started
   */
  long getStartTimeInMs();
}
