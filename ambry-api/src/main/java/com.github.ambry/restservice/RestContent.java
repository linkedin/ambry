package com.github.ambry.restservice;

/**
 * Interface for RestContent - piece of content in a request.
 */
public interface RestContent extends RestObject {
  /**
   * Used to check if this is the last chunk of a particular request.
   *
   * @return whether this is the last chunk
   */
  public boolean isLast();

  /**
   * Get the content as a byte array.
   * @return
   */
  public byte[] getBytes();
}
