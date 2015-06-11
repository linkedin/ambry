package com.github.ambry.restservice;

/**
 * Interface for RestContent
 */
public interface RestContent extends RestObject {
  /**
   * Used to check if this is the last chunk of a particular request
   *
   * @return whether this is the last chunk
   */
  public boolean isLast();

  public byte[] getBytes();
}
