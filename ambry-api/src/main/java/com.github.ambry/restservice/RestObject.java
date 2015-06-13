package com.github.ambry.restservice;

/**
 * Interface for RestObject - A generic object that both NioServer and Ambry can understand and exchange info through
 */
public interface RestObject {
  /**
   * Clean up any resources and do work that needs to be done at the end of object lifecycle.
   */
  public void release();
}
