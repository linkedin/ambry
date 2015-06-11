package com.github.ambry.restservice;

/**
 * Interface for RestObject. A Generic object that both NIOServer and Ambry can understand
 */
public interface RestObject {
  /**
   * Clean up any resources and do work that needs to be done at the end of object lifecycle
   */
  public void release();
}
