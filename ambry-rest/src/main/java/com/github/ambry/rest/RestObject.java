package com.github.ambry.rest;

/**
 * Interface for RestObject. A Generic object that both RestServer and Ambry can understand
 */
public interface RestObject {
  /**
   * Clean up any resources and do work that needs to be done at the end of object lifecycle
   */
  public void release();
}
