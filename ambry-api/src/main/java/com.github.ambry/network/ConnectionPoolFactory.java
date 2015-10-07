package com.github.ambry.network;

/**
 *  Factory to create a ConnectionPool.
 */
public interface ConnectionPoolFactory {

  /**
   * Gets a connection pool
   * @return The connection pool that was created
   */
  public ConnectionPool getConnectionPool()
      throws Exception;
}
