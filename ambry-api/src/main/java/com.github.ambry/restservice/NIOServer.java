package com.github.ambry.restservice;

/**
 * Interface for a NIO (non blocking I/O) server.
 */
public interface NioServer {

  /**
   * Do startup tasks for the NioServer. Returns when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Do shutdown tasks for the NioServer. Returns when shutdown is FULLY complete.
   */
  public void shutdown();
}
