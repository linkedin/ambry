package com.github.ambry.restservice;

/**
 * Interface for a NIO (non blocking I/O) server.
 */
public interface NioServer {

  /**
   * Do startup tasks for the NioServer. Return when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Do shutdown tasks for the NioServer. Return when shutdown is FULLY complete.
   * @throws Exception
   */
  public void shutdown()
      throws Exception;
}
