package com.github.ambry.rest;

/**
 * Interface for a RestRequestDelegator
 */
public interface RestRequestDelegator {
  /**
   * Does startup tasks for the delegator
   * @throws Exception
   */
  public void start()
      throws Exception;

  /**
   * Returns a RestMessageHandler that can be used to handle incoming messages
   * @return
   * @throws RestException
   */
  public RestMessageHandler getMessageHandler()
      throws RestException;

  /**
   * Does shutdown tasks for the delegator
   * @throws Exception
   */
  public void shutdown()
      throws Exception;
}
