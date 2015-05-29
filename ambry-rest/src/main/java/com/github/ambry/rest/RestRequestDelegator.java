package com.github.ambry.rest;

import java.util.concurrent.TimeUnit;


/**
 * Interface for a RestRequestDelegator
 */
public interface RestRequestDelegator {
  /**
   * Does startup tasks for the delegator
   * @throws Exception
   */
  public void start()
      throws InstantiationException;

  /**
   * Returns a RestMessageHandler that can be used to handle incoming messages
   * @return
   * @throws RestException
   */
  public RestMessageHandler getMessageHandler()
      throws RestException;

  /**
   * Does shutdown tasks for the delegator.
   * @throws Exception
   */
  public void shutdown()
      throws Exception;

  /**
   * Await shutdown of the service for a specified amount of time.
   * @param timeout
   * @param timeUnit
   * @return true if service exited within timeout. false otherwise
   * @throws InterruptedException
   */
  public boolean awaitShutdown(long timeout, TimeUnit timeUnit) throws InterruptedException;

  /**
   * To know whether the service is up. If shutdown was called, isUp is false. To check if a service has completed
   * shutting down, use isTerminated()
   * @return
   */
  public boolean isUp();

  /**
   * returns true if and only if the service was started and was subsequently shutdown and the shutdown is complete
   * @return
   */
  public boolean isTerminated();
}
