package com.github.ambry.rest;

/**
 * Interface for a rest server
 */

import java.util.concurrent.TimeUnit;


/**
 * Components required for LI:
 * 1. Config (UC)
 * 2. Blob storage service
 * 3. Metrics
 * 4. VIP request handler
 * 5. JMX reporter?
 * 6. Public access log
 */
public interface RestServer {
  public void start()
      throws InstantiationException;

  public void shutdown()
      throws Exception;

  /**
   * Await shutdown of the service for a specified amount of time.
   *
   * @param timeout
   * @param timeUnit
   * @return true if service exited within timeout. false otherwise
   * @throws InterruptedException
   */
  public boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException;

  /**
   * To know whether the service is up. If shutdown was called, isUp is false. To check if a service has completed
   * shutting down, use isTerminated()
   *
   * @return
   */
  public boolean isUp();

  /**
   * returns true if and only if the service was started and was subsequently shutdown and the shutdown is complete
   *
   * @return
   */
  public boolean isTerminated();
}
