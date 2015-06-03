package com.github.ambry.rest;

/**
 * Interface for a rest server
 */

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
}
