package com.github.ambry.rest;

/**
 * Service that maintains the health of the server
 */
public class RestServerState {
  private volatile boolean serviceUp = false;
  private final String healthCheckUri;

  public RestServerState(String healthCheckUri) {
    this.healthCheckUri = healthCheckUri;
  }

  public String getHealthCheckUri() {
    return healthCheckUri;
  }

  public void markServiceUp() {
    serviceUp = true;
  }

  public boolean isServiceUp() {
    return serviceUp;
  }

  public void markServiceDown() {
    serviceUp = false;
  }
}
