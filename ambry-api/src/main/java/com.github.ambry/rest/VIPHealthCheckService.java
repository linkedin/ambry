package com.github.ambry.rest;

/**
 * Service that maintains the health of the system to respond to VIP health check requests
 */
public class VIPHealthCheckService {
  public enum State {
    /**
     * The front end is up and running.
     */
    UP,
    /**
     * Frontend has been shutdown.
     */
    DOWN
  }

  private volatile State frontEndState = State.DOWN;
  private final String healthCheckUri;

  public VIPHealthCheckService(String healthCheckUri){
    this.healthCheckUri = healthCheckUri;
  }

  public String getHealthCheckUri(){
    return this.healthCheckUri;
  }

  public void startUp() {
    this.frontEndState = State.UP;
  }

  public boolean isServiceUp() {
    return (frontEndState == State.UP);
  }

  public void shutdown() {
    this.frontEndState = State.DOWN;
  }
}
