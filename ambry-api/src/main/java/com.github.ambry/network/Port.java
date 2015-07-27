package com.github.ambry.network;

/**
 * Represents a port containing port number and {@PortType}
 */
public class Port {
  private final int port;
  private final PortType type;

  public Port(int port, PortType type) {
    this.port = port;
    this.type = type;
  }

  public int getPort() {
    return this.port;
  }

  public PortType getPortType() {
    return this.type;
  }

  @Override
  public String toString() {
    return "Port[" + getPort() + ":" + getPortType() + "]";
  }
}
