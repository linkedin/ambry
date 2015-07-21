package com.github.ambry.network;

/**
 * Represents a port containing port number and {@PortType}
 */
public class Port {
  private final int portNo;
  private final PortType type;

  public Port(int port, PortType type) {
    this.portNo = port;
    this.type = type;
  }

  public int getPortNo() {
    return this.portNo;
  }

  public PortType getPortType() {
    return this.type;
  }

  @Override
  public String toString() {
    return "Ports[" + getPortNo() + ":" + getPortType() + "]";
  }
}
