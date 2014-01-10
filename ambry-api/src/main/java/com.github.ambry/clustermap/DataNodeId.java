package com.github.ambry.clustermap;

/**
 * A DataNodeId has many devices. A DataNodeId stores one or more {@link ReplicaId}s upon each device.
 */
public interface DataNodeId {

  /**
   * Gets the hostname of this DataNodeId.
   *
   * @return fully qualified domain name of the DataNodeId.
   */
  public String getHostname();

  /**
   * Gets the DataNodeId's connection port.
   *
   * @return port upon which to establish a connection with the DataNodeId.
   */
  public int getPort();

  /**
   * Gets the state of the DataNodeId.
   *
   * @return state of the DataNodeId.
   */
  public HardwareState getState();

  /**
   * Gets the DataNodeId's datacenter
   *
   * @return name of the Datacenter
   */
  public String getDatacenterName();
}
