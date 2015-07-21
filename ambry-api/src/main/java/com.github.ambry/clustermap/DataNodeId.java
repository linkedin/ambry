package com.github.ambry.clustermap;

/**
 * A DataNodeId has many devices. A DataNodeId stores one or more {@link ReplicaId}s upon each device.
 */
public abstract class DataNodeId implements Resource, Comparable<DataNodeId> {

  /**
   * Gets the hostname of this DataNodeId.
   *
   * @return fully qualified domain name of the DataNodeId.
   */
  public abstract String getHostname();

  /**
   * Gets the DataNodeId's connection port.
   *
   * @return port upon which to establish a connection with the DataNodeId.
   */
  public abstract int getPort();

  /**
   * Gets the DataNodeId's SSL connection port.
   *
   * @return port upon which to establish a SSL encrypted connection with the DataNodeId
   * if no SSL port exists, returns -1
   */
  public abstract int getSSLPort();

  /**
   * Gets the state of the DataNodeId.
   *
   * @return state of the DataNodeId.
   */
  public abstract HardwareState getState();

  /**
   * Gets the DataNodeId's datacenter
   *
   * @return name of the Datacenter
   */
  public abstract String getDatacenterName();
}
