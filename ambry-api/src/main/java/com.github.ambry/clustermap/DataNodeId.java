/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.github.ambry.network.Port;
import java.util.ArrayList;


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
   * @return port upon which to establish an SSL encrypted connection with the DataNodeId
   * if no SSL port exists, throws IllegalArgumentException
   */
  public abstract int getSSLPort();

  /**
   * Returns true is SSL port exists for the DataNodeId.
   *
   * @return true if SSL port exists for the datanode, false otherwise
   */
  public abstract boolean hasSSLPort();

  /**
   * Returns the Port to connect to based on the whether the {@DataNodeId} belongs to the list of ssl-enabled
   * Datacenters
   * @param sslEnabledDataCenters List of ssl enabled Datacenters
   * @return {@Port} to which the caller can connect to
   */
  public abstract Port getPortToConnectTo(ArrayList<String> sslEnabledDataCenters);

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
