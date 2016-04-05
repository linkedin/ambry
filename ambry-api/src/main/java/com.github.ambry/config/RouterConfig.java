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
package com.github.ambry.config;

/**
 * Configuration parameters required by a Router.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for a Router and presents them for retrieval through defined APIs.
 */
public class RouterConfig {

  /**
   * Number of background threads to perform coordinator operations in CoordinatorBackedRouter.
   */
  @Config("router.scaling.unit.count")
  @Default("1")
  public final int routerScalingUnitCount;

  /**
   * The hostname of the node upon which the router runs.
   */
  @Config("router.hostname")
  public final String routerHostname;

  /**
   * The name of the datacenter in which the router is located.
   */
  @Config("router.datacenter.name")
  public final String routerDatacenterName;

  /**
   * The max connections allowed per (datanode, port) for plain text
   */
  @Config("router.scaling.unit.max.connections.per.port.plain.text")
  @Default("5")
  public final int routerScalingUnitMaxConnectionsPerPortPlainText;

  /**
   * The max connections allowed per (datanode, port) for ssl
   */
  @Config("router.scaling.unit.max.connections.per.port.ssl")
  @Default("2")
  public final int routerScalingUnitMaxConnectionsPerPortSsl;

  /**
   * Timeout for checking out an available connection to a (datanode, port).
   */
  @Config("router.connection.checkout.timeout.ms")
  @Default("1000")
  public final int routerConnectionCheckoutTimeoutMs;

  /**
   * The max chunk size to be used for put operations.
   */
  @Config("router.max.put.chunk.size.bytes")
  @Default("4*1024*1024")
  public final int routerMaxPutChunkSizeBytes;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerScalingUnitCount = verifiableProperties.getIntInRange("router.scaling.unit.count", 1, 0, 10);
    routerHostname = verifiableProperties.getString("router.hostname");
    routerDatacenterName = verifiableProperties.getString("router.datacenter.name");
    routerScalingUnitMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.plain.text", 5, 1, 20);
    routerScalingUnitMaxConnectionsPerPortSsl =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.ssl", 2, 1, 20);
    routerConnectionCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("router.connection.checkout.timeout.ms", 1000, 1, 5000);
    routerMaxPutChunkSizeBytes = verifiableProperties.getInt("router.max.put.chunk.size.bytes", 4 * 1024 * 1024);
  }
}
