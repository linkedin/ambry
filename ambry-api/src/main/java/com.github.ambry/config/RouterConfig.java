/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
 * Configuration parameters required by a {@link com.github.ambry.router.Router}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for a Router and presents them for retrieval through defined APIs.
 */
public class RouterConfig {

  /**
   * Number of independent scaling units for the router.
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
   * Timeout for requests issued by the router to the network layer.
   */
  @Config("router.request.timeout.ms")
  @Default("2000")
  public final int routerRequestTimeoutMs;

  /**
   * The max chunk size to be used for put operations.
   */
  @Config("router.max.put.chunk.size.bytes")
  @Default("4*1024*1024")
  public final int routerMaxPutChunkSizeBytes;

  /**
   * The maximum number of parallel requests issued at a time by the put manager for a chunk.
   */
  @Config("router.put.request.parallelism")
  @Default("3")
  public final int routerPutRequestParallelism;

  /**
   * The minimum number of successful responses required for a put operation.
   */
  @Config("router.put.success.target")
  @Default("2")
  public final int routerPutSuccessTarget;

  /**
   * The maximum number of times to retry putting any chunk of a put operation
   */
  @Config("router.max.slipped.put.attempts")
  @Default("1")
  public final int routerMaxSlippedPutAttempts;

  /**
   * The maximum number of parallel requests allowed for a delete operation.
   */
  @Config("router.delete.request.parallelism")
  @Default("12")
  public final int routerDeleteRequestParallelism;

  /**
   * The minimum number of successful responses required for a delete operation.
   */
  @Config("router.delete.success.target")
  @Default("2")
  public final int routerDeleteSuccessTarget;

  /**
   * The maximum number of parallel requests issued at a time by the get manager for a get operation on a chunk.
   */
  @Config("router.get.request.parallelism")
  @Default("2")
  public final int routerGetRequestParallelism;

  /**
   * The minimum number of successful responses required for a get operation on a chunk.
   */
  @Config("router.get.success.target")
  @Default("1")
  public final int routerGetSuccessTarget;

  /**
   * Indicates whether get operations are allowed to make requests to nodes in remote data centers.
   */
  @Config("router.get.cross.dc.enabled")
  @Default("true")
  public final boolean routerGetCrossDcEnabled;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerScalingUnitCount = verifiableProperties.getIntInRange("router.scaling.unit.count", 1, 1, Integer.MAX_VALUE);
    routerHostname = verifiableProperties.getString("router.hostname");
    routerDatacenterName = verifiableProperties.getString("router.datacenter.name");
    routerScalingUnitMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.plain.text", 5, 1, 20);
    routerScalingUnitMaxConnectionsPerPortSsl =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.ssl", 2, 1, 20);
    routerConnectionCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("router.connection.checkout.timeout.ms", 1000, 1, 5000);
    routerRequestTimeoutMs = verifiableProperties.getInt("router.request.timeout.ms", 2000);
    routerMaxPutChunkSizeBytes = verifiableProperties.getInt("router.max.put.chunk.size.bytes", 4 * 1024 * 1024);
    routerPutRequestParallelism = verifiableProperties.getInt("router.put.request.parallelism", 3);
    routerPutSuccessTarget = verifiableProperties.getInt("router.put.success.target", 2);
    routerMaxSlippedPutAttempts = verifiableProperties.getInt("router.max.slipped.put.attempts", 1);
    routerDeleteRequestParallelism = verifiableProperties.getInt("router.delete.request.parallelism", 12);
    routerDeleteSuccessTarget = verifiableProperties.getInt("router.delete.success.target", 2);
    routerGetRequestParallelism = verifiableProperties.getInt("router.get.request.parallelism", 2);
    routerGetSuccessTarget = verifiableProperties.getInt("router.get.success.target", 1);
    routerGetCrossDcEnabled = verifiableProperties.getBoolean("router.get.cross.dc.enabled", true);
  }
}
