package com.github.ambry.config;

/**
 * Configuration parameters required by a Router.
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
    routerRequestTimeoutMs = verifiableProperties.getInt("router.request.timeout.ms", 2000);
    routerMaxPutChunkSizeBytes = verifiableProperties.getInt("router.max.put.chunk.size.bytes", 4 * 1024 * 1024);
    routerPutRequestParallelism = verifiableProperties.getInt("router.put.request.parallelism", 3);
    routerPutSuccessTarget = verifiableProperties.getInt("router.put.success.target", 2);
  }
}
