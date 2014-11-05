package com.github.ambry.config;

/**
 * The configs for Coordinator. This includes configs for operations as well as for coordinator.
 */
public class CoordinatorConfig {

  /**
   * The hostname of the node upon which the coordinator runs.
   */
  @Config("coordinator.hostname")
  public final String hostname;

  /**
   * The name of the datacenter in which the coordinator is located.
   */
  @Config("coordinator.datacenter.name")
  public final String datacenterName;

  /**
   * The number of threads in the requester thread pool.
   */
  @Config("coordinator.requester.pool.size")
  @Default("100")
  public final int requesterPoolSize;

  /**
   * Timeout for operations that the coordinator issues.
   */
  @Config("coordinator.operation.timeout.ms")
  @Default("10000")
  public final int operationTimeoutMs;

  /**
  * Duration for which a data node should be unresponsive in order to be considered as soft failed when the operation
  * times out.
  */
  @Config("coordinator.node.timeout.ms")
  @Default("5000")
  public final int nodeTimeoutMs;

  /**
   * The factory class the coordinator uses to create a connection pool.
   */
  @Config("coordinator.connection.pool.factory")
  @Default("com.github.ambry.shared.BlockingChannelConnectionPoolFactory")
  public final String connectionPoolFactory;

  /**
   * Timeout for checking out a connection from the connection pool
   */
  @Config("coordinator.connection.pool.checkout.timeout.ms")
  @Default("2000")
  public final int connectionPoolCheckoutTimeoutMs;

  /**
   * Indicates if all operations should or should not do cross dc proxy calls
   */
  @Config("coordinator.cross.dc.proxy.call.enable")
  @Default("true")
  public final boolean crossDCProxyCallEnable;

  public CoordinatorConfig(VerifiableProperties verifiableProperties) {
    this.hostname = verifiableProperties.getString("coordinator.hostname");
    this.datacenterName = verifiableProperties.getString("coordinator.datacenter.name");
    this.requesterPoolSize =
        verifiableProperties.getIntInRange("coordinator.requester.pool.size", 100, 1, Integer.MAX_VALUE);
    this.operationTimeoutMs =
        verifiableProperties.getIntInRange("coordinator.operation.timeout.ms", 30000, 1, Integer.MAX_VALUE);
      this.nodeTimeoutMs =
              verifiableProperties.getIntInRange("coordinator.node.timeout.ms", 5000, 1, Integer.MAX_VALUE);
    this.connectionPoolFactory = verifiableProperties.getString("coordinator.connection.pool.factory",
        "com.github.ambry.shared.BlockingChannelConnectionPoolFactory");
    this.connectionPoolCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("coordinator.connection.pool.checkout.timeout.ms", 2000, 1, 5000);
    this.crossDCProxyCallEnable = verifiableProperties.getBoolean("coordinator.cross.dc.proxy.call.enable", true);
  }
}
