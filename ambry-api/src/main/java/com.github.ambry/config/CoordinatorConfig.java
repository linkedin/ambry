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
  @Default("30000")
  public final int operationTimeoutMs;

  /**
   * The factory class the coordinator uses to create a connection pool.
   */
  @Config("coordinator.connection.pool.factory")
  @Default("com.github.ambry.coordinator.ConnectionPoolFactory")
  public final String connectionPoolFactory;

  public CoordinatorConfig(VerifiableProperties verifiableProperties) {
    this.hostname = verifiableProperties.getString("coordinator.hostname");
    this.datacenterName = verifiableProperties.getString("coordinator.datacenter.name");
    this.requesterPoolSize =
            verifiableProperties.getIntInRange("coordinator.requester.pool.size", 100, 1, Integer.MAX_VALUE);
    this.operationTimeoutMs =
            verifiableProperties.getIntInRange("coordinator.operation.timeout.ms", 30000, 1, Integer.MAX_VALUE);
    this.connectionPoolFactory = verifiableProperties.getString("coordinator.connection.pool.factory", "com.github.ambry.coordinator.ConnectionPoolFactory");
  }
}
