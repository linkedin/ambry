package com.github.ambry.config;

/**
 * TODO: write description
 */
public class RouterConfig {

  @Config("router.operation.pool.size")
  @Default("200")
  public final int routerOperationPoolSize;

  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerOperationPoolSize = verifiableProperties.getInt("router.operation.pool.size", 200);
  }
}
