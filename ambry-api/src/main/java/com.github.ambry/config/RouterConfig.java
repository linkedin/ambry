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
  @Config("router.operation.pool.size")
  @Default("200")
  public final int routerOperationPoolSize;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerOperationPoolSize = verifiableProperties.getInt("router.operation.pool.size", 200);
  }
}
