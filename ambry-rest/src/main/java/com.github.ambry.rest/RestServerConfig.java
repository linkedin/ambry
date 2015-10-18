package com.github.ambry.rest;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by {@link RestServer} and Rest infrastructure
 * ({@link RequestResponseHandlerController}, {@link AsyncRequestResponseHandler}).
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for {@link RestServer} and presents them for retrieval through defined APIs.
 */
class RestServerConfig {
  /**
   * The {@link BlobStorageServiceFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link BlobStorageService}.
   */
  @Config("rest.blob.storage.service.factory")
  public final String restBlobStorageServiceFactory;

  /**
   * The {@link NioServerFactory} that needs to be used by the {@link RestServer} for
   * bootstrapping the {@link NioServer}.
   */
  @Config("rest.nio.server.factory")
  @Default("NettyServerFactory")
  public final String restNioServerFactory;

  /**
   * The number of {@link AsyncRequestResponseHandler} instances that need to be started by the
   * {@link RequestResponseHandlerController} to handle requests and responses.
   */
  @Config("rest.scaling.unit.count")
  @Default("5")
  public final int restScalingUnitCount;

  /**
   * The {@link com.github.ambry.router.RouterFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link com.github.ambry.router.Router}.
   */
  @Config("rest.router.factory")
  @Default("com.github.ambry.router.CoordinatorBackedRouterFactory")
  public final String restRouterFactory;

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    restBlobStorageServiceFactory = verifiableProperties.getString("rest.blob.storage.service.factory");
    restNioServerFactory =
        verifiableProperties.getString("rest.nio.server.factory", "com.github.ambry.rest.NettyServerFactory");
    restScalingUnitCount = verifiableProperties.getInt("rest.scaling.unit.count", 5);
    restRouterFactory =
        verifiableProperties.getString("rest.router.factory", "com.github.ambry.router.CoordinatorBackedRouterFactory");
  }
}
