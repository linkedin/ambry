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
  @Config("rest.server.blob.storage.service.factory")
  public final String restServerBlobStorageServiceFactory;

  /**
   * The {@link NioServerFactory} that needs to be used by the {@link RestServer} for
   * bootstrapping the {@link NioServer}.
   */
  @Config("rest.server.nio.server.factory")
  @Default("com.github.ambry.rest.NettyServerFactory")
  public final String restServerNioServerFactory;

  /**
   * The number of {@link AsyncRequestResponseHandler} instances that need to be started by the
   * {@link RequestResponseHandlerController} to handle requests and responses.
   */
  @Config("rest.server.scaling.unit.count")
  @Default("5")
  public final int restServerScalingUnitCount;

  /**
   * The {@link com.github.ambry.router.RouterFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link com.github.ambry.router.Router}.
   */
  @Config("rest.server.router.factory")
  @Default("com.github.ambry.router.CoordinatorBackedRouterFactory")
  public final String restServerRouterFactory;

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    restServerBlobStorageServiceFactory = verifiableProperties.getString("rest.server.blob.storage.service.factory");
    restServerNioServerFactory =
        verifiableProperties.getString("rest.server.nio.server.factory", "com.github.ambry.rest.NettyServerFactory");
    restServerScalingUnitCount = verifiableProperties.getInt("rest.server.scaling.unit.count", 5);
    restServerRouterFactory = verifiableProperties
        .getString("rest.server.router.factory", "com.github.ambry.router.CoordinatorBackedRouterFactory");
  }
}
