package com.github.ambry.rest;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by {@link RestServer} and Rest infrastructure
 * ({@link com.github.ambry.rest.RestRequestHandlerController},
 * {@link com.github.ambry.rest.RestRequestHandler}).
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for {@link RestServer} and presents them for retrieval through defined APIs.
 */
class RestServerConfig {
  /**
   * The {@link com.github.ambry.rest.BlobStorageServiceFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link com.github.ambry.rest.BlobStorageService}.
   */
  @Config("rest.blob.storage.service.factory")
  public final String restBlobStorageServiceFactory;

  /**
   * The {@link com.github.ambry.rest.NioServerFactory} that needs to be used by the {@link RestServer} for
   * bootstrapping the {@link com.github.ambry.rest.NioServer}.
   */
  @Config("rest.nio.server.factory")
  @Default("com.github.ambry.rest.NettyServerFactory")
  public final String restNioServerFactory;

  /**
   * The number of {@link com.github.ambry.rest.RestRequestHandler} instances that need to be started by the
   * {@link com.github.ambry.rest.RestRequestHandlerController} to handle requests.
   */
  @Config("rest.request.handler.count")
  @Default("5")
  public final int restRequestHandlerCount;

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    restBlobStorageServiceFactory = verifiableProperties.getString("rest.blob.storage.service.factory");
    restNioServerFactory =
        verifiableProperties.getString("rest.nio.server.factory", "com.github.ambry.rest.NettyServerFactory");
    restRequestHandlerCount = verifiableProperties.getInt("rest.request.handler.count", 5);
  }
}
