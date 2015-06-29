package com.github.ambry.restservice;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by {@link RestServer} and Rest infrastructure
 * ({@link com.github.ambry.restservice.RestRequestHandlerController},
 * {@link com.github.ambry.restservice.RestRequestHandler}).
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for {@link RestServer} and presents them for retrieval through defined APIs.
 */
class RestServerConfig {
  /**
   * The {@link com.github.ambry.restservice.BlobStorageServiceFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link com.github.ambry.restservice.BlobStorageService}.
   */
  @Config("rest.blob.storage.service.factory")
  public final String restBlobStorageServiceFactory;

  /**
   * The {@link com.github.ambry.restservice.NioServerFactory} that needs to be used by the {@link RestServer} for
   * bootstrapping the {@link com.github.ambry.restservice.NioServer}.
   */
  @Config("rest.nio.server.factory")
  @Default("com.github.ambry.restservice.NettyServerFactory")
  public final String restNioServerFactory;

  /**
   * The number of {@link com.github.ambry.restservice.RestRequestHandler} instances that need to be started by the
   * {@link com.github.ambry.restservice.RestRequestHandlerController} to handle requests.
   */
  @Config("rest.request.handler.count")
  @Default("5")
  public final int restRequestHandlerCount;

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    restBlobStorageServiceFactory = verifiableProperties.getString("rest.blob.storage.service.factory");
    restNioServerFactory =
        verifiableProperties.getString("rest.nio.server.factory", "com.github.ambry.restservice.NettyServerFactory");
    restRequestHandlerCount = verifiableProperties.getInt("rest.request.handler.count", 5);
  }
}
