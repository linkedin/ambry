package com.github.ambry.rest;

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
  protected static String BLOB_STORAGE_SERVICE_FACTORY = "rest.blob.storage.service.factory";
  protected static String NIO_SERVER_FACTORY = "rest.nio.server.factory";
  protected static String REQUEST_HANDLER_COUNT_KEY = "rest.request.handler.count";

  /**
   * The {@link com.github.ambry.restservice.BlobStorageServiceFactory} that needs to be used by the {@link RestServer}
   * for bootstrapping the {@link com.github.ambry.restservice.BlobStorageService}.
   */
  @Config("rest.blob.storage.service.factory")
  private final String blobStorageServiceFactory;

  /**
   * The {@link com.github.ambry.restservice.NioServerFactory} that needs to be used by the {@link RestServer} for
   * bootstrapping the {@link com.github.ambry.restservice.NioServer}.
   */
  @Config("rest.nio.server.factory")
  @Default("com.github.ambry.rest.NettyServerFactory")
  private final String nioServerFactory;

  /**
   * The number of {@link com.github.ambry.restservice.RestRequestHandler} instances that need to be started by the
   * {@link com.github.ambry.restservice.RestRequestHandlerController} to handle requests.
   */
  @Config("rest.request.handler.count")
  @Default("5")
  private final int requestHandlerCount;

  String getBlobStorageServiceFactory() {
    return blobStorageServiceFactory;
  }

  String getNioServerFactory() {
    return nioServerFactory;
  }

  public int getRequestHandlerCount() {
    return requestHandlerCount;
  }

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    blobStorageServiceFactory = verifiableProperties.getString(BLOB_STORAGE_SERVICE_FACTORY);
    nioServerFactory = verifiableProperties.getString(NIO_SERVER_FACTORY, "com.github.ambry.rest.NettyServerFactory");
    requestHandlerCount = verifiableProperties.getInt(REQUEST_HANDLER_COUNT_KEY, 5);
  }
}
