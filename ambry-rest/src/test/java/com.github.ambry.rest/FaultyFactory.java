package com.github.ambry.rest;

import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;


/**
 * Factory that returns null on any function.
 * <p/>
 * Public because factories are usually constructed via {@link com.github.ambry.utils.Utils#getObj(String, Object...)}
 */
public class FaultyFactory implements BlobStorageServiceFactory, NioServerFactory, RouterFactory {

  // for BlobStorageServiceFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3, Object obj4) {
    // don't care.
  }

  // for NioServerFactory and RouterFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3) {
    // don't care.
  }

  @Override
  public BlobStorageService getBlobStorageService()
      throws InstantiationException {
    return null;
  }

  @Override
  public NioServer getNioServer()
      throws InstantiationException {
    return null;
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    return null;
  }
}