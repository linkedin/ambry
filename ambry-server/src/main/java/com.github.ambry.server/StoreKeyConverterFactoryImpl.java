package com.github.ambry.server;

import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;


/**
 * Default StoreKeyConverterFactoryImpl.  Creates StoreKeyConverterImplNoOp
 */
class StoreKeyConverterFactoryImpl implements StoreKeyConverterFactory {

  @Override
  public StoreKeyConverter getStoreKeyConverter() throws InstantiationException {
    return new StoreKeyConverterImplNoOp();
  }
}
