package com.github.ambry.store;

import java.io.DataInputStream;
import java.io.IOException;


/**
 * Factory that creates the store token from an inputstream
 */
public class StoreFindTokenFactory implements FindTokenFactory {
  private StoreKeyFactory factory;

  public StoreFindTokenFactory(StoreKeyFactory factory) {
    this.factory = factory;
  }

  @Override
  public FindToken getFindToken(DataInputStream stream)
      throws IOException {
    return StoreFindToken.fromBytes(stream, factory);
  }

  @Override
  public FindToken getNewFindToken() {
    return new StoreFindToken();
  }
}
