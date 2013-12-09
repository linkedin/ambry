package com.github.ambry.store;

import java.io.DataInputStream;
import java.io.IOException;

public class MockIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getStoreKey(DataInputStream value) throws IOException {
    return new MockId(value);
  }
}
