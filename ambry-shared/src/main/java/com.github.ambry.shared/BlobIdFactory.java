package com.github.ambry.shared;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;

import java.io.IOException;
import java.io.DataInputStream;

/**
 * The BlobId factory that creates the blobId
 */
public class BlobIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getStoreKey(DataInputStream value) throws IOException {
    return new BlobId(value);
  }
}