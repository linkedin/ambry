package com.github.ambry.shared;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;

/**
 * The BlobId factory that creates the blobId
 */
public class BlobIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getKey(String value) {
    return new BlobId(value);
  }
}