package com.github.ambry.shared;

import com.github.ambry.store.IndexKey;
import com.github.ambry.store.IndexKeyFactory;

/**
 * The BlobId factory that creates the blobId
 */
public class BlobIdFactory implements IndexKeyFactory {

  @Override
  public IndexKey getKey(String value) {
    return new BlobId(value);
  }
}