package com.github.ambry.commons;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * The BlobId factory that creates the blobId
 */
public class BlobIdFactory implements StoreKeyFactory {

  private ClusterMap clusterMap;

  public BlobIdFactory(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  @Override
  public StoreKey getStoreKey(DataInputStream value)
      throws IOException {
    return new BlobId(value, clusterMap);
  }
}