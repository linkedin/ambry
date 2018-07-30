package com.github.ambry.cloud;

public interface BlobEventConsumer {

  // Return true means event is acknowledged, false means retry
  boolean onBlobEvent(BlobEvent blobEvent);
}
