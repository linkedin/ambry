package com.github.ambry.notification;

/**
 * The enumeration of all the sources by which a replica can be created in the system
 */
public enum BlobReplicaSourceType {
  /**
   * The blob replica was created by a primary write. This means that the blob
   * was written directly to the server
   */
  PRIMARY,
  /**
   * The blob replica was created by a repair operation. This could be because the primary
   * write failed or because a replica needs to be restored from a repair
   */
  REPAIRED
}
