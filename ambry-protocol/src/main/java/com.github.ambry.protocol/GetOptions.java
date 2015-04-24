package com.github.ambry.protocol;

/**
 * The list of options for the Get request.
 */
public enum GetOptions {
  /**
   * This is the default. This returns all blobs that are not expired and not deleted
   */
  None,
  /**
   * Indicates that the blob should be returned even if it is expired
   */
  Include_Expired_Blobs,
  /**
   * Indicates that the blob should be returned even if it is deleted
   */
  Include_Deleted_Blobs,
  /**
   * Indicates that the blob should be returned regardless of whether it is deleted or expired
   */
  Include_All
}
