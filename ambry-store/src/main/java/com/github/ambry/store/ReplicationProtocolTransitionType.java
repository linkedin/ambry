package com.github.ambry.store;

public enum ReplicationProtocolTransitionType {
  /**
   * Pre restart protocol: NA
   * Bootstrap status: NA
   * Post restart protocol: Blob based
   */
  NEW_PARTITION_TO_BLOB_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: NA
   * Bootstrap status: NA
   * Post restart protocol: File based
   */
  NEW_PARTITION_TO_FILE_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: Complete
   * Post restart protocol: File based
   */
  BLOB_BASED_COMPLETE_TO_FILE_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: Complete
   * Post restart protocol: Blob based
   */
  FILE_BASED_COMPLETE_TO_BLOB_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: Complete
   * Post restart protocol: Blob based
   */
  BLOB_BASED_COMPLETE_TO_BLOB_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: Complete
   * Post restart protocol: File based
   */
  FILE_BASED_COMPLETE_TO_FILE_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: InComplete
   * Post restart protocol: Blob based
   */
  BLOB_BASED_INCOMPLETE_TO_BLOB_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: InComplete
   * Post restart protocol: File based
   */
  FILE_BASED_INCOMPLETE_TO_FILE_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: InComplete
   * Post restart protocol: File based
   */
  BLOB_BASED_INCOMPLETE_TO_FILE_BASED_BOOTSTRAP,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: InComplete
   * Post restart protocol: Blob based
   */
  FILE_BASED_INCOMPLETE_TO_BLOB_BASED_BOOTSTRAP
}
