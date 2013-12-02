package com.github.ambry.coordinator;

/**
 * Errors that the Coordinator returns
 */
public enum CoordinatorError {
  // General errors
  /**
   * Coordinator experienced an unexpected internal error. This indicates a bug in the coordinator implementation. An
   * operation that changes the state of a blob (put, delete, cancelTTL) may have partially completed and so may
   * eventually complete in the future.
   */
  UnexpectedInternalError,
  /**
   * Coordinator caught InterruptedException. An operation that changes the state of a blob (put, delete, cancelTTL) may
   * have partially completed and so may eventually complete in the future.
   */
  Interrupted,
  /**
   * Insufficient Ambry DataNodes could be contacted to complete an operation. An operation that changes the state of a
   * blob (put, delete, cancelTTL) may have partially completed and so may eventually complete in the future.
   */
  AmbryUnavailable,
  /**
   * Operation did not complete within specified time out. An operation that changes the state of a blob (put, delete,
   * cancelTTL) may have partially completed and so may eventually complete in the future.
   */
  OperationTimedOut,

  // Errors on write path
  /**
   * Insufficient capacity available in Ambry for object to be stored.
   */
  InsufficientCapacity,
  /**
   * Blob is too large. Cannot store blob of such size.
   */
  BlobTooLarge,

  // Errors on read path
  /**
   * No Blob could be found for specified blob id.
   */
  BlobDoesNotExist,
  /**
   * Blob has been deleted and so cannot be retrieved.
   */
  BlobDeleted,
  /**
   * TTL of Blob has expired and so Blob cannot be retrieved.
   */
  BlobExpired
}
