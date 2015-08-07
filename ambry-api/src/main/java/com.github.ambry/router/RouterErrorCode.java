package com.github.ambry.router;

import com.github.ambry.coordinator.CoordinatorError;


/**
 * TODO: write description
 */
public enum RouterErrorCode {
  // General errors. May occur for any operation.
  /**
   * Router experienced an unexpected internal error. The caller should retry the operation. An operation that
   * changes the state of an existing blob (delete, cancelTTL) may have partially completed and so may eventually
   * complete in the future.
   */
  UnexpectedInternalError,
  /**
   * Insufficient Ambry DataNodes could be contacted to successfully complete an operation. The caller should retry the
   * operation. An operation that changes the state of an existing blob (delete, cancelTTL) may have partially completed
   * and so may eventually complete in the future.
   */
  AmbryUnavailable,
  /**
   * Operation did not complete within specified time out. The caller should retry the operation. An operation that
   * changes the state of an existing blob (delete, cancelTTL) may have partially completed and so may eventually
   * complete in the future.
   */
  OperationTimedOut,

  /**
   * Caller passed in an invalid BlobId and so operation could not be attempted. May occur for getBlobProperties,
   * getBlobUserMetadata, getBlob, delete, or cancelTTL operations.
   */
  InvalidBlobId,
  /**
   * Caller passed in an illegal argument for put operation. May occur for put operation.
   */
  InvalidPutArgument,

  // Errors on write path. May occur for put operations.
  /**
   * Insufficient capacity available in Ambry for object to be stored.
   */
  InsufficientCapacity,
  /**
   * Blob is too large. Cannot store blob of such size.
   */
  BlobTooLarge,

  // Errors on read path. May occur for getBlobProperties, getBlobUserMetadata, or getBlob operations.
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
  BlobExpired;

  public static RouterErrorCode convertCoordinatorErrorToRouterErrorCode(CoordinatorError error) {
    try {
      return RouterErrorCode.valueOf(error.toString());
    } catch (IllegalArgumentException e) {
      return RouterErrorCode.UnexpectedInternalError;
    }
  }
}
