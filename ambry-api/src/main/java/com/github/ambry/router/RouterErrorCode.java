/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

/**
 * All the error codes that accompany a {@link RouterException}.
 */
public enum RouterErrorCode {
  // General errors. May occur for any operation.
  /**
   * Insufficient Ambry DataNodes could be contacted to successfully complete an operation. The caller should retry the
   * operation. An operation that changes the state of an existing blob (delete) may have partially completed and so may
   * eventually complete in the future.
   */
  AmbryUnavailable,

  /**
   * Caller passed in an invalid blob id and so operation could not be attempted. May occur for
   * {@link Router#getBlob(String, GetBlobOptions)}, {@link Router#deleteBlob(String, String)} (and their variants) operations.
   */
  InvalidBlobId,

  /**
   * Caller passed in an illegal argument for
   * {@link Router#putBlob(com.github.ambry.messageformat.BlobProperties, byte[], ReadableStreamChannel, PutBlobOptions)}
   * operation (and its variant).
   */
  InvalidPutArgument,

  /**
   * Operation did not complete within specified time out. The caller should retry the operation. An operation that
   * changes the state of an existing blob (delete) may have partially completed and so may eventually complete in the
   * future.
   */
  OperationTimedOut,

  /**
   * Thrown when an operation is attempted after the {@link Router} is closed.
   */
  RouterClosed,

  /**
   * Router experienced an unexpected internal error. The caller should retry the operation. An operation that
   * changes the state of an existing blob (delete) may have partially completed and so may eventually complete in the
   * future.
   */
  UnexpectedInternalError,

  // Errors on write path. May occur for put operations.
  /**
   * Blob is too large. Cannot store blob of such size.
   */
  BlobTooLarge,

  /**
   * Unexpected error reading from the input channel for puts.
   */
  BadInputChannel,

  /**
   * Insufficient capacity available in Ambry for object to be stored.
   */
  InsufficientCapacity,

  // Errors on read path. May occur for getBlobInfo, getBlob or deleteBlob operations.

  /**
   * Blob has been deleted and so cannot be retrieved.
   */
  BlobDeleted,

  /**
   * No Blob could be found for specified blob id.
   */
  BlobDoesNotExist,

  /**
   * TTL of Blob has expired and so Blob cannot be retrieved.
   */
  BlobExpired,

  /**
   * The range offsets provided for a getBlob operation are invalid for the specified blob.
   */
  RangeNotSatisfiable,

  /**
   * The channel returned to the user in a getBlob operation has been closed before operation completion.
   */
  ChannelClosed,

  /**
   * The update has been rejected
   */
  BlobUpdateNotAllowed,

  /**
   * ContainerId or AccountId from blobId doesn't match these in store server.
   */
  BlobAuthorizationFailure,

  /**
   * Blob already undeleted so it can't not be undeleted again. Should delete this blob before undelete.
   */
  BlobUndeleted,

  /**
   * LifeVersions from two responses are different. eg, undelete responses returns two different lifeVersions.
   */
  LifeVersionConflict,

  /**
   * Blob is not yet deleted. For undelete, a blob needs to be deleted first.
   */
  BlobNotDeleted,
}
