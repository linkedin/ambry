/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 * An operation progress tracker that assist in keeping track of the chunk(GetChunk) or an operation(GetBlobInfo) state.
 * Exposes methods like {@link #isDone()} and {@link #hasSucceeded()} to assist in tracking when the operation is complete
 * and if yes, whether it succeeded or failed.
 */
class ProgressTracker {
  OperationTracker operationTracker;
  DecryptionStatusTracker decryptionStatusTracker;

  /**
   * Instantiates {@link ProgressTracker}
   * @param operationTracker {@link OperationTracker} instance to assist in tracking the status of the operation based on
   *                                                 requests sent to the storage nodes.
   */
  ProgressTracker(OperationTracker operationTracker) {
    this.operationTracker = operationTracker;
  }

  /**
   * Sets the {@link DecryptionStatusTracker}
   * @param decryptionStatusTracker {@link DecryptionStatusTracker} to assist in tracking the status of decryption
   */
  void setDecryptionStatusTracker(DecryptionStatusTracker decryptionStatusTracker) {
    this.decryptionStatusTracker = decryptionStatusTracker;
  }

  void setDecryptionSuccess() {
    decryptionStatusTracker.setSucceeded();
  }

  void setDecryptionFailed() {
    decryptionStatusTracker.setFailed();
  }

  /**
   * Determines if an operation has completed (either succeeded or failed).
   *
   * @return {@code true} if the operation has completed.
   */
  boolean isDone() {
    return operationTracker.isDone() && (decryptionStatusTracker == null || decryptionStatusTracker.isDone());
  }

  /**
   * Determines if an operation has succeeded. This has to be called only if {@link #isDone()} returns true.
   * Bcoz a false returned by this method means that operation has failed.
   *
   * @return {@code true} if the operation has successfully completed. {@code false} if the operation has failed
   */
  boolean hasSucceeded() {
    return operationTracker.hasSucceeded() && (decryptionStatusTracker == null
        || decryptionStatusTracker.hasSucceeded());
  }
}
