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
 * An operation progress tracker that assists in keeping track of the chunk(GetChunk) or an operation(GetBlobInfo) state.
 */
class ProgressTracker {
  private final OperationTracker operationTracker;
  private CryptoJobStatusTracker cryptoJobStatusTracker;
  private CryptoJobType cryptoJobType;

  /**
   * Instantiates {@link ProgressTracker}
   * @param operationTracker {@link OperationTracker} instance to assist in tracking the status of the operation based on
   *                                                 requests sent to the storage nodes.
   */
  ProgressTracker(OperationTracker operationTracker) {
    this.operationTracker = operationTracker;
  }

  /**
   * Initializes the {@link CryptoJobStatusTracker}
   */
  void initializeCryptoJobTracker(CryptoJobType cryptoJobType) {
    cryptoJobStatusTracker = new CryptoJobStatusTracker();
    this.cryptoJobType = cryptoJobType;
  }

  /**
   * @return {@code true} if crypto job is required. {@code false} otherwise
   */
  boolean isCryptoJobRequired() {
    return cryptoJobStatusTracker != null;
  }

  /**
   * Sets crypto job as succeeded
   */
  void setCryptoJobSuccess() {
    if (cryptoJobStatusTracker == null) {
      throw new IllegalStateException("No crypto job.");
    }
    cryptoJobStatusTracker.setSucceeded();
  }

  /**
   * Sets crypto job as failed
   */
  void setCryptoJobFailed() {
    if (cryptoJobStatusTracker == null) {
      throw new IllegalStateException("No crypto job.");
    }
    cryptoJobStatusTracker.setFailed();
  }

  /**
   * Determines if an operation has completed (either succeeded or failed).
   *
   * @return {@code true} if the operation has completed.
   */
  boolean isDone() {
    return operationTracker.isDone() && (cryptoJobStatusTracker == null || cryptoJobStatusTracker.isDone());
  }

  /**
   * Determines if an operation has succeeded.
   * @return {@code true} if the operation has successfully completed. {@code false} if the operation has failed
   */
  boolean hasSucceeded() {
    if ((!isDone())) {
      throw new IllegalStateException(new RouterException("hasSucceeded called before operation is complete",
          RouterErrorCode.UnexpectedInternalError));
    }
    return operationTracker.hasSucceeded() && (cryptoJobStatusTracker == null || cryptoJobStatusTracker.hasSucceeded());
  }

  /**
   * @return the crypto job type (encryption, decryption).  Returns {@code NULL} if no crypto job was set.
   */
  CryptoJobType getCryptoJobType() {
    return cryptoJobType;
  }
}

enum CryptoJobType {
  ENCRYPTION, DECRYPTION
}

/**
 * Tracks encryption/decryption job status
 */
class CryptoJobStatusTracker {
  private boolean succeeded;
  private boolean done;

  /**
   * Updates the crypto job status as completed and succeeded
   */
  void setSucceeded() {
    succeeded = true;
    done = true;
  }

  /**
   * Updates the crypto job status as completed and failed
   */
  void setFailed() {
    done = true;
    succeeded = false;
  }

  /**
   * @return {@code true} if the crypto job is completed. {@code false} otherwise
   */
  boolean isDone() {
    return done;
  }

  /**
   * @return {@code true} if the crypto job has succeeded. {@code false} if failed.
   */
  boolean hasSucceeded() {
    return succeeded;
  }
}
