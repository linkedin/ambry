/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 * The final state of a single request that is tracked by operation tracker. Note that, after request is created in router,
 * it is not guaranteed to be sent out. The request may already time out during connection checkout etc. This enum is
 * consumed by operation tracker to change success/failure counter and determine whether to update Histograms.
 */
public enum TrackedRequestFinalState {
  SUCCESS, FAILURE, TIMED_OUT, NOT_FOUND, REQUEST_DISABLED, DISK_DOWN;

  /**
   *  Return the corresponding {@link TrackedRequestFinalState}  for the given {@link RouterErrorCode}.
   * @param code The {@link RouterErrorCode} to handle.
   * @return The corresponding {@link TrackedRequestFinalState}.
   */
  public static TrackedRequestFinalState fromRouterErrorCodeToFinalState(RouterErrorCode code) {
    switch (code) {
      case OperationTimedOut:
        return TIMED_OUT;
      case BlobDoesNotExist:
        return NOT_FOUND;
      default:
        return FAILURE;
    }
  }
}
