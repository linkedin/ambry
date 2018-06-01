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
package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.ThrowingConsumer;


/**
 * Common utility functions that will be used across frontend package
 */
class FrontendUtils {

  /**
   * Fetches {@link BlobId} from the given string representation of BlobId
   * @param blobIdStr string representation of BlobId
   * @param clusterMap {@link ClusterMap} instance to use
   * @return the {@link BlobId} thus generated
   * @throws RestServiceException on invalid blobId
   */
  static BlobId getBlobIdFromString(String blobIdStr, ClusterMap clusterMap) throws RestServiceException {
    try {
      return new BlobId(blobIdStr, clusterMap);
    } catch (Exception e) {
      throw new RestServiceException("Invalid blob id=" + blobIdStr, RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * Create a {@link Callback} that handles common tasks like updating metrics via a {@link CallbackTracker} and
   * handling errors that may occur in the callback body.
   * @param <T> the result type that the callback accepts.
   * @param callbackTracker the {@link CallbackTracker} to use to update metrics for an async call. The operation is
   *                        marked as started right before the callback is constructed.
   * @param failureCallback the callback to call if this callback was called with an exception or if the
   *                        {@code successAction} call throws an exception.
   * @param successAction the action to take when the callback is called successfully. The result of the callback is
   *                      provided to this consumer. This consumer is allowed to throw an exception, in which case the
   *                      {@code failureCallback} will be called.
   * @return the managed {@link Callback}.
   */
  static <T> Callback<T> managedCallback(CallbackTracker callbackTracker, Callback<?> failureCallback,
      ThrowingConsumer<? super T> successAction) {
    callbackTracker.markOperationStart();
    return (result, exception) -> {
      try {
        callbackTracker.markOperationEnd();
        if (exception == null) {
          successAction.accept(result);
        }
      } catch (Exception e) {
        callbackTracker.markCallbackProcessingError();
        exception = e;
      } finally {
        if (exception != null) {
          failureCallback.onCompletion(null, exception);
        }
        callbackTracker.markCallbackProcessingEnd();
      }
    };
  }
}
