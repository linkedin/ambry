/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaUtils;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tracks callbacks for {@link TtlUpdateOperation} and {@link UndeleteOperation} over multiple chunks of a single blob
 */
class BatchOperationCallbackTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchOperationCallbackTracker.class);
  static final FutureResult<Void> DUMMY_FUTURE = new FutureResult<>();

  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final long numBlobIds;
  private final AtomicBoolean finalOperationReadyToDo = new AtomicBoolean(false);
  private final BlobId finalBlobId;
  private final BiConsumer<BlobId, Callback> finalOperation;
  private final QuotaChargeCallback quotaChargeCallback;
  private final ConcurrentMap<BlobId, Boolean> blobIdToAck = new ConcurrentHashMap<>();
  private final AtomicLong ackedCount = new AtomicLong(0);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final NonBlockingRouter nonBlockingRouter;

  /**
   * Constructor
   * @param blobIds the {@link BlobId}s being tracked
   * @param finalBlobId the final {@link BlobId} to send after all the {@code blobids} are acked.
   * @param futureResult the {@link FutureResult} to be triggered once acks are received for all blobs
   * @param callback the {@link Callback} to be triggered once acks are received for all blobs
   * @param quotaChargeCallback The {@link QuotaChargeCallback} to be triggered to account for quota usage.
   * @param finalOperation The operation to call on the {@code finalBlobId}.
   * @param nonBlockingRouter The non-blocking router object
   */
  BatchOperationCallbackTracker(List<BlobId> blobIds, BlobId finalBlobId, FutureResult<Void> futureResult,
      Callback<Void> callback, QuotaChargeCallback quotaChargeCallback, BiConsumer<BlobId, Callback> finalOperation,
      NonBlockingRouter nonBlockingRouter) {
    numBlobIds = blobIds.size();
    blobIds.forEach(blobId -> blobIdToAck.put(blobId, false));
    if (blobIdToAck.size() != numBlobIds) {
      throw new IllegalArgumentException("The list of BlobIds provided has duplicates: " + blobIds);
    }
    this.futureResult = futureResult;
    this.callback = callback;
    this.quotaChargeCallback = quotaChargeCallback;
    this.finalOperation = finalOperation;
    this.finalBlobId = finalBlobId;
    this.nonBlockingRouter = nonBlockingRouter;
  }

  /**
   * Gets a {@link Callback} personalized for {@code blobId}.
   * @param blobId the {@link BlobId} for which the
   * @return the {@link Callback} to be used with the {@link TtlUpdateOperation} and {@link UndeleteOperation} for {@code blobId}.
   */
  Callback<Void> getCallback(final BlobId blobId) {
    return (result, exception) -> {
      if (exception == null) {
        if (!blobIdToAck.containsKey(blobId)) {
          complete(new RouterException("Ack for unknown " + blobId + " arrived",
              RouterErrorCode.UnexpectedInternalError));
        } else if (blobIdToAck.put(blobId, true)) {
          // already acked once
          complete(new RouterException("Ack for " + blobId + " arrived more than once",
              RouterErrorCode.UnexpectedInternalError));
        } else if (ackedCount.incrementAndGet() >= numBlobIds) {
          // acked for the first time for this blob id and all the blob ids have been acked
          if(finalOperationReadyToDo.compareAndSet(false, true)) {
            // if final operation hasn't been started yet, then start it.
            blobIdToAck.put(finalBlobId, false);
            nonBlockingRouter.currentOperationsCount.incrementAndGet();
            finalOperation.accept(finalBlobId, getCallback(finalBlobId));
          }

          if(blobId.equals(finalBlobId)) {
              complete(null);
          }
        }
      } else {
        complete(exception);
      }
    };
  }

  /**
   * @return if the operation is completed. Used for tests.
   */
  boolean isCompleted() {
    return completed.get();
  }

  /**
   * Completes the batch operation
   * @param e the {@link Exception} that occurred (if any).
   */
  private void complete(Exception e) {
    if (completed.compareAndSet(false, true)) {
      if (QuotaUtils.postProcessCharge(quotaChargeCallback)) {
        try {
          quotaChargeCallback.checkAndCharge(false, true);
        } catch (QuotaException quotaException) {
          LOGGER.info("Exception {} while charging quota for ttl operation", quotaException.toString());
        }
      }
      nonBlockingRouter.completeOperation(futureResult, callback, null, e, false);
    }
  }
}
