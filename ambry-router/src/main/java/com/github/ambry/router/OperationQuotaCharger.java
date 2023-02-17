/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.quota.Chargeable;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaResource;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link Chargeable} implementation for blob operations that ensures charging happens only once per chunk.
 */
public class OperationQuotaCharger implements Chargeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperationQuotaCharger.class);
  private final QuotaChargeCallback quotaChargeCallback;
  private final String operationName;
  private final NonBlockingRouterMetrics routerMetrics;
  private BlobId blobId;
  private boolean isCharged;
  private long chunkSize;
  private int numAttempts;

  /**
   * Constructor for {@link OperationQuotaCharger}.
   *
   * @param quotaChargeCallback {@link QuotaChargeCallback} object to charge and check quotas.
   * @param blobId {@link BlobId} of the blob for which quota will be charged.
   * @param operationName Name of the operation.
   */
  public OperationQuotaCharger(QuotaChargeCallback quotaChargeCallback, BlobId blobId, String operationName,
      NonBlockingRouterMetrics routerMetrics) {
    this.quotaChargeCallback = quotaChargeCallback;
    this.blobId = blobId;
    this.operationName = operationName;
    this.routerMetrics = routerMetrics;
    this.isCharged = false;
    this.chunkSize = -1;
    numAttempts = 0;
  }

  /**
   * Constructor for {@link OperationQuotaCharger}.
   *
   * @param quotaChargeCallback {@link QuotaChargeCallback} object to charge and check quotas.
   * @param operationName Name of the operation.
   */
  public OperationQuotaCharger(QuotaChargeCallback quotaChargeCallback, String operationName,
      NonBlockingRouterMetrics routerMetrics) {
    this(quotaChargeCallback, null, operationName, routerMetrics);
  }

  @Override
  public QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed) {
    QuotaAction quotaAction = QuotaAction.ALLOW;
    if (Objects.isNull(quotaChargeCallback) || isCharged) {
      return quotaAction;
    }
    try {
      quotaAction = checkAndChargeHelper(shouldCheckExceedAllowed);
      isCharged = quotaAction == QuotaAction.ALLOW;
    } catch (QuotaException quotaException) {
      numAttempts++;
      // When there is an exception, we let the request through, because we don't want to affect user's request due to
      // any issue with quota system. But we don't set isCharged flag to true when the exception is retryable, so that
      // if charging is attempted again (due to chunk request parallelism), the charge can be passed down.
      LOGGER.warn("Quota charging failed in {} for blob {} due to {}. Number of attempts {}", operationName,
          getBlobIdStr(), quotaException.toString(), numAttempts);
      if (!quotaException.isRetryable()) {
        isCharged = true;
      }
    } catch (Exception exception) {
      routerMetrics.unknownExceptionInChargeableRate.mark();
      LOGGER.warn("Quota charging failed in {} for blob {} due to {}. This should never happen.", operationName,
          getBlobIdStr(), exception.toString(), numAttempts);
      isCharged = true;
    }
    return quotaAction;
  }

  @Override
  public QuotaResource getQuotaResource() {
    if (Objects.isNull(quotaChargeCallback)) {
      if (!operationName.equals(ReplicateBlobOperation.class.getSimpleName())) {
        LOGGER.warn("quota charge callback is null for operation: {}", operationName);
      }
      return null;
    }
    try {
      return quotaChargeCallback.getQuotaResource();
    } catch (Exception exception) {
      if (!(exception instanceof QuotaException)) {
        routerMetrics.unknownExceptionInChargeableRate.mark();
      }
      LOGGER.error(
          "Could not create QuotaResource object during {} operation for the chunk {} due to {}. This should never happen.",
          operationName, getBlobIdStr(), exception.toString());
    }
    // A null return means quota resource could not be created for this chunk. The consumer should decide how to handle nulls.
    return null;
  }

  @Override
  public QuotaMethod getQuotaMethod() {
    if (Objects.isNull(quotaChargeCallback)) {
      if (!operationName.equals(ReplicateBlobOperation.class.getSimpleName())) {
        LOGGER.warn("quota charge callback is null for operation: {}", operationName);
      }
      return null;
    }
    return quotaChargeCallback.getQuotaMethod();
  }

  /**
   * Return the chunkSize representing the size of chunk being uploaded or downloaded.
   * For operations like delete and ttlupdate that don't download or upload data, the size is -1.
   * @return chunkSize value.
   */
  public long getChunkSize() {
    return chunkSize;
  }

  /**
   * Set the chunkSize representing the size of chunk being uploaded or downloaded.
   * @param chunkSize chunkSize value.
   */
  public void setChunkSize(long chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * @return BlobId for the operation or chunk.
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * Set the blobId.
   * @param blobId {@link BlobId} object.
   */
  public void setBlobId(BlobId blobId) {
    this.blobId = blobId;
  }

  /**
   * Perform {@link QuotaChargeCallback#checkAndCharge} with or without chunkSize.
   * @param shouldCheckExceedAllowed if {@code true} then it should be checked if usage is allowed to exceed quota.
   * @return QuotaAction representing the recommended action to take.
   * @throws QuotaException in case of any exception which checking or charging for quota.
   */
  private QuotaAction checkAndChargeHelper(boolean shouldCheckExceedAllowed) throws QuotaException {
    if (chunkSize == -1) {
      return quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false);
    } else {
      return quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false, chunkSize);
    }
  }

  /**
   * @return an appropriate string representation of blobId.
   */
  private String getBlobIdStr() {
    return Objects.isNull(blobId) ? "null" : blobId.toString();
  }
}
