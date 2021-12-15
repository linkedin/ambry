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
package com.github.ambry.quota;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Callback for charging request cost against quota. Used by {@link QuotaEnforcer}s to chargeIfUsageWithinQuota quota for a request.
 */
public interface QuotaChargeCallback {
  Logger logger = LoggerFactory.getLogger(QuotaChargeCallback.class);

  /**
   * Build {@link QuotaChargeCallback} to handle quota compliance of requests.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @return QuotaChargeCallback object.
   */
  static QuotaChargeCallback buildQuotaChargeCallback(RestRequest restRequest, QuotaManager quotaManager) {
    RequestCostPolicy requestCostPolicy = new UserQuotaRequestCostPolicy(quotaManager.getQuotaConfig());
    return new QuotaChargeCallback() {
      @Override
      public boolean checkAndCharge(long chunkSize) throws RestServiceException {
        Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), entry -> entry.getValue()));
        boolean charged = quotaManager.chargeIfUsageWithinQuota(restRequest, null, requestCost);
        if (quotaManager.getQuotaMode() == QuotaMode.THROTTLING) {
          return charged;
        }
        return true;
      }

      @Override
      public boolean checkAndCharge() throws RestServiceException {
        return checkAndCharge(quotaManager.getQuotaConfig().quotaAccountingUnit);
      }

      @Override
      public boolean check() {
        return false;
      }

      @Override
      public boolean chargeIfQuotaExceedAllowed(long chunkSize) throws RestServiceException {
        Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), entry -> entry.getValue()));
        boolean charged = quotaManager.chargeIfQuotaExceedAllowed(restRequest, null, requestCost);
        if (quotaManager.getQuotaMode() == QuotaMode.THROTTLING) {
          return charged;
        }
        return true;
      }

      @Override
      public boolean chargeIfQuotaExceedAllowed() throws RestServiceException {
        return chargeIfQuotaExceedAllowed(quotaManager.getQuotaConfig().quotaAccountingUnit);
      }

      @Override
      public QuotaResource getQuotaResource() throws RestServiceException {
        return QuotaUtils.getQuotaResourceId(restRequest);
      }

      @Override
      public QuotaMethod getQuotaMethod() {
        return QuotaUtils.getQuotaMethod(restRequest);
      }
    };
  }

  /**
   * Callback method that can be used to checkAndCharge against quota is usage is within quota.
   * @param chunkSize of the chunk.
   * @return {@code true} if usage is within quota and checkAndCharge succeeded. {@code false} otherwise.
   * @throws RestServiceException in case of any exception.
   */
  boolean checkAndCharge(long chunkSize) throws RestServiceException;

  /**
   * Callback method that can be used to checkAndCharge against quota is usage is within quota. Call this method
   * when the quota checkAndCharge doesn't depend on the chunk size.
   * @return {@code true} if usage is within quota and checkAndCharge succeeded. {@code false} otherwise.
   * @throws RestServiceException in case of any exception.
   */
  boolean checkAndCharge() throws RestServiceException;

  /**
   * Check if request should be throttled based on quota usage.
   * @return {@code true} if request usage exceeds limit and request should be throttled. {@code false} otherwise.
   */
  boolean check();

  /**
   * Check if usage is allowed to exceed the quota limit.
   * @return {@code true} if usage is allowed to exceed the quota limit. {@code false} otherwise.
   * @throws RestServiceException in case of any exception.
   */
  boolean chargeIfQuotaExceedAllowed() throws RestServiceException;

  /**
   * Check if usage is allowed to exceed the quota limit.
   * @return {@code true} if usage is allowed to exceed the quota limit. {@code false} otherwise.
   * @throws RestServiceException in case of any exception.
   */
  boolean chargeIfQuotaExceedAllowed(long chunkSize) throws RestServiceException;

  /**
   * @return QuotaResource object.
   * @throws RestServiceException in case of any errors.
   */
  QuotaResource getQuotaResource() throws RestServiceException;

  /**
   * @return QuotaMethod object.
   */
  QuotaMethod getQuotaMethod();
}
