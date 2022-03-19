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
package com.github.ambry.quota;

import com.github.ambry.account.Account;
import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Common utility functions that can be used across implementations of Quota interfaces.
 */
public class QuotaUtils {
  public final static long BYTES_IN_GB = 1024 * 1024 * 1024; // 1GB

  /**
   * Returns checks if user request quota should be applied to the request.
   * Request quota should not be applied to Admin requests and OPTIONS requests.
   * @param restRequest {@link RestRequest} object.
   * @return {@code true} if user request quota should be applied to the request. {@code false} otherwise.
   */
  public static boolean isRequestResourceQuotaManaged(RestRequest restRequest) {
    RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
    return !(restRequest.getRestMethod() == RestMethod.OPTIONS || requestPath.matchesOperation(Operations.GET_PEERS)
        || requestPath.matchesOperation(Operations.GET_CLUSTER_MAP_SNAPSHOT) || requestPath.matchesOperation(
        Operations.ACCOUNTS) || requestPath.matchesOperation(Operations.STATS_REPORT) || requestPath.matchesOperation(
        Operations.ACCOUNTS_CONTAINERS));
  }

  /**
   * Create {@link QuotaResource} for the specified {@link RestRequest}.
   *
   * @param restRequest {@link RestRequest} object.
   * @return QuotaResource extracted from headers of {@link RestRequest}.
   */
  public static QuotaMethod getQuotaMethod(RestRequest restRequest) {
    return isReadRequest(restRequest) ? QuotaMethod.READ : QuotaMethod.WRITE;
  }

  /**
   * @return {@code true} if the request is a read request. {@code false} otherwise.
   */
  private static boolean isReadRequest(RestRequest restRequest) {
    switch (restRequest.getRestMethod()) {
      case GET:
      case OPTIONS:
      case HEAD:
        return true;
      default:
        return false;
    }
  }

  /**
   * Build {@link QuotaChargeCallback} to handle quota compliance of requests.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param isQuotaEnforcedOnRequest flag indicating if request quota should be enforced after charging. Requests like
   *                                 updatettl, delete etc are charged, but quota is not enforced on them.
   * @return QuotaChargeCallback object.
   */
  public static QuotaChargeCallback buildQuotaChargeCallback(RestRequest restRequest, QuotaManager quotaManager,
      boolean isQuotaEnforcedOnRequest) {
    if (!quotaManager.getQuotaConfig().bandwidthThrottlingFeatureEnabled) {
      return new PostProcessQuotaChargeCallback(quotaManager, restRequest, isQuotaEnforcedOnRequest);
    } else {
      return new PreProcessQuotaChargeCallback(quotaManager, restRequest);
    }
  }

  /**
   * @param quotaChargeCallback {@link QuotaChargeCallback} object.
   * @return {@code true} if the charge needs to happen AFTER a request is processed. {@code false} otherwise.
   */
  public static boolean postProcessCharge(QuotaChargeCallback quotaChargeCallback) {
    // Bandwidth throttling based implementation of quota enforcement will need ensure quota compliance BEFORE sending
    // the chunk request to server. Hence return false if bandwidth throttling feature is enabled.
    return quotaChargeCallback != null && !quotaChargeCallback.getQuotaConfig().bandwidthThrottlingFeatureEnabled;
  }

  /*
   * @return QuotaName of the CU quota associated with {@link RestRequest}.
   */
  public static QuotaName getCUQuotaName(RestRequest restRequest) {
    return isReadRequest(restRequest) ? QuotaName.READ_CAPACITY_UNIT : QuotaName.WRITE_CAPACITY_UNIT;
  }

  /**
   * Calculate the storage cost incurred to serve a request.
   * @param restRequest {@link RestRequest} to find type of request.
   * @param size size of the blob or chunk.
   * @return storage cost.
   */
  public static double calculateStorageCost(RestRequest restRequest, long size) {
    return RestUtils.isUploadRequest(restRequest) ? size / (double) QuotaUtils.BYTES_IN_GB : 0;
  }

  /**
   * Calculate percentage usage based on specified limit and usage values. A limit of less than or equal to 0 is assumed
   * to denote 100% usage.
   *
   * @param limit max allowed usage.
   * @param usage actual usage value.
   * @return percentage of usage.
   */
  public static float getUsagePercentage(double limit, double usage) {
    return (float) ((limit <= 0) ? 100 : ((usage * 100) / limit));
  }

  /**
   * Convert a {@link Collection} of {@link Account}s to {@link Collection} of {@link QuotaResource} objects.
   * @param accounts {@link Collection} of account objects to be converted.
   * @return Collection of {@link QuotaResource} objects.
   */
  public static Collection<QuotaResource> getQuotaResourcesFromAccounts(Collection<Account> accounts) {
    Set<QuotaResource> quotaResources = new HashSet<>();
    accounts.forEach(account -> {
      if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
        quotaResources.add(QuotaResource.fromAccount(account));
      } else {
        account.getAllContainers().forEach(container -> quotaResources.add(QuotaResource.fromContainer(container)));
      }
    });
    return quotaResources;
  }

  /**
   * Returns {@code true} if request should be throttled. {@code false} otherwise.
   * @param quotaAction {@link QuotaAction} specifying the action to take for quota compliance.
   * @return {@code true} if request should be throttled. {@code false} otherwise.
   */
  public static boolean shouldThrottle(QuotaAction quotaAction) {
    return quotaAction != QuotaAction.ALLOW;
  }
}
