/**
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
import com.github.ambry.account.Container;
import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;

import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Common utility functions that can be used across implementations of Quota interfaces.
 */
public class QuotaUtils {

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
   * @throws RestServiceException if appropriate headers aren't found in the {@link RestRequest}.
   */
  public static QuotaResource getQuotaResourceId(RestRequest restRequest) throws RestServiceException {
    Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
    if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
      return QuotaResource.fromAccountId(account.getId());
    } else {
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      return QuotaResource.fromContainerId(account.getId(), container.getId());
    }
  }

  /**
   * Create {@link QuotaResource} for the specified {@link RestRequest}.
   *
   * @param restRequest {@link RestRequest} object.
   * @return QuotaResource extracted from headers of {@link RestRequest}.
   * @throws RestServiceException if appropriate headers aren't found in the {@link RestRequest}.
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
   * @return QuotaName of the quota associated with {@link RestRequest}.
   */
  public static QuotaName getQuotaName(RestRequest restRequest) {
    return isReadRequest(restRequest) ? QuotaName.READ_CAPACITY_UNIT : QuotaName.WRITE_CAPACITY_UNIT;
  }
}
