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

import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;

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
}
