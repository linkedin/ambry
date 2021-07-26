package com.github.ambry.quota;

import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;

import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Common utility functions that can be used across implementations of Quota interfaces.
 */
public class QuotaUtils {

  /**
   * Returns checks if user request quota should be applied to the request.
   * @param restRequest {@link RestRequest} object.
   * @return {@code true} if user request quota should be applied to the request. {@code false} otherwise.
   */
  public static boolean isRequestResourceQuotaManaged(RestRequest restRequest) {
    RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
    return !(requestPath.matchesOperation(Operations.GET_PEERS) || requestPath.matchesOperation(
        Operations.GET_CLUSTER_MAP_SNAPSHOT) || requestPath.matchesOperation(Operations.ACCOUNTS)
        || requestPath.matchesOperation(Operations.STATS_REPORT));
  }
}
