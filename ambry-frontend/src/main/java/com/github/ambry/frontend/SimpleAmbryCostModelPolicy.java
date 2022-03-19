/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Simple {@link AmbryCostModelPolicy} implementation that calculates CU costs in 4MB units and Storage cost in GB units.
 */
public class SimpleAmbryCostModelPolicy implements AmbryCostModelPolicy {
  public final static double CU_COST_UNIT = 4 * 1024 * 1024; //4 MB
  final static double INDEX_ONLY_COST = 1;
  final static double MIN_CU_COST = INDEX_ONLY_COST;

  private static final Logger logger = LoggerFactory.getLogger(SimpleAmbryCostModelPolicy.class);

  @Override
  public Map<String, Double> calculateRequestCost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo) {
    Map<String, Double> costMap = new HashMap<>();
    costMap.put(QuotaUtils.getCUQuotaName(restRequest).name(),
        calculateCapacityUnitCost(restRequest, restResponseChannel, blobInfo));
    costMap.put(QuotaName.STORAGE_IN_GB.name(),
        QuotaUtils.calculateStorageCost(restRequest, restRequest.getBlobBytesReceived()));
    return costMap;
  }

  /**
   * Calculate the cost incurred in terms of capacity unit to serve a request.
   * For post and get requests it is the number of CU_COST_UNITs determined by blob size.
   * For all other requests, the default cost is DEFAULT_COST_FOR_HEAD_DELETE_TTL.
   * @param restRequest {@link RestRequest} to find type of request.
   * @param restResponseChannel {@link RestResponseChannel} object.
   * @param blobInfo {@link BlobInfo} object representing the blob being served.
   * @return cost in terms of capacity units.
   */
  private double calculateCapacityUnitCost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo) {
    RequestPath requestPath = getRequestPath(restRequest);
    switch (restRequest.getRestMethod()) {
      case POST:
        long contentSize;
        if (requestPath.matchesOperation(Operations.STITCH)) {
          contentSize = Long.parseLong((String) restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
        } else {
          contentSize = restRequest.getBytesReceived();
        }
        double cost = Math.ceil(contentSize / CU_COST_UNIT);
        return Math.max(cost, MIN_CU_COST);
      case GET:
        SubResource subResource = requestPath.getSubResource();
        if (requestPath.matchesOperation(Operations.GET_SIGNED_URL)) {
          return INDEX_ONLY_COST;
        } else if (subResource == SubResource.BlobInfo || subResource == SubResource.UserMetadata) {
          return INDEX_ONLY_COST;
        } else {
          long size;
          if (blobInfo == null) { // Maybe its a GET for a deleted blob
            return MIN_CU_COST;
          }
          if (restRequest.getArgs().containsKey(RestUtils.Headers.RANGE)) { // Range request case
            try {
              size = RestUtils.buildByteRange(restRequest.getArgs().get(RestUtils.Headers.RANGE).toString())
                  .toResolvedByteRange(blobInfo.getBlobProperties().getBlobSize(), true)
                  .getRangeSize();
            } catch (RestServiceException rsEx) {
              // this should never happen.
              logger.error("Exception while calculation CU cost for range request", rsEx);
              size = 0; // this will default to one chunk
            }
          } else { // regular GET blob
            size = blobInfo.getBlobProperties().getBlobSize();
          }
          cost = Math.ceil(size / CU_COST_UNIT);
          return Math.max(cost, MIN_CU_COST);
        }
      default:
        return INDEX_ONLY_COST; // Assuming 1 unit of capacity unit cost for all requests that don't fetch or store a blob's data.
    }
  }
}
