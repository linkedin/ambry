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
 * distributed under the License is distributed /on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.frontend;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.RequestCostPolicy;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.SubResource;


/**
 * {@link RequestCostPolicy} implementation that calculates request cost in terms of metrics tracked by user quota - capacity unit and storage.
 * Capacity unit cost is defined as the number of 4MB chunks. Storage cost is defined as number of GB of storage used.
 */
public class UserQuotaRequestCostPolicy implements RequestCostPolicy {
  final static double CU_COST_UNIT = 4 * 1024 * 1024; //4 MB
  final static double BYTES_IN_GB = 1024 * 1024 * 1024;
  final static double INDEX_ONLY_COST = 1;
  final static double MIN_CU_COST = INDEX_ONLY_COST;
  private static final Logger logger = LoggerFactory.getLogger(UserQuotaRequestCostPolicy.class);

  @Override
  public Map<String, Double> calculateRequestCost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo) {
    Map<String, Double> costMap = new HashMap<>();
    costMap.put(restMethodToCostMetric(restRequest.getRestMethod()),
        calculateCapacityUnitCost(restRequest, restResponseChannel, blobInfo));
    costMap.put(QuotaName.STORAGE_IN_GB.name(), calculateStorageCost(restRequest, restResponseChannel));
    return costMap;
  }

  /**
   * Get appropriate cost metric name for {@link RestMethod}.
   * @param restMethod {@link RestMethod} object.
   * @return CUQuotaMethod name for specified {@link RestMethod}.
   */
  private String restMethodToCostMetric(RestMethod restMethod) {
    switch (restMethod) {
      case GET:
      case OPTIONS:
      case HEAD:
        return QuotaName.READ_CAPACITY_UNIT.name();
      default:
        return QuotaName.WRITE_CAPACITY_UNIT.name();
    }
  }

  /**
   * Calculate the storage cost incurred to serve a request.
   * For post requests this is the number bytes in GB of the blob data. For all other write requests the default is DEFAULT_COST_FOR_HEAD_DELETE_TTL.
   * For read requests there is no storage cost incurred.
   * @param restRequest {@link RestRequest} to find type of request.
   * @param restResponseChannel {@link RestResponseChannel} object.
   * @return storage cost.
   */
  private double calculateStorageCost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    switch (restRequest.getRestMethod()) {
      case POST:
        long contentSize = 0;
        if (restRequest.getUri().contains(Operations.STITCH)) {
          contentSize = Long.parseLong((String) restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
        } else {
          contentSize = restRequest.getBytesReceived();
        }
        return contentSize / BYTES_IN_GB;
      case DELETE:
      case PUT:
        return CU_COST_UNIT / BYTES_IN_GB; // Assuming 1 chunk worth of storage cost for deletes and ttl updates.
      default:
        return 0;
    }
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
    switch (restRequest.getRestMethod()) {
      case POST:
        long contentSize = 0;
        if (restRequest.getUri().contains(Operations.STITCH)) {
          contentSize = Long.parseLong((String) restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
        } else {
          contentSize = restRequest.getBytesReceived();
        }
        double cost = Math.ceil(contentSize / CU_COST_UNIT);
        return (cost > 0) ? cost : 1;
      case GET:
        String uri = restRequest.getUri();
        if (uri.contains(Operations.GET_SIGNED_URL)) {
          return INDEX_ONLY_COST;
        } else if (uri.contains(SubResource.BlobInfo.name()) || uri.contains(SubResource.UserMetadata.name())) {
          // metadata request
          return INDEX_ONLY_COST;
        } else {
          long size = 0;
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
