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
import com.github.ambry.rest.RestUtils;
import java.util.HashMap;
import java.util.Map;


/**
 * {@link RequestCostPolicy} implementation that calculates request cost in terms of metrics tracked by user quota - capacity unit and storage.
 * Capacity unit cost is defined as the number of 4MB chunks. Storage cost is defined as number of GB of storage used.
 */
public class SimpleUserQuotaRequestCostPolicy implements RequestCostPolicy {
  final static double CU_COST_UNIT = 4 * 1024 * 1024; //4 MB
  final static double BYTES_IN_GB = 1024 * 1024 * 1024;
  final static double DEFAULT_COST_FOR_HEAD_DELETE_TTL = 1;

  @Override
  public Map<String, Double> calculateRequestCost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo) {
    Map<String, Double> costMap = new HashMap<>();
    if (blobInfo != null) {
      costMap.put(restMethodToCostMetric(restRequest.getRestMethod()),
          calculateCapacityUnitCost(restRequest, blobInfo));
      costMap.put(QuotaName.STORAGE_IN_GB.name(), calculateStorageCost(restRequest, restResponseChannel, blobInfo));
    }
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
   * @param blobInfo {@link BlobInfo} with information about the blob.
   * @return storage cost.
   */
  private double calculateStorageCost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo) {
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
   * @param blobInfo {@link BlobInfo} object representing the blob being served.
   * @return cost in terms of capacity units.
   */
  private double calculateCapacityUnitCost(RestRequest restRequest, BlobInfo blobInfo) {
    switch (restRequest.getRestMethod()) {
      case POST:
        double cost = Math.ceil(restRequest.getBytesReceived() / CU_COST_UNIT);
        return (cost > 0) ? cost : 1;
      case GET:
        cost = Math.ceil(blobInfo.getBlobProperties().getBlobSize() / CU_COST_UNIT);
        return (cost > 0) ? cost : 1;
      default:
        return DEFAULT_COST_FOR_HEAD_DELETE_TTL; // Assuming 1 unit of capacity unit cost for all requests that don't fetch or store a blob's data.
    }
  }
}
