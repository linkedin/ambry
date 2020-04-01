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
package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs the {@link RestUtils.SubResource#Replicas} operation.
 */
class GetReplicasHandler {
  static String REPLICAS_KEY = "replicas";

  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(GetReplicasHandler.class);

  /**
   * Instantiate a handler to handle {@link RestUtils.SubResource#Replicas} operations.
   * @param metrics the {@link FrontendMetrics} instance to use for metrics.
   * @param clusterMap the {@link ClusterMap} to use to find the replicas of a blob ID.
   */
  GetReplicasHandler(FrontendMetrics metrics, ClusterMap clusterMap) {
    this.metrics = metrics;
    this.clusterMap = clusterMap;
  }

  /**
   * Handles {@link RestUtils.SubResource#Replicas} operations by obtaining the replicas of the blob ID from the cluster
   * map and returning a serialized JSON object in the response.
   * @param blobId the blob ID whose replicas are required.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers in.
   * @return a {@link ReadableStreamChannel} that contains the getReplicas response.
   * @throws RestServiceException if there was any problem constructing the response.
   */
  ReadableStreamChannel getReplicas(String blobId, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    logger.trace("Getting replicas of blob ID - {}", blobId);
    long startTime = System.currentTimeMillis();
    ReadableStreamChannel channel = null;
    try {
      channel = FrontendUtils.serializeJsonToChannel(getReplicas(blobId));
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
    } finally {
      metrics.getReplicasProcessingTimeInMs.update(System.currentTimeMillis() - startTime);
    }
    return channel;
  }

  /**
   * Extracts the blob ID provided by the client and figures out the partition that the blob ID would belong to
   * based on the cluster map. Using the partition information, returns the list of replicas as a part of a JSONObject.
   * @param blobId the blob ID whose replicas are required.
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws RestServiceException if there were missing or invalid arguments or if there was a {@link JSONException}
   *                                or any other while building the response
   */
  private JSONObject getReplicas(String blobId) throws RestServiceException {
    try {
      PartitionId partitionId = new BlobId(blobId, clusterMap).getPartition();
      if (partitionId == null) {
        metrics.invalidBlobIdError.inc();
        logger.warn("Partition for blob id {} is null. The blob id might be invalid", blobId);
        throw new RestServiceException("Partition for blob id " + blobId + " is null. The id might be invalid",
            RestServiceErrorCode.NotFound);
      }
      return packageResult(partitionId.getReplicaIds());
    } catch (IllegalArgumentException e) {
      metrics.invalidBlobIdError.inc();
      throw new RestServiceException("Invalid blob id received for getReplicasForBlob request - " + blobId, e,
          RestServiceErrorCode.NotFound);
    } catch (IOException | JSONException e) {
      metrics.responseConstructionError.inc();
      throw new RestServiceException("Could not create response for GET of replicas for " + blobId, e,
          RestServiceErrorCode.InternalServerError);
    }
  }

  /**
   * Packages the list of replicas into a {@link JSONObject}.
   * @param replicaIds the list of {@link ReplicaId}s that need to packaged into a {@link JSONObject}.
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws JSONException if there was an error building the {@link JSONObject}.
   */
  private static JSONObject packageResult(List<? extends ReplicaId> replicaIds) throws JSONException {
    JSONObject result = new JSONObject();
    if (replicaIds != null) {
      for (ReplicaId replicaId : replicaIds) {
        result.append(REPLICAS_KEY, replicaId.toString());
      }
    }
    return result;
  }
}
