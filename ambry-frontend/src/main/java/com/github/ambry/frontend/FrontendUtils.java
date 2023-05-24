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

import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.AsyncOperationTracker;
import com.github.ambry.utils.ThrowingConsumer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Common utility functions that will be used across frontend package
 */
class FrontendUtils {
  static final String NAMED_BLOB_PREFIX = "/named";
  static final String SLASH = "/";

  /**
   * Throws the specified {@link RestServiceException} if isEnabled is {@code true}. No-op otherwise.
   * @param restServiceException {@link RestServiceException} object to throw.
   * @param isEnabled if {@code true} then throw the specified exception. do nothing otherwise.
   * @throws RestServiceException the exception to throw.
   */
  static void throwRestServiceExceptionIfEnabled(RestServiceException restServiceException, boolean isEnabled)
      throws RestServiceException {
    if (isEnabled) {
      throw restServiceException;
    }
  }

  /**
   * Fetches {@link BlobId} from the given string representation of BlobId
   * @param blobIdStr string representation of BlobId
   * @param clusterMap {@link ClusterMap} instance to use
   * @return the {@link BlobId} thus generated
   * @throws RestServiceException on invalid blobId
   */
  static BlobId getBlobIdFromString(String blobIdStr, ClusterMap clusterMap) throws RestServiceException {
    try {
      return new BlobId(blobIdStr, clusterMap);
    } catch (Exception e) {
      throw new RestServiceException("Invalid blob id=" + blobIdStr, RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * @param metrics the {@link AsyncOperationTracker.Metrics} instance to update.
   * @param successAction the action to take if the callback was called successfully.
   * @param context the context in which this callback is being called (for logging)
   * @param logger the {@link Logger} instance to use
   * @param finalCallback the final callback to call once the chain is complete or if it is interrupted
   * @return the {@link Callback} returned by {@link CallbackUtils#chainCallback}.
   */
  static <T, V> Callback<T> buildCallback(AsyncOperationTracker.Metrics metrics, ThrowingConsumer<T> successAction,
      String context, Logger logger, Callback<V> finalCallback) {
    AsyncOperationTracker tracker = new AsyncOperationTracker(context, logger, metrics);
    return CallbackUtils.chainCallback(tracker, finalCallback, successAction);
  }

  /**
   * Parse a {@link JSONObject} from the data in {@code channel}. This assumes that the data is UTF-8 encoded.
   * @param channel the {@link RetainingAsyncWritableChannel} that contains the JSON data.
   * @return a {@link JSONObject}.
   * @throws IOException if closing the {@link InputStream} fails.
   * @throws RestServiceException if JSON parsing fails.
   */
  static JSONObject readJsonFromChannel(RetainingAsyncWritableChannel channel) throws RestServiceException {
    try (InputStream inputStream = channel.consumeContentAsInputStream()) {
      return new JSONObject(new JSONTokener(new InputStreamReader(inputStream, StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new RestServiceException("Could not parse json request body", e, RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * @param jsonObject the {@link JSONObject} to serialize.
   * @return the {@link ReadableStreamChannel} containing UTF-8 formatted json.
   * @throws RestServiceException if there was an error during serialization.
   */
  static ReadableStreamChannel serializeJsonToChannel(JSONObject jsonObject) throws RestServiceException {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try (Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
        jsonObject.write(writer);
      }
      return new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
    } catch (Exception e) {
      throw new RestServiceException("Could not serialize response json.", e, RestServiceErrorCode.InternalServerError);
    }
  }

  /**
   * Fetch {@link RestRequestMetricsGroup} for GetRequest based on the {@link RestUtils.SubResource}.
   * @param frontendMetrics instance of {@link FrontendMetrics} to use
   * @param subResource {@link RestUtils.SubResource} corresponding to the GetRequest
   * @return the appropriate {@link RestRequestMetricsGroup} based on the given params
   */
  static RestRequestMetricsGroup getMetricsGroupForGet(FrontendMetrics frontendMetrics,
      RestUtils.SubResource subResource) {
    RestRequestMetricsGroup group = null;
    if (subResource == null || subResource.equals(RestUtils.SubResource.Segment)) {
      group = frontendMetrics.getBlobMetricsGroup;
    } else {
      switch (subResource) {
        case BlobInfo:
          group = frontendMetrics.getBlobInfoMetricsGroup;
          break;
        case UserMetadata:
          group = frontendMetrics.getUserMetadataMetricsGroup;
          break;
        case Replicas:
          group = frontendMetrics.getReplicasMetricsGroup;
          break;
        case BlobChunkIds:
          group = frontendMetrics.getBlobChunkIdsMetricsGroup;
          break;
      }
    }
    return group;
  }

  /**
   * Refresh the OperationOrBlobId in request path with the specific latest version info.
   * @param restRequest the {@link RestRequest}
   * @param datasetVersionRecord the {@link DatasetVersionRecord} which include the version info.
   * @throws RestServiceException
   */
  static void replaceRequestPathWithNewOperationOrBlobIdIfNeeded(RestRequest restRequest,
      DatasetVersionRecord datasetVersionRecord, String version) throws RestServiceException {
    if (version.equals("LATEST") || version.equals("MAJOR") || version.equals("MINOR") || version.equals("PATCH")) {
      RequestPath originalRequestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
      String originalOperationOrBlobId = originalRequestPath.getOperationOrBlobId(false);
      int index = originalOperationOrBlobId.lastIndexOf(PATH_SEPARATOR_STRING);
      String latestOperationOrBlobId;
      if (index != -1) {
        latestOperationOrBlobId =
            originalOperationOrBlobId.substring(0, index) + PATH_SEPARATOR_STRING + datasetVersionRecord.getVersion();
      } else {
        throw new RestServiceException("Failed to parse the originalOperationOrBlobId: " + originalOperationOrBlobId,
            RestServiceErrorCode.BadRequest);
      }
      RequestPath newRequestPath =
          new RequestPath(originalRequestPath.getPrefix(), originalRequestPath.getClusterName(),
              originalRequestPath.getPathAfterPrefixes(), latestOperationOrBlobId, originalRequestPath.getSubResource(),
              originalRequestPath.getBlobSegmentIdx());
      restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, newRequestPath);
    }
  }
}
