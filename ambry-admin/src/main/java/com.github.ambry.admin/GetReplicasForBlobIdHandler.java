package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.FutureResult;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs the custom {@link AdminOperationType#getReplicasForBlobId} operation supported by the admin.
 */
class GetReplicasForBlobIdHandler {
  protected static String BLOB_ID_KEY = "blobId";
  protected static String REPLICAS_KEY = "replicas";
  private static Logger logger = LoggerFactory.getLogger(GetReplicasForBlobIdHandler.class);

  /**
   * Handles {@link AdminOperationType#getReplicasForBlobId}} operations.
   * <p/>
   * Extracts the blob ID from the {@code restRequest}, infers replicas of the blob ID if possible, packages the replica
   * list into a {@link JSONObject} and makes the object available via a {@link ReadableStreamChannel}. Invokes the
   * {@code callback} (if any) when operation is complete.
   * <p/>
   * Content sent via the {@code restRequest} is ignored.
   * @param restRequest {@link RestRequest} containing details of the request.
   * @param clusterMap the {@link ClusterMap} to use to find the replicas for blob ID.
   * @param callback the {@link Callback} to invoke when operation is complete. Can be null.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @return a {@link Future} that will eventually contain the getReplicasForBlobId response in the form of a
   *          {@link ReadableStreamChannel}.
   */
  public static Future<ReadableStreamChannel> handleGetRequest(RestRequest restRequest, ClusterMap clusterMap,
      Callback<ReadableStreamChannel> callback, AdminMetrics adminMetrics) {
    logger.trace("Handling getReplicasForBlobId - {}", restRequest.getUri());
    adminMetrics.getReplicasForBlobIdRate.mark();
    long startTime = System.currentTimeMillis();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    ReadableStreamChannel channel = null;
    RestServiceException exception = null;
    RuntimeException re = null;
    try {
      String replicaStr = getReplicasForBlobId(restRequest, clusterMap, adminMetrics).toString();
      // TODO: this needs to go into a handle head request.
      // responseChannel.setContentType("application/json");
      channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(replicaStr.getBytes()));
    } catch (RestServiceException e) {
      exception = e;
      re = new RuntimeException(e);
    } finally {
      futureResult.done(channel, re);
      if (callback != null) {
        callback.onCompletion(channel, exception);
      }
      long processingTime = System.currentTimeMillis() - startTime;
      logger.trace("Processing getReplicasForBlobId response for request {} took {} ms", restRequest.getUri(),
          processingTime);
      adminMetrics.getReplicasForBlobIdProcessingTimeInMs.update(processingTime);
    }
    return futureResult;
  }

  /**
   * Extracts the blob ID provided by the client and figures out the partition that the blob ID would belong to
   * based on the cluster map. Using the partition information, returns the list of replicas as a part of a JSONObject.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @param clusterMap {@link ClusterMap} to use to find the replicas of the blob id.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws RestServiceException if there were missing or invalid arguments or if there was a {@link JSONException}
   *                                or any other while building the response
   */
  private static JSONObject getReplicasForBlobId(RestRequest restRequest, ClusterMap clusterMap,
      AdminMetrics adminMetrics)
      throws RestServiceException {
    Map<String, List<String>> parameters = restRequest.getArgs();
    if (parameters != null && parameters.containsKey(BLOB_ID_KEY)) {
      String blobIdStr = parameters.get(BLOB_ID_KEY).get(0);
      logger.trace("BlobId for request {} is {}", restRequest.getUri(), blobIdStr);
      try {
        PartitionId partitionId = new BlobId(blobIdStr, clusterMap).getPartition();
        if (partitionId == null) {
          logger.warn("Partition for blob id {} is null. The blob id might be invalid", blobIdStr);
          adminMetrics.getReplicasForBlobIdPartitionNullError.inc();
          throw new RestServiceException("Partition for blob id " + blobIdStr + " is null. The id might be invalid",
              RestServiceErrorCode.InvalidArgs);
        }
        return packageResult(partitionId.getReplicaIds());
      } catch (IllegalArgumentException e) {
        adminMetrics.getReplicasForBlobIdInvalidBlobIdError.inc();
        throw new RestServiceException("Invalid blob id received for getReplicasForBlob request - " + blobIdStr, e,
            RestServiceErrorCode.InvalidArgs);
      } catch (IOException e) {
        adminMetrics.getReplicasForBlobIdObjectCreationError.inc();
        throw new RestServiceException(
            "BlobId object creation failed for getReplicasForBlobId request for blob id " + blobIdStr, e,
            RestServiceErrorCode.InternalObjectCreationError);
      } catch (JSONException e) {
        adminMetrics.getReplicasForBlobIdResponseBuildingError.inc();
        throw new RestServiceException("Unable to construct result JSON object during getReplicasForBlobId", e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      adminMetrics.getReplicasForBlobIdMissingParameterError.inc();
      throw new RestServiceException("Request for getReplicasForBlobId missing parameter - " + BLOB_ID_KEY,
          RestServiceErrorCode.MissingArgs);
    }
  }

  /**
   * Packages the list of replicas into a {@link JSONObject}.
   * @param replicaIds the list of {@link ReplicaId}s that need to packaged into a {@link JSONObject}.
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws JSONException if there was an error building the {@link JSONObject}.
   */
  private static JSONObject packageResult(List<ReplicaId> replicaIds)
      throws JSONException {
    JSONObject result = new JSONObject();
    if (replicaIds != null) {
      for (ReplicaId replicaId : replicaIds) {
        result.append(REPLICAS_KEY, replicaId);
      }
    }
    return result;
  }
}
