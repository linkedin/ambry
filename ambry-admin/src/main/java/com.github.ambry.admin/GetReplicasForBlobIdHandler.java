package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestRequestInfo;
import com.github.ambry.rest.RestRequestMetadata;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
   * Extracts the parameters from the {@link RestRequestMetadata}, infers replicas of the blobId if possible and writes
   * the response to the client via a {@link RestResponseHandler}.
   * <p/>
   * Flushes the written data and closes the connection on receiving an end marker (the last part of
   * {@link com.github.ambry.rest.RestRequestContent} of the request). Any other content is ignored.
   * @param restRequestInfo {@link RestRequestInfo} containing details of the request.
   * @param clusterMap {@link ClusterMap} to use to find the replicas of the blob id.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @throws RestServiceException
   */
  public static void handleRequest(RestRequestInfo restRequestInfo, ClusterMap clusterMap, AdminMetrics adminMetrics)
      throws RestServiceException {
    RestResponseHandler responseHandler = restRequestInfo.getRestResponseHandler();
    if (restRequestInfo.isFirstPart()) {
      logger.trace("Handling getReplicasForBlobId - {}", restRequestInfo.getRestRequestMetadata().getUri());
      adminMetrics.getReplicasForBlobIdRate.mark();
      long startTime = System.currentTimeMillis();
      try {
        String replicaStr =
            getReplicasForBlobId(restRequestInfo.getRestRequestMetadata(), clusterMap, adminMetrics).toString();
        responseHandler.setContentType("application/json");
        responseHandler.addToResponseBody(replicaStr.getBytes(), true);
        responseHandler.flush();
        logger.trace("Sent getReplicasForBlobId response for request {}",
            restRequestInfo.getRestRequestMetadata().getUri());
      } finally {
        long processingTime = System.currentTimeMillis() - startTime;
        logger.trace("Processing getReplicasForBlobId response for request {} took {} ms",
            restRequestInfo.getRestRequestMetadata().getUri(), processingTime);
        adminMetrics.getReplicasForBlobIdProcessingTimeInMs.update(processingTime);
      }
    } else if (restRequestInfo.getRestRequestContent().isLast()) {
      responseHandler.onRequestComplete(null, false);
      logger.trace("GetReplicasForBlobId request {} complete", restRequestInfo.getRestRequestMetadata().getUri());
    }
  }

  /**
   * Extracts the blobid provided by the client and figures out the partition that the blobid would belong to
   * based on the cluster map. Using the partition information, returns the list of replicas as a part of a JSONObject.
   * @param restRequestMetadata {@link RestRequestMetadata} containing metadata about the request.
   * @param clusterMap {@link ClusterMap} to use to find the replicas of the blob id.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws RestServiceException
   */
  private static JSONObject getReplicasForBlobId(RestRequestMetadata restRequestMetadata, ClusterMap clusterMap,
      AdminMetrics adminMetrics)
      throws RestServiceException {
    Map<String, List<String>> parameters = restRequestMetadata.getArgs();
    if (parameters != null && parameters.containsKey(BLOB_ID_KEY)) {
      String blobIdStr = parameters.get(BLOB_ID_KEY).get(0);
      logger.trace("BlobId for request {} is {}", restRequestMetadata.getUri(), blobIdStr);
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
   * @throws JSONException
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
