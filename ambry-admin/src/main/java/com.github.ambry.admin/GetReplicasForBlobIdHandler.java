package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
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


/**
 * Performs the custom {@link AdminOperationType#getReplicasForBlobId} operation supported by the admin.
 */
class GetReplicasForBlobIdHandler {
  protected static String BLOB_ID_KEY = "blobId";
  protected static String REPLICAS_KEY = "replicas";

  /**
   * Handles {@link AdminOperationType#getReplicasForBlobId}} operations.
   * <p/>
   * Extracts the parameters from the {@link RestRequestMetadata}, infers replicas of the blobId if possible and writes
   * the response to the client via a {@link RestResponseHandler}.
   * <p/>
   * Flushes the written data and closes the connection on receiving an end marker (the last part of
   * {@link com.github.ambry.rest.RestRequestContent} of the request). Any other content is ignored.
   * @param restRequestInfo
   * @param clusterMap
   * @throws RestServiceException
   */
  public static void handleRequest(RestRequestInfo restRequestInfo, ClusterMap clusterMap)
      throws RestServiceException {
    RestResponseHandler responseHandler = restRequestInfo.getRestResponseHandler();
    if (restRequestInfo.getRestRequestContent() == null) {
      String replicaStr = getReplicasForBlobId(restRequestInfo.getRestRequestMetadata(), clusterMap).toString();
      responseHandler.setContentType("application/json");
      responseHandler.addToResponseBody(replicaStr.getBytes(), true);
    } else if (restRequestInfo.getRestRequestContent().isLast()) {
      responseHandler.flush();
      responseHandler.onRequestComplete(null, false);
    }
  }

  /**
   * Extracts the blobid provided by the client and figures out the partition that the blobid would belong to
   * based on the cluster map. Using the partition information, returns the list of replicas as a part of a JSONObject.
   * @param restRequestMetadata
   * @param clusterMap
   * @return - A {@link JSONObject} that wraps the replica list.
   * @throws RestServiceException
   */
  private static JSONObject getReplicasForBlobId(RestRequestMetadata restRequestMetadata, ClusterMap clusterMap)
      throws RestServiceException {
    Map<String, List<String>> parameters = restRequestMetadata.getArgs();
    if (parameters != null && parameters.containsKey(BLOB_ID_KEY)) {
      try {
        // TODO: opportunity for batch get here.
        BlobId blobId = new BlobId(parameters.get(BLOB_ID_KEY).get(0), clusterMap);
        return packageResult(blobId.getPartition().getReplicaIds());
      } catch (IllegalArgumentException e) {
        throw new RestServiceException("Invalid blob id", e, RestServiceErrorCode.InvalidArgs);
      } catch (IOException e) {
        throw new RestServiceException("Unable to create blob id object ", e, RestServiceErrorCode.InternalServerError);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to construct result object", e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Request missing parameter - " + BLOB_ID_KEY, RestServiceErrorCode.MissingArgs);
    }
  }

  /**
   * Packages the list of replicas into a {@link JSONObject}.
   * @param replicaIds
   * @return A {@link JSONObject} that wraps the replica list.
   * @throws JSONException
   */
  private static JSONObject packageResult(List<ReplicaId> replicaIds)
      throws JSONException {
    JSONObject result = new JSONObject();
    for (ReplicaId replicaId : replicaIds) {
      result.append(REPLICAS_KEY, replicaId);
    }
    return result;
  }
}
