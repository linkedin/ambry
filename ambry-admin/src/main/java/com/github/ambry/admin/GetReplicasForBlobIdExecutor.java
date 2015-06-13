package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.io.IOException;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes the custom echo GetReplicasFroBlobId operation.
 */
public class GetReplicasForBlobIdExecutor implements TaskExecutor {
  public static String BLOB_ID_KEY = "blobId";
  public static String REPLICAS_KEY = "replicas";

  private final ClusterMap clusterMap;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetReplicasForBlobIdExecutor(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  /**
   * Extracts the blob id provided by the client and figures out the partition that the blob id would belong to
   * based on the cluster map. Using the partition information, returns the list of replicas.
   * @param executionData
   * @return
   * @throws RestServiceException
   */
  public JSONObject execute(AdminExecutionData executionData)
      throws RestServiceException {
    JSONObject data = executionData.getOperationData();
    if (data != null && data.has(BLOB_ID_KEY)) {
      try {
        BlobId blobId = new BlobId(data.getString(BLOB_ID_KEY), clusterMap);
        return packageResult(getReplicas(blobId));
      } catch (IOException e) {
        throw new RestServiceException("Unable to get blob id object - " + e, RestServiceErrorCode.InternalServerError);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to construct result object - " + e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Execution data does not have key - " + BLOB_ID_KEY,
          RestServiceErrorCode.BadExecutionData);
    }
  }

  /**
   * Gets replicas for the particular partition (using the cluster map).
   * @param blobId
   * @return
   */
  private List<ReplicaId> getReplicas(BlobId blobId) {
    return blobId.getPartition().getReplicaIds();
  }

  /**
   * Packages result into a JSON.
   * @param replicaIds
   * @return
   * @throws JSONException
   */
  private JSONObject packageResult(List<ReplicaId> replicaIds)
      throws JSONException {
    JSONObject result = new JSONObject();
    for (ReplicaId replicaId : replicaIds) {
      result.append(REPLICAS_KEY, replicaId);
    }
    return result;
  }
}
