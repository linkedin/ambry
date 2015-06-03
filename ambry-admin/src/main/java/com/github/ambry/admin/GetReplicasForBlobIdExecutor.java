package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.storageservice.BlobStorageServiceErrorCode;
import com.github.ambry.storageservice.BlobStorageServiceException;
import java.io.IOException;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class GetReplicasForBlobIdExecutor implements TaskExecutor {
  public static String BLOB_ID_KEY = "blobId";
  public static String REPLICAS_KEY = "replicas";

  private final ClusterMap clusterMap;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetReplicasForBlobIdExecutor(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  public AdminExecutionResult execute(AdminExecutionData executionData)
      throws BlobStorageServiceException {
    JSONObject data = executionData.getOperationData();
    if (data != null && data.has(BLOB_ID_KEY)) {
      try {
        BlobId blobId = new BlobId(data.getString(BLOB_ID_KEY), clusterMap);
        return packageResult(getReplicas(blobId));
      } catch (IOException e) {
        throw new BlobStorageServiceException("Unable to get blob id object - " + e,
            BlobStorageServiceErrorCode.InternalError);
      } catch (JSONException e) {
        throw new BlobStorageServiceException("Unable to construct result object - " + e,
            BlobStorageServiceErrorCode.ResponseBuildingError);
      }
    } else {
      throw new BlobStorageServiceException("Input AdminExecutionData does not have key - " + BLOB_ID_KEY,
          BlobStorageServiceErrorCode.BadRequest);
    }
  }

  private List<ReplicaId> getReplicas(BlobId blobId) {
    return blobId.getPartition().getReplicaIds();
  }

  private AdminExecutionResult packageResult(List<ReplicaId> replicaIds)
      throws JSONException {
    JSONObject result = new JSONObject();
    for (ReplicaId replicaId : replicaIds) {
      result.append(REPLICAS_KEY, replicaId);
    }
    return new AdminExecutionResult(result);
  }
}
