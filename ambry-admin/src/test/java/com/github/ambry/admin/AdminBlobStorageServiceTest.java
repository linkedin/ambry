package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.storageservice.BlobStorageServiceException;
import com.github.ambry.storageservice.ExecutionResult;
import java.io.IOException;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * TODO: write description
 */
public class AdminBlobStorageServiceTest {

  @Test
  public void startShutDownTest()
      throws Exception {
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(null);
    adminBlobStorageService.start();
    adminBlobStorageService.shutdown();
  }

  @Test
  public void echoTest()
      throws BlobStorageServiceException, JSONException {
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(null);
    String inputText = "TextToBeEchoed";
    AdminExecutionData executionData = createEchoExecutionData(inputText);
    ExecutionResult executionResult = adminBlobStorageService.execute(executionData);
    String echoedText = executionResult.getOperationResult().getString(EchoExecutor.TEXT_KEY);
    assertEquals("Echoed text must be equal to input text", inputText, echoedText);
  }

  private AdminExecutionData createEchoExecutionData(String inputText)
      throws JSONException {
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("text", inputText);
    executionData.put("operationType", "Echo");
    executionData.put("operationData", operationData);

    return new AdminExecutionData(executionData);
  }

  @Test
  public void getReplicasForBlobIdTest()
      throws BlobStorageServiceException, IOException, JSONException {
    ClusterMap clusterMap = new MockClusterMap();
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(clusterMap);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      createBlobIdAndTest(partitionId, adminBlobStorageService);
    }
  }

  private void createBlobIdAndTest(PartitionId partitionId, AdminBlobStorageService adminBlobStorageService)
      throws BlobStorageServiceException, JSONException {
    String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
    BlobId blobId = new BlobId(partitionId);
    AdminExecutionData executionData = createGetReplicasForBlobIdExecutionData(blobId.getID());
    ExecutionResult executionResult = adminBlobStorageService.execute(executionData);
    String returnedReplicasStr =
        executionResult.getOperationResult().getString(GetReplicasForBlobIdExecutor.REPLICAS_KEY).replace("\"", "");
    assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
        originalReplicaStr, returnedReplicasStr);
  }

  private AdminExecutionData createGetReplicasForBlobIdExecutionData(String blobId)
      throws JSONException {
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("blobId", blobId);
    executionData.put("operationType", "GetReplicasForBlobId");
    executionData.put("operationData", operationData);

    return new AdminExecutionData(executionData);
  }

  @Test(expected = BlobStorageServiceException.class)
  public void unknownOperationExceptionTest()
      throws JSONException, BlobStorageServiceException {
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(null);
    AdminExecutionData executionData = createUnknownOperationExecutionData();
    adminBlobStorageService.execute(executionData);
  }

  private AdminExecutionData createUnknownOperationExecutionData()
      throws JSONException {
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("dummyData", "dummyData");
    executionData.put("operationType", "@@@UnknownOperation@@@");
    executionData.put("operationData", operationData);

    return new AdminExecutionData(executionData);
  }
}
