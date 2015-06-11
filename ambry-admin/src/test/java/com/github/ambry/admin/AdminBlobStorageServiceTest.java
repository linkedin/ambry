package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.MockRestContent;
import com.github.ambry.restservice.MockRestRequest;
import com.github.ambry.restservice.MockRestResponseHandler;
import com.github.ambry.restservice.RestContent;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class AdminBlobStorageServiceTest {

  @Test
  public void startShutDownTest()
      throws Exception {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    adminBlobStorageService.start();
    adminBlobStorageService.shutdown();
  }

  @Test(expected = IllegalStateException.class)
  public void handleGetTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.GET, "/", new JSONObject());
    adminBlobStorageService.handleMessage(messageInfo);
  }

  @Test(expected = IllegalStateException.class)
  public void handlePostTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.POST, "/", new JSONObject());
    adminBlobStorageService.handleMessage(messageInfo);
  }

  @Test(expected = IllegalStateException.class)
  public void handleDeleteTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.DELETE, "/", new JSONObject());
    adminBlobStorageService.handleMessage(messageInfo);
  }

  @Test(expected = IllegalStateException.class)
  public void handleHeadTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.HEAD, "/", new JSONObject());
    adminBlobStorageService.handleMessage(messageInfo);
  }

  @Test
  public void handleEchoTest()
      throws InstantiationException, IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    String inputText = "textToBeEchoed";
    MessageInfo messageInfo = createEchoMessageInfo(inputText);
    adminBlobStorageService.handleMessage(messageInfo);
    String echoedText = getJsonizedResponseBody(messageInfo).getString(EchoExecutor.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
    finishUpCheck(adminBlobStorageService, messageInfo);
  }

  @Test
  public void handleEchoWithBadInputTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createBadEchoMessageInfo();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }
  }

  @Test
  public void handleGetReplicasForBlobIdTest()
      throws InstantiationException, IOException, JSONException, RestServiceException, URISyntaxException {
    ClusterMap clusterMap = new MockClusterMap();
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      createBlobIdAndTest(partitionId, adminBlobStorageService);
    }
  }

  @Test
  public void handleGetReplicasForBlobIdWithBadInputTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createBadGetReplicasForBlobIdMessageInfo();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }
  }

  @Test
  public void customGetUnknownOperationTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createUnknownCustomGetOperationMessageInfo();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnknownOperationType, e.getErrorCode());
    }
  }

  @Test
  public void customGetWithBadExecutionDataTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    // no operationType
    try {
      MessageInfo messageInfo = createMesageInfoWithExecDataMissingOpType();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("executionData header was missing " + AdminExecutionData.OPERATION_TYPE_KEY
          + ", yet no exception was thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }

    // no operationData
    try {
      MessageInfo messageInfo = createMessageInfoWithExecDataMissingOpData();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("executionData header was missing " + AdminExecutionData.OPERATION_DATA_KEY
          + " , yet no exception was thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }

    // executionData invalid JSON
    try {
      MessageInfo messageInfo = createMessageInfoWithExecDataNotValidJSON();
      adminBlobStorageService.handleMessage(messageInfo);
      fail("executionData header was not a valid JSON , yet no exception was thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }
  }

  // helpers
  // general
  private AdminBlobStorageService getAdminBlobStorageService()
      throws IOException {
    return getAdminBlobStorageService(new MockClusterMap());
  }

  private AdminBlobStorageService getAdminBlobStorageService(ClusterMap clusterMap)
      throws IOException {
    // dud properties. pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new AdminBlobStorageService(verifiableProperties, clusterMap, new MetricRegistry());
  }

  private RestRequest getRestRequest(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    request.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(request);
  }

  private MessageInfo createMessageInfo(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    RestRequest restRequest = getRestRequest(restMethod, uri, headers);
    return new MessageInfo(restRequest, restRequest, new MockRestResponseHandler());
  }

  private JSONObject getJsonizedResponseBody(MessageInfo messageInfo)
      throws JSONException, RestServiceException {
    MockRestResponseHandler restResponseHandler = (MockRestResponseHandler) messageInfo.getResponseHandler();
    return new JSONObject(restResponseHandler.getFlushedBody());
  }

  private void finishUpCheck(AdminBlobStorageService adminBlobStorageService, MessageInfo messageInfo)
      throws InstantiationException, JSONException, RestServiceException {
    RestContent lastContent = createRestContent(true);
    adminBlobStorageService
        .handleMessage(new MessageInfo(messageInfo.getRestRequest(), lastContent, messageInfo.getResponseHandler()));
    assertTrue("Channel is not closed", ((MockRestResponseHandler) messageInfo.getResponseHandler()).isChannelClosed());
  }

  private RestContent createRestContent(boolean isLast)
      throws InstantiationException, JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestContent.IS_LAST_KEY, isLast);
    data.put(MockRestContent.CONTENT_KEY, "");
    return new MockRestContent(data);
  }

  //handleEchoTest() helpers
  private MessageInfo createEchoMessageInfo(String inputText)
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("text", inputText);
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "Echo");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // handleEchoWithBadInputTest() helpers
  private MessageInfo createBadEchoMessageInfo()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "Echo");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // handleGetReplicasForBlobIdTest() helpers
  private void createBlobIdAndTest(PartitionId partitionId, AdminBlobStorageService adminBlobStorageService)
      throws InstantiationException, JSONException, RestServiceException, URISyntaxException {
    String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
    BlobId blobId = new BlobId(partitionId);
    MessageInfo messageInfo = createGetReplicasForBlobIdMessageInfo(blobId.getID());
    adminBlobStorageService.handleMessage(messageInfo);
    String returnedReplicasStr =
        getJsonizedResponseBody(messageInfo).getString(GetReplicasForBlobIdExecutor.REPLICAS_KEY).replace("\"", "");
    assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
        originalReplicaStr, returnedReplicasStr);
    finishUpCheck(adminBlobStorageService, messageInfo);
  }

  private MessageInfo createGetReplicasForBlobIdMessageInfo(String blobId)
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("blobId", blobId);
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "GetReplicasForBlobId");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // handleGetReplicasForBlobIdWithBadInputTest() helpers
  private MessageInfo createBadGetReplicasForBlobIdMessageInfo()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "GetReplicasForBlobId");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // customGetUnknownOperationTest() helpers
  private MessageInfo createUnknownCustomGetOperationMessageInfo()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("dummyData", "dummyData");
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "@@@UnknownOperation@@@");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // customGetWithBadExecutionDataTest() helpers
  private MessageInfo createMessageInfoWithExecDataNotValidJSON()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    headers.put("executionData", "@@@InvalidJSON@@@");

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  private MessageInfo createMesageInfoWithExecDataMissingOpType()
      throws JSONException, URISyntaxException {
    JSONObject operationData = new JSONObject();
    operationData.put("text", "text");
    JSONObject executionData = new JSONObject();
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  private MessageInfo createMessageInfoWithExecDataMissingOpData()
      throws JSONException, URISyntaxException {
    JSONObject executionData = new JSONObject();
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "Echo");

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }
}
