package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.MessageInfo;
import com.github.ambry.rest.MockRestContent;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseHandler;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.storageservice.ExecutionData;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class AdminMessageHandlerTest {

  @Test(expected = IllegalStateException.class)
  public void handleGetTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/blobid", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  // tests with bad input data for handleGet()
  @Test
  public void specialGetWithBadExecutionDataTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    // no operationType
    try {
      MockRestRequest restRequest = createRequestWithExecDataMissingOpType();
      adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, null));
      fail("executionData header was missing " + ExecutionData.OPERATION_TYPE_KEY + ", yet no exception was thrown");
    } catch (RestException e) {
      //nothing to do. expected.
    }

    // no operationData
    try {
      MockRestRequest restRequest = createRequestWithExecDataMissingOpData();
      adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, null));
      fail("executionData header was missing " + ExecutionData.OPERATION_DATA_KEY + " , yet no exception was thrown");
    } catch (RestException e) {
      //nothing to do. expected.
    }

    // executionData invalid JSON
    try {
      MockRestRequest restRequest = createRequestWithExecDataNotValidJSON();
      adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, null));
      fail("executionData header was not a valid JSON , yet no exception was thrown");
    } catch (RestException e) {
      //nothing to do. expected.
    }
  }

  @Test(expected = IllegalStateException.class)
  public void handlePostTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.POST, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handlePost(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  @Test(expected = IllegalStateException.class)
  public void handleDeleteTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.DELETE, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleDelete(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  @Test(expected = IllegalStateException.class)
  public void handleHeadTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleHead(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  @Test
  public void onErrorWithResponseHandlerNotNullTest()
      throws IOException, JSONException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    Exception placeholderException = new Exception("placeHolderException");
    adminMessageHandler.onError(new MessageInfo(restRequest, restRequest, restResponseHandler), placeholderException);

    JSONObject response = restResponseHandler.getFlushedResponse();
    assertEquals("Response status error ", MockRestResponseHandler.STATUS_ERROR,
        response.getString(MockRestResponseHandler.RESPONSE_STATUS_KEY));
    assertEquals("Response error message mismatch", placeholderException.toString(),
        response.getString(MockRestResponseHandler.ERROR_MESSAGE_KEY));
    assertTrue("Error has not been sent", restResponseHandler.isErrorSent());
  }

  @Test
  public void onErrorWithBadInputTest()
      throws IOException, JSONException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    Exception placeholderException = new Exception("placeHolderException");
    adminMessageHandler.onError(new MessageInfo(restRequest, restRequest, null), placeholderException);
    adminMessageHandler.onError(null, placeholderException);
  }

  @Test
  public void onRequestCompleteWithRequestNotNullTest()
      throws IOException, JSONException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    /**
     * Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
     * this has to be updated with a check to verify state
     */
    adminMessageHandler.onRequestComplete(restRequest);
  }

  @Test
  public void onRequestCompleteWithRequestNullTest()
      throws IOException, JSONException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    /**
     * Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
     * this has to be updated with a check to verify state
     */
    adminMessageHandler.onRequestComplete(null);
  }

  @Test
  public void handleEchoTest()
      throws InstantiationException, IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    String inputText = "textToBeEchoed";
    MockRestRequest restRequest = createEchoRequest(inputText);
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();

    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
    checkEchoResponse(inputText, restResponseHandler);

    MockRestContent lastContent = createRestContent(true);
    adminMessageHandler.handleGet(new MessageInfo(restRequest, lastContent, restResponseHandler));
    assertTrue("Channel is not closed", restResponseHandler.isChannelClosed());
  }

  @Test(expected = RestException.class)
  public void handleEchoWithBadInputTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createBadEchoRequest();
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  @Test
  public void handleGetReplicasForBlobIdTest()
      throws InstantiationException, IOException, JSONException, RestException {
    ClusterMap clusterMap = new MockClusterMap();
    PartitionId partitionId = clusterMap.getWritablePartitionIds().get(0);
    BlobId blobId = new BlobId(partitionId);
    String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");

    AdminMessageHandler adminMessageHandler = getAdminMessageHandler(clusterMap);
    MockRestRequest restRequest = createGetReplicasForBlobIdRequest(blobId.getID());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();

    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
    checkGetReplicaForBlobIdResponse(originalReplicaStr, restResponseHandler);

    MockRestContent lastContent = createRestContent(true);
    adminMessageHandler.handleGet(new MessageInfo(restRequest, lastContent, restResponseHandler));
    assertTrue("Channel is not closed", restResponseHandler.isChannelClosed());
  }

  @Test(expected = RestException.class)
  public void handleGetReplicasForBlobIdWithBadInputTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createBadGetReplicasForBlobIdRequest();
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  @Test(expected = RestException.class)
  public void unknownOperationTest()
      throws IOException, JSONException, RestException {
    AdminMessageHandler adminMessageHandler = getAdminMessageHandler();
    MockRestRequest restRequest = createUnknownOperationRequest();
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    adminMessageHandler.handleGet(new MessageInfo(restRequest, restRequest, restResponseHandler));
  }

  //helpers
  //general
  private AdminMessageHandler getAdminMessageHandler()
      throws IOException {
    return getAdminMessageHandler(new MockClusterMap());
  }

  private AdminMessageHandler getAdminMessageHandler(ClusterMap clusterMap)
      throws IOException {
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    AdminBlobStorageService adminBlobStorageService = new AdminBlobStorageService(clusterMap);
    return new AdminMessageHandler(adminMetrics, adminBlobStorageService);
  }

  private MockRestRequest createRestRequest(RestMethod method, String uri, JSONObject headers)
      throws JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method);
    data.put(MockRestRequest.URI_KEY, uri);
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data);
  }

  private MockRestContent createRestContent(boolean isLast)
      throws InstantiationException, JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestContent.IS_LAST_KEY, isLast);
    data.put(MockRestContent.CONTENT_KEY, "");
    return new MockRestContent(data);
  }

  //handleEchoTest() helpers
  private void checkEchoResponse(String inputText, MockRestResponseHandler restResponseHandler)
      throws JSONException, RestException {
    String bodyStr = restResponseHandler.getFlushedBody().getString(MockRestResponseHandler.BODY_STRING_KEY);
    JSONObject body = new JSONObject(bodyStr);
    String echoedText = body.getString(EchoExecutor.TEXT_KEY);
    assertEquals("Echoed text must be equal to input text", inputText, echoedText);
  }

  private MockRestRequest createEchoRequest(String inputText)
      throws JSONException {
    JSONObject operationData = new JSONObject();
    operationData.put("text", inputText);
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "Echo");
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  // handleEchoWithBadInputTest() helpers
  private MockRestRequest createBadEchoRequest()
      throws JSONException {
    JSONObject operationData = new JSONObject();
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "Echo");
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  //handleGetReplicasForBlobIdTest() helpers
  private void checkGetReplicaForBlobIdResponse(String originalReplicaStr, MockRestResponseHandler restResponseHandler)
      throws JSONException, RestException {
    String bodyStr = restResponseHandler.getFlushedBody().getString(MockRestResponseHandler.BODY_STRING_KEY);
    JSONObject body = new JSONObject(bodyStr);
    String returnedReplicasStr = body.getString(GetReplicasForBlobIdExecutor.REPLICAS_KEY).replace("\"", "");
    assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
        originalReplicaStr, returnedReplicasStr);
  }

  private MockRestRequest createGetReplicasForBlobIdRequest(String blobId)
      throws JSONException {
    JSONObject operationData = new JSONObject();
    operationData.put("blobId", blobId);
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "GetReplicasForBlobId");
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  // handleGetReplicasForBlobIdWithBadInputTest() helpers
  private MockRestRequest createBadGetReplicasForBlobIdRequest()
      throws JSONException {
    JSONObject operationData = new JSONObject();
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "GetReplicasForBlobId");
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  // unknownOperationTest() helpers
  private MockRestRequest createUnknownOperationRequest()
      throws JSONException {
    JSONObject executionData = new JSONObject();
    JSONObject operationData = new JSONObject();
    operationData.put("dummyData", "dummyData");
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "@@@UnknownOperation@@@");
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  // specialGetWitBadExecutionDataTest() helpers
  private MockRestRequest createRequestWithExecDataNotValidJSON()
      throws JSONException {
    JSONObject headers = new JSONObject();
    headers.put("executionData", "@@@InvalidJSON@@@");

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  private MockRestRequest createRequestWithExecDataMissingOpType()
      throws JSONException {
    JSONObject operationData = new JSONObject();
    operationData.put("text", "text");
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }

  private MockRestRequest createRequestWithExecDataMissingOpData()
      throws JSONException {
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY, "Echo");

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createRestRequest(RestMethod.GET, "/", headers);
  }
}
