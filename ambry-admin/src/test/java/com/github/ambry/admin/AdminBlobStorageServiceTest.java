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
 * Unit tests for AdminBlobStorageService
 */
public class AdminBlobStorageServiceTest {

  /**
   * Tests basic startup and shutdown functionality (no exceptions).
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutDownTest()
      throws InstantiationException, IOException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    adminBlobStorageService.start();
    adminBlobStorageService.shutdown();
  }

  /**
   * Tests traditional GET operations.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test(expected = IllegalStateException.class)
  public void handleGetTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.GET, "/", new JSONObject());
    adminBlobStorageService.handleGet(messageInfo);
  }

  /**
   * Tests traditional POST operations.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test(expected = IllegalStateException.class)
  public void handlePostTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.POST, "/", new JSONObject());
    adminBlobStorageService.handlePost(messageInfo);
  }

  /**
   * Tests traditional DELETE operations.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test(expected = IllegalStateException.class)
  public void handleDeleteTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.DELETE, "/", new JSONObject());
    adminBlobStorageService.handleDelete(messageInfo);
  }

  /**
   * Tests traditional HEAD operations.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test(expected = IllegalStateException.class)
  public void handleHeadTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    MessageInfo messageInfo = createMessageInfo(RestMethod.HEAD, "/", new JSONObject());
    adminBlobStorageService.handleHead(messageInfo);
  }

  /**
   * Tests a custom GET operation - Echo. Checks to see that the echo matches input text.
   * @throws InstantiationException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleEchoTest()
      throws InstantiationException, IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    String inputText = "textToBeEchoed";
    MessageInfo messageInfo = createEchoMessageInfo(inputText);
    adminBlobStorageService.handleGet(messageInfo);
    String echoedText = getJsonizedResponseBody(messageInfo).getString(EchoExecutor.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
    finishUpHandleGetCheck(adminBlobStorageService, messageInfo);
  }

  /**
   * Tests reactions of the echo executor to bad input - specifically if we do not include required operationData.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleEchoWithBadInputTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createBadEchoMessageInfo();
      adminBlobStorageService.handleGet(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }
  }

  /**
   * Tests a custom GET operation - GetReplicasForBlobId. Matches the replicas returned from AdminBlobStorageService
   * with replicas obtained locally.
   * @throws InstantiationException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
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

  /**
   * Tests reactions of the GetReplicasForBlobId executor to bad input - specifically if we do not include
   * required operationData.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleGetReplicasForBlobIdWithBadInputTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createBadGetReplicasForBlobIdMessageInfo();
      adminBlobStorageService.handleGet(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to custom operations that are not defined. Should throw exception.
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void customGetUnknownOperationTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    try {
      AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
      MessageInfo messageInfo = createUnknownCustomGetOperationMessageInfo();
      adminBlobStorageService.handleGet(messageInfo);
      fail("Test should have thrown Exception");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnknownCustomOperationType,
          e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to custom operations that have bad executionData. Should throw exceptions
   * -> no operationType
   * -> no operationData
   * -> invalid JSON
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void customGetWithBadExecutionDataTest()
      throws IOException, JSONException, RestServiceException, URISyntaxException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    // no operationType
    try {
      MessageInfo messageInfo = createMesageInfoWithExecDataMissingOpType();
      adminBlobStorageService.handleGet(messageInfo);
      fail("executionData header was missing " + AdminExecutionData.OPERATION_TYPE_KEY
          + ", yet no exception was thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }

    // no operationData
    try {
      MessageInfo messageInfo = createMessageInfoWithExecDataMissingOpData();
      adminBlobStorageService.handleGet(messageInfo);
      fail("executionData header was missing " + AdminExecutionData.OPERATION_DATA_KEY
          + " , yet no exception was thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadExecutionData, e.getErrorCode());
    }

    // executionData invalid JSON
    try {
      MessageInfo messageInfo = createMessageInfoWithExecDataNotValidJSON();
      adminBlobStorageService.handleGet(messageInfo);
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

  /**
   * Method to easily construct requests.
   * @param restMethod
   * @param uri
   * @param headers
   * @return
   * @throws JSONException
   * @throws URISyntaxException
   */
  private RestRequest getRestRequest(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    request.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(request);
  }

  /**
   * Method to easily create message information objects.
   * @param restMethod
   * @param uri
   * @param headers
   * @return
   * @throws JSONException
   * @throws URISyntaxException
   */
  private MessageInfo createMessageInfo(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    RestRequest restRequest = getRestRequest(restMethod, uri, headers);
    return new MessageInfo(restRequest, restRequest, new MockRestResponseHandler());
  }

  /**
   * Decodes the response received from the BlobStorageService.
   * @param messageInfo
   * @return
   * @throws JSONException
   * @throws RestServiceException
   */
  private JSONObject getJsonizedResponseBody(MessageInfo messageInfo)
      throws JSONException, RestServiceException {
    MockRestResponseHandler restResponseHandler = (MockRestResponseHandler) messageInfo.getResponseHandler();
    return new JSONObject(restResponseHandler.getFlushedBody());
  }

  /**
   * Concludes the test by putting in the end marker (last RestContent) and checks that the BlobStorageService
   * interprets it correctly.
   * @param adminBlobStorageService
   * @param messageInfo
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   */
  private void finishUpHandleGetCheck(AdminBlobStorageService adminBlobStorageService, MessageInfo messageInfo)
      throws InstantiationException, JSONException, RestServiceException {
    RestContent lastContent = createRestContent(true);
    adminBlobStorageService
        .handleGet(new MessageInfo(messageInfo.getRestRequest(), lastContent, messageInfo.getResponseHandler()));
    assertTrue("Channel is not closed", ((MockRestResponseHandler) messageInfo.getResponseHandler()).isChannelClosed());
  }

  /**
   * Method to easily create RestContent
   * @param isLast
   * @return
   * @throws InstantiationException
   * @throws JSONException
   */
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
    // bad input - operation data does not have text that needs to be echoed.
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "Echo");
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // handleGetReplicasForBlobIdTest() helpers

  /**
   * For each parition in the cluster map, a blob id is created. This blob id is sent to the server and the returned
   * replica list is checked against the locally obtained replica list.
   * @param partitionId
   * @param adminBlobStorageService
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  private void createBlobIdAndTest(PartitionId partitionId, AdminBlobStorageService adminBlobStorageService)
      throws InstantiationException, JSONException, RestServiceException, URISyntaxException {
    String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
    BlobId blobId = new BlobId(partitionId);
    MessageInfo messageInfo = createGetReplicasForBlobIdMessageInfo(blobId.getID());
    adminBlobStorageService.handleGet(messageInfo);
    String returnedReplicasStr =
        getJsonizedResponseBody(messageInfo).getString(GetReplicasForBlobIdExecutor.REPLICAS_KEY).replace("\"", "");
    assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
        originalReplicaStr, returnedReplicasStr);
    finishUpHandleGetCheck(adminBlobStorageService, messageInfo);
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
    // bad input - operationData missing the blob id whose replicas need to be returned.
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
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "@@@UnknownOperation@@@");  // bad input - unknown cust op
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);
    headers.put(AdminBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  // customGetWithBadExecutionDataTest() helpers
  private MessageInfo createMessageInfoWithExecDataNotValidJSON()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    headers.put("executionData", "@@@InvalidJSON@@@"); // bad input - execution data is not a json object

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  private MessageInfo createMesageInfoWithExecDataMissingOpType()
      throws JSONException, URISyntaxException {
    JSONObject operationData = new JSONObject();
    operationData.put("text", "text");
    JSONObject executionData = new JSONObject();
    // bad input - operation type missing
    executionData.put(AdminExecutionData.OPERATION_DATA_KEY, operationData);

    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  private MessageInfo createMessageInfoWithExecDataMissingOpData()
      throws JSONException, URISyntaxException {
    JSONObject executionData = new JSONObject();
    executionData.put(AdminExecutionData.OPERATION_TYPE_KEY, "Echo");
    // bad input - operation data missing
    JSONObject headers = new JSONObject();
    headers.put("executionData", executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }
}
