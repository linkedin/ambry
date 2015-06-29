package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequestContent;
import com.github.ambry.rest.MockRestRequestMetadata;
import com.github.ambry.rest.MockRestResponseHandler;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequestContent;
import com.github.ambry.rest.RestRequestInfo;
import com.github.ambry.rest.RestRequestMetadata;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link AdminBlobStorageService}.
 */
public class AdminBlobStorageServiceTest {

  private static ClusterMap clusterMap;
  private static AdminBlobStorageService adminBlobStorageService;

  /**
   * Sets up the {@link AdminBlobStorageService} instance before all tests. Most tests use the instance started here.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startAdminBlobStorageService()
      throws InstantiationException, IOException {
    clusterMap = new MockClusterMap();
    adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    adminBlobStorageService.start();
  }

  /**
   * Shuts down the {@link AdminBlobStorageService} instance after all tests.
   */
  @AfterClass
  public static void shutdownAdminBlobStorageService() {
    adminBlobStorageService.shutdown();
  }

  /**
   * Tests basic startup and shutdown functionality (no exceptions).
   * @throws InstantiationException
   */
  @Test
  public void startShutDownTest()
      throws InstantiationException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    adminBlobStorageService.start();
    adminBlobStorageService.shutdown();
  }

  /**
   * Tests for {@link AdminBlobStorageService#shutdown()} when {@link AdminBlobStorageService#start()} has not been
   * called previously.
   * <p/>
   * This test is for  cases where {@link AdminBlobStorageService#start()} has failed and
   * {@link AdminBlobStorageService#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws IOException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    adminBlobStorageService.shutdown();
  }

  /**
   * This tests for exceptions thrown when an {@link AdminBlobStorageService} instance is used without calling
   * {@link AdminBlobStorageService#start()} first.
   * first.
   * @throws InstantiationException
   * @throws JSONException
   * @throws URISyntaxException
   * @throws RestServiceException
   */
  @Test
  public void useServiceWithoutStartTest()
      throws InstantiationException, JSONException, URISyntaxException, RestServiceException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    // fine to use without start.
    doEchoTest(adminBlobStorageService);
  }

  /**
   * Tests blob GET operations.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleGetTest()
      throws JSONException, RestServiceException, URISyntaxException {
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.GET, "/", new JSONObject());
    try {
      adminBlobStorageService.handleGet(restRequestInfo);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  /**
   * Tests blob POST operations.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handlePostTest()
      throws JSONException, RestServiceException, URISyntaxException {
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.POST, "/", new JSONObject());
    try {
      adminBlobStorageService.handlePost(restRequestInfo);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  /**
   * Tests blob DELETE operations.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleDeleteTest()
      throws JSONException, RestServiceException, URISyntaxException {
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.DELETE, "/", new JSONObject());
    try {
      adminBlobStorageService.handleDelete(restRequestInfo);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  /**
   * Tests blob HEAD operations.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleHeadTest()
      throws JSONException, RestServiceException, URISyntaxException {
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.HEAD, "/", new JSONObject());
    try {
      adminBlobStorageService.handleHead(restRequestInfo);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  /**
   * Tests the {@link AdminOperationType#echo} admin operation. Checks to see that the echo matches input text.
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleEchoTest()
      throws InstantiationException, JSONException, RestServiceException, URISyntaxException {
    doEchoTest(adminBlobStorageService);
  }

  /**
   * Tests reactions of the {@link AdminOperationType#echo} operation to bad input - specifically if we do not include
   * required parameters.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleEchoWithBadInputTest()
      throws JSONException, RestServiceException, URISyntaxException {
    try {
      // bad input - uri does not have text that needs to be echoed.
      RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.GET, AdminOperationType.echo.toString(), null);
      adminBlobStorageService.handleGet(restRequestInfo);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }
  }

  /**
   * Tests the {@link AdminOperationType#getReplicasForBlobId} admin operation.
   * <p/>
   * For the each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created.
   * The string representation is sent to the {@link AdminBlobStorageService} as a part of getReplicasForBlobId request.
   * The returned replica list is checked for equality against a locally obtained replica list.
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleGetReplicasForBlobIdTest()
      throws InstantiationException, JSONException, RestServiceException, URISyntaxException {
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      RestRequestInfo restRequestInfo = createGetReplicasForBlobIdRestRequestInfo(blobId.getID());
      adminBlobStorageService.handleGet(restRequestInfo);
      finishGetRequest(adminBlobStorageService, restRequestInfo);
      String returnedReplicasStr =
          getJsonizedResponseBody(restRequestInfo).getString(GetReplicasForBlobIdHandler.REPLICAS_KEY)
              .replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests reactions of the {@link AdminOperationType#getReplicasForBlobId} operation to bad input - specifically if we
   * do not include required parameters.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleGetReplicasForBlobIdWithBadInputTest()
      throws JSONException, RestServiceException, URISyntaxException {
    try {
      // bad input - uri missing the blob id whose replicas need to be returned.
      RestRequestInfo restRequestInfo =
          createRestRequestInfo(RestMethod.GET, AdminOperationType.getReplicasForBlobId.toString(), null);
      adminBlobStorageService.handleGet(restRequestInfo);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }

    try {
      // bad input - invalid blob id.
      RestRequestInfo restRequestInfo = createGetReplicasForBlobIdRestRequestInfo("12345");
      adminBlobStorageService.handleGet(restRequestInfo);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to operations that are unknown/not defined.
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void unknownCustomGetOperationTest()
      throws JSONException, RestServiceException, URISyntaxException {
    try {
      RestRequestInfo restRequestInfo =
          createRestRequestInfo(RestMethod.GET, "unknownOperation?dummyParam=dummyValue", null);
      adminBlobStorageService.handleGet(restRequestInfo);
      fail("Exception should have been thrown because an unknown operation has been specified");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to operations that have bad input.
   * 1. No operation specified
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void customGetWithBadUriTest()
      throws JSONException, RestServiceException, URISyntaxException {
    // No operation specified
    try {
      // bad input - no operation specified
      RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.GET, "/", null);
      adminBlobStorageService.handleGet(restRequestInfo);
      fail("Exception should have been thrown because no operation has been specified");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedOperation, e.getErrorCode());
    }
  }

  // helpers
  // general

  /**
   * Method to easily create RestRequestInfo objects containing a specific request.
   * @param restMethod - the {@link RestMethod} desired.
   * @param uri - string representation of the desired URI.
   * @param headers - any associated headers as a {@link JSONObject}.
   * @return A {@link RestRequestInfo} object that defines the operation required by the input along with a
   * {@link com.github.ambry.rest.RestResponseHandler}.
   * @throws JSONException
   * @throws URISyntaxException
   */
  private RestRequestInfo createRestRequestInfo(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequestMetadata.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequestMetadata.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequestMetadata.HEADERS_KEY, headers);
    }
    RestRequestMetadata restRequestMetadata = new MockRestRequestMetadata(request);
    return new RestRequestInfo(restRequestMetadata, null, new MockRestResponseHandler(), true);
  }

  /**
   * Extracts the response received from the {@link AdminBlobStorageService} and decodes it into a {@link JSONObject}.
   * @param restRequestInfo - the {@link RestRequestInfo} that was sent to {@link AdminBlobStorageService}.
   * @return - the response decoded into a {@link JSONObject}.
   * @throws JSONException
   * @throws RestServiceException
   */
  private JSONObject getJsonizedResponseBody(RestRequestInfo restRequestInfo)
      throws JSONException, RestServiceException {
    MockRestResponseHandler restResponseHandler = (MockRestResponseHandler) restRequestInfo.getRestResponseHandler();
    return new JSONObject(restResponseHandler.getFlushedResponseBody());
  }

  /**
   * Concludes a test by putting in the end marker (last {@link RestRequestContent}) and checks that the
   * {@link AdminBlobStorageService} interprets it correctly.
   * @param adminBlobStorageService
   * @param restRequestInfo
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   */
  private void finishGetRequest(AdminBlobStorageService adminBlobStorageService, RestRequestInfo restRequestInfo)
      throws InstantiationException, JSONException, RestServiceException {
    RestRequestContent restRequestContent = createRestContent(null, true);
    adminBlobStorageService.handleGet(new RestRequestInfo(restRequestInfo.getRestRequestMetadata(), restRequestContent,
        restRequestInfo.getRestResponseHandler()));
    assertFalse("Channel is not closed",
        ((MockRestResponseHandler) restRequestInfo.getRestResponseHandler()).getChannelActive());
  }

  /**
   * Method to easily create {@link RestRequestContent}.
   * @param content - the actual content that forms the underlying data.
   * @param isLast - true if this the last part of the content in a request.
   * @return - A {@link RestRequestContent} object with the specified content and behaviour.
   * @throws InstantiationException
   * @throws JSONException
   */
  private RestRequestContent createRestContent(String content, boolean isLast)
      throws InstantiationException, JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequestContent.IS_LAST_KEY, isLast);
    if (content == null) {
      content = "";
    }
    data.put(MockRestRequestContent.CONTENT_KEY, content);
    return new MockRestRequestContent(data);
  }

  /**
   * Does the {@link AdminOperationType#echo} test by creating a {@link RestRequestInfo} specifying echo, sends it to
   * the {@link AdminBlobStorageService} instance and checks equality of response with input text.
   * @param adminBlobStorageService
   * @throws InstantiationException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  private void doEchoTest(AdminBlobStorageService adminBlobStorageService)
      throws InstantiationException, JSONException, RestServiceException, URISyntaxException {
    String inputText = "textToBeEchoed";
    String uri = AdminOperationType.echo + "?" + EchoHandler.TEXT_KEY + "=" + inputText;
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.GET, uri, null);
    adminBlobStorageService.handleGet(restRequestInfo);
    finishGetRequest(adminBlobStorageService, restRequestInfo);
    String echoedText = getJsonizedResponseBody(restRequestInfo).getString(EchoHandler.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  // BeforeClass helpers
  private static AdminBlobStorageService getAdminBlobStorageService(ClusterMap clusterMap) {
    // dud properties. pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    AdminConfig adminConfig = new AdminConfig(verifiableProperties);
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    return new AdminBlobStorageService(adminConfig, adminMetrics, clusterMap);
  }

  // handleGetReplicasForBlobIdTest() helpers
  // handleGetReplicasForBlobIdWithBadInputTest() helpers
  private RestRequestInfo createGetReplicasForBlobIdRestRequestInfo(String blobId)
      throws JSONException, URISyntaxException {
    String uri = AdminOperationType.getReplicasForBlobId + "?" + GetReplicasForBlobIdHandler.BLOB_ID_KEY + "=" + blobId;
    return createRestRequestInfo(RestMethod.GET, uri, null);
  }
}
