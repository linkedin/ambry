package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest()
      throws Exception {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap);
    // fine to use without start.
    doEchoTest(adminBlobStorageService, AdminUsage.WithCallback);
  }

  /**
   * Tests blob GET operations.
   * @throws Exception
   */
  @Test
  public void getBlobTest()
      throws Exception {
    doGetBlobTest(adminBlobStorageService, AdminUsage.WithCallback);
    doGetBlobTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests blob POST operations.
   * @throws Exception
   */
  @Test
  public void postBlobTest()
      throws Exception {
    doPostBlobTest(adminBlobStorageService, AdminUsage.WithCallback);
    doPostBlobTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests blob DELETE operations.
   * @throws Exception
   */
  @Test
  public void deleteBlobTest()
      throws Exception {
    doDeleteBlobTest(adminBlobStorageService, AdminUsage.WithCallback);
    doDeleteBlobTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests blob HEAD operations.
   * @throws Exception
   */
  @Test
  public void headBlobTest()
      throws Exception {
    doHeadBlobTest(adminBlobStorageService, AdminUsage.WithCallback);
    doHeadBlobTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests the {@link AdminOperationType#echo} admin operation. Checks to see that the echo matches input text.
   * @throws Exception
   */
  @Test
  public void echoTest()
      throws Exception {
    doEchoTest(adminBlobStorageService, AdminUsage.WithCallback);
    doEchoTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests reactions of the {@link AdminOperationType#echo} operation to bad input - specifically if we do not include
   * required parameters.
   * @throws Exception
   */
  @Test
  public void echoWithBadInputTest()
      throws Exception {
    doEchoWithBadInputTest(adminBlobStorageService, AdminUsage.WithCallback);
    doEchoWithBadInputTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests the {@link AdminOperationType#getReplicasForBlobId} admin operation.
   * <p/>
   * For the each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created.
   * The string representation is sent to the {@link AdminBlobStorageService} as a part of getReplicasForBlobId request.
   * The returned replica list is checked for equality against a locally obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasForBlobIdTest()
      throws Exception {
    doGetReplicasForBlobIdTest(adminBlobStorageService, AdminUsage.WithCallback);
    doGetReplicasForBlobIdTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests reactions of the {@link AdminOperationType#getReplicasForBlobId} operation to bad input - specifically if we
   * do not include required parameters.
   * @throws Exception
   */
  @Test
  public void getReplicasForBlobIdWithBadInputTest()
      throws Exception {
    doGetReplicasForBlobIdWithBadInputTest(adminBlobStorageService, AdminUsage.WithCallback);
    doGetReplicasForBlobIdWithBadInputTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests reaction of GET to operations that are unknown/not defined.
   * @throws Exception
   */
  @Test
  public void unknownCustomGetOperationTest()
      throws Exception {
    doUnknownCustomGetOperationTest(adminBlobStorageService, AdminUsage.WithCallback);
    doUnknownCustomGetOperationTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  /**
   * Tests reaction of GET to operations that have bad input.
   * 1. No operation specified
   * @throws Exception
   */
  @Test
  public void customGetWithBadUriTest()
      throws Exception {
    doCustomGetWithBadUriTest(adminBlobStorageService, AdminUsage.WithCallback);
    doCustomGetWithBadUriTest(adminBlobStorageService, AdminUsage.WithoutCallback);
  }

  // helpers
  // general

  /**
   * Method to easily create {@link RestRequest} objects containing a specific request.
   * @param restMethod the {@link RestMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link JSONObject}.
   * @return A {@link RestRequest} object that defines the request required by the input.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(request);
  }

  /**
   * Reads the response received from the {@link AdminBlobStorageService} and decodes it into a {@link JSONObject}.
   * @param channel the {@link ReadableStreamChannel} that was received from the {@link AdminBlobStorageService}.
   * @return the response decoded into a {@link JSONObject}.
   * @throws IOException
   * @throws JSONException
   */
  private JSONObject getJsonizedResponseBody(ReadableStreamChannel channel)
      throws IOException, JSONException {
    ByteBuffer responseBuffer = ByteBuffer.allocate((int) channel.getSize());
    channel.read(new ByteBufferChannel(responseBuffer));
    return new JSONObject(new String(responseBuffer.array()));
  }

  /**
   * Verifies that the exceptions thrown by the {@code future} and the {@code callback} are the same.
   * @param callback the {@link AdminOperationCallback} that contains an exception.
   * @param future the {@link Future} that contains an exception.
   */
  private void verifyFutureCallbackExceptionMatch(AdminOperationCallback callback, Future future) {
    try {
      future.get();
      fail("Callback had an exception but future.get() did not throw exception");
    } catch (Exception e) {
      assertEquals("Callback and future exceptions do not match", callback.getException(), e.getCause().getCause());
    }
  }

  /**
   * Recursively looks inside {@code e} to find a {@link RestServiceException} and match the error code with
   * {@code restServiceErrorCode}. If a {@link RestServiceException} is not found, or if the error code does not match,
   * rethrows {@code e}.
   * @param e the {@link Exception} to look work on.
   * @param restServiceErrorCode the {@link RestServiceErrorCode} expected.
   * @throws Exception
   */
  private void matchRestServiceErrorCode(Exception e, RestServiceErrorCode restServiceErrorCode)
      throws Exception {
    Throwable throwable = e;
    while (throwable != null) {
      if (throwable instanceof RestServiceException) {
        assertEquals("Unexpected RestServiceException", restServiceErrorCode,
            ((RestServiceException) throwable).getErrorCode());
        return;
      }
      throwable = throwable.getCause();
    }
    throw e;
  }

  /**
   * Does a {@link AdminBlobStorageService#handleGet(RestRequest, Callback)} and returns the result, if any. If an
   * exception occurs during the operation, throws the exception. Also matches the result/exception of both the future
   * and callback (if not null).
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param handleGetCallback the {@link AdminOperationCallback} to invoke on operation completion. Can be null.
   * @return a {@link ReadableStreamChannel} containing the result of the GET.
   * @throws Exception if any exceptions occurred during the operation.
   */
  private ReadableStreamChannel doGet(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      AdminOperationCallback<ReadableStreamChannel> handleGetCallback)
      throws Exception {
    Future<ReadableStreamChannel> handleGetFuture = adminBlobStorageService.handleGet(restRequest, handleGetCallback);
    if (handleGetCallback != null) {
      if (handleGetCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("handleGet: Future is not done but callback has been received", handleGetFuture.isDone());
        if (handleGetCallback.getException() == null) {
          assertEquals("handleGet: Future Blob and callback Blob do not match", handleGetFuture.get(),
              handleGetCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(handleGetCallback, handleGetFuture);
          throw handleGetCallback.getException();
        }
      } else {
        throw new IllegalStateException("handleGet() timed out");
      }
    }
    return handleGetFuture.get();
  }

  /**
   * Does a {@link AdminBlobStorageService#handlePost(RestRequest, Callback)} and returns the result, if any. If an
   * exception occurs during the operation, throws the exception. Also matches the result/exception of both the future
   * and callback (if not null).
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param handlePostCallback the {@link AdminOperationCallback} to invoke on operation completion. Can be null.
   * @return a {@link String} containing the result of the POST.
   * @throws Exception if any exceptions occurred during the operation.
   */
  private String doPost(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      AdminOperationCallback<String> handlePostCallback)
      throws Exception {
    Future<String> handlePostFuture = adminBlobStorageService.handlePost(restRequest, handlePostCallback);
    if (handlePostCallback != null) {
      if (handlePostCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("handlePost: Future is not done but callback has been received", handlePostFuture.isDone());
        if (handlePostCallback.getException() == null) {
          assertEquals("handlePost: Future String and callback String do not match", handlePostFuture.get(),
              handlePostCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(handlePostCallback, handlePostFuture);
          throw handlePostCallback.getException();
        }
      } else {
        throw new IllegalStateException("handlePost() timed out");
      }
    }
    return handlePostFuture.get();
  }

  /**
   * Does a {@link AdminBlobStorageService#handleDelete(RestRequest, Callback)} and returns the result, if any. If an
   * exception occurs during the operation, throws the exception. Also matches the result/exception of both the future
   * and callback (if not null).
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param handleDeleteCallback the {@link AdminOperationCallback} to invoke on operation completion. Can be null.
   * @return a {@link String} containing the result of the DELETE.
   * @throws Exception if any exceptions occurred during the operation.
   */
  private Void doDelete(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      AdminOperationCallback<Void> handleDeleteCallback)
      throws Exception {
    Future<Void> handleDeleteFuture = adminBlobStorageService.handleDelete(restRequest, handleDeleteCallback);
    if (handleDeleteCallback != null) {
      if (handleDeleteCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("handleDelete: Future is not done but callback has been received", handleDeleteFuture.isDone());
        if (handleDeleteCallback.getException() != null) {
          verifyFutureCallbackExceptionMatch(handleDeleteCallback, handleDeleteFuture);
          throw handleDeleteCallback.getException();
        }
      } else {
        throw new IllegalStateException("handleDelete() timed out");
      }
    }
    return handleDeleteFuture.get();
  }

  /**
   * Does a {@link AdminBlobStorageService#handleHead(RestRequest, Callback)} and returns the result, if any. If an
   * exception occurs during the operation, throws the exception. Also matches the exception of both the future
   * and callback (if not null).
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param handleHeadCallback the {@link AdminOperationCallback} to invoke on operation completion. Can be null.
   * @return a {@link String} containing the result of the HEAD.
   * @throws Exception if any exceptions occurred during the operation.
   */
  private BlobInfo doHead(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      AdminOperationCallback<BlobInfo> handleHeadCallback)
      throws Exception {
    Future<BlobInfo> handleHeadFuture = adminBlobStorageService.handleHead(restRequest, handleHeadCallback);
    if (handleHeadCallback != null) {
      if (handleHeadCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("handleHead: Future is not done but callback has been received", handleHeadFuture.isDone());
        if (handleHeadCallback.getException() == null) {
          assertEquals("handleHead: Future BlobInfo and callback BlobInfo do not match", handleHeadFuture.get(),
              handleHeadCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(handleHeadCallback, handleHeadFuture);
          throw handleHeadCallback.getException();
        }
      } else {
        throw new IllegalStateException("handleHead() timed out");
      }
    }
    return handleHeadFuture.get();
  }

  /**
   * Does the {@link AdminOperationType#echo} test by creating a {@link RestRequest} specifying echo, sends it to
   * the {@link AdminBlobStorageService} instance and checks equality of response with input text.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doEchoTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    String inputText = "textToBeEchoed";
    String uri = AdminOperationType.echo + "?" + EchoHandler.TEXT_KEY + "=" + inputText;
    RestRequest restRequest = createRestRequest(RestMethod.GET, uri, null);
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    ReadableStreamChannel channel = doGet(adminBlobStorageService, restRequest, handleGetCallback);
    String echoedText = getJsonizedResponseBody(channel).getString(EchoHandler.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  // BeforeClass helpers

  /**
   * Sets up and gets an instance of {@link AdminBlobStorageService}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return an instance of {@link AdminBlobStorageService}.
   */
  private static AdminBlobStorageService getAdminBlobStorageService(ClusterMap clusterMap) {
    // dud properties. pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    AdminConfig adminConfig = new AdminConfig(verifiableProperties);
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    return new AdminBlobStorageService(adminConfig, adminMetrics, clusterMap);
  }

  // getBlobTest() helpers

  /**
   * Does the test for a blob GET.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doGetBlobTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    try {
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }

  // postBlobTest() helpers

  /**
   *  Does the test for a blob POST.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doPostBlobTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<String> handlePostCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handlePostCallback = new AdminOperationCallback<String>();
    }
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", new JSONObject());
    try {
      doPost(adminBlobStorageService, restRequest, handlePostCallback);
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }

  // deleteBlobTest() helpers

  /**
   *  Does the test for a blob DELETE.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doDeleteBlobTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<Void> handleDeleteCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleDeleteCallback = new AdminOperationCallback<Void>();
    }
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, "/", new JSONObject());
    try {
      doDelete(adminBlobStorageService, restRequest, handleDeleteCallback);
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }

  // headBlobTest() helpers

  /**
   *  Does the test for a blob HEAD.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doHeadBlobTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<BlobInfo> handleHeadCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleHeadCallback = new AdminOperationCallback<BlobInfo>();
    }
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, "/", new JSONObject());
    try {
      doHead(adminBlobStorageService, restRequest, handleHeadCallback);
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }

  // echoWithBadInputTest() helpers

  /**
   * Tests for the reaction of {@link EchoHandler} when bad input is provided for the echo operation. Checks that the
   * right {@link RestServiceException} is thrown.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doEchoWithBadInputTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    // bad input - uri does not have text that needs to be echoed.
    RestRequest restRequest = createRestRequest(RestMethod.GET, AdminOperationType.echo.toString(), null);
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    try {
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.MissingArgs);
    }
  }

  // handleGetReplicasForBlobIdTest() helpers

  /**
   * Does the test for {@link AdminOperationType#getReplicasForBlobId}. For every partition in a {@link ClusterMap},
   * creates a blob ID and queries for its replicas. It then compares the response received to a replica list obtained
   * locally.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doGetReplicasForBlobIdTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      if (handleGetCallback != null) {
        handleGetCallback.reset();
      }
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId.getID());
      ReadableStreamChannel channel = doGet(adminBlobStorageService, restRequest, handleGetCallback);
      String returnedReplicasStr =
          getJsonizedResponseBody(channel).getString(GetReplicasForBlobIdHandler.REPLICAS_KEY).replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }
  // handleGetReplicasForBlobIdWithBadInputTest() helpers

  /**
   * Tests the reaction of the {@link GetReplicasForBlobIdHandler} when bad input is provided i.e missing or invalid
   * arguments. Checks that the right {@link RestServiceException} is thrown.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doGetReplicasForBlobIdWithBadInputTest(AdminBlobStorageService adminBlobStorageService,
      AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    try {
      // bad input - uri missing the blob id whose replicas need to be returned.
      RestRequest restRequest =
          createRestRequest(RestMethod.GET, AdminOperationType.getReplicasForBlobId.toString(), null);
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.MissingArgs);
    } finally {
      if (handleGetCallback != null) {
        handleGetCallback.reset();
      }
    }

    try {
      // bad input - invalid blob id.
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest("12345");
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.InvalidArgs);
    } finally {
      if (handleGetCallback != null) {
        handleGetCallback.reset();
      }
    }

    try {
      // bad input - invalid blob id for this cluster map.
      String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId);
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.InvalidArgs);
    } finally {
      if (handleGetCallback != null) {
        handleGetCallback.reset();
      }
    }
  }

  /**
   * Creates a request that can be used to test {@link AdminOperationType#getReplicasForBlobId}.
   * @param blobId the blob ID to include in the request.
   * @return a {@link RestRequest} for {@link AdminOperationType#getReplicasForBlobId} with blob ID as specified.
   * @throws JSONException
   * @throws URISyntaxException
   * @throws UnsupportedEncodingException
   */
  private RestRequest createGetReplicasForBlobIdRestRequest(String blobId)
      throws JSONException, URISyntaxException, UnsupportedEncodingException {
    String uri = AdminOperationType.getReplicasForBlobId + "?" + GetReplicasForBlobIdHandler.BLOB_ID_KEY + "=" + blobId;
    return createRestRequest(RestMethod.GET, uri, null);
  }

  // unknownCustomGetOperationTest() helpers

  /**
   * Does a GET with an invalid admin operation specified and checks that the right {@link RestServiceException} is
   * thrown.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doUnknownCustomGetOperationTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    try {
      RestRequest restRequest = createRestRequest(RestMethod.GET, "unknownOperation?dummyParam=dummyValue", null);
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because an unknown operation has been specified");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }

  // customGetWithBadUriTest() helpers

  /**
   * Does the GET operation with a bad URI and checks that the right {@link RestServiceException} is thrown.
   * <p/>
   * Creates a callback if {@code adminUsage} is {@link AdminUsage#WithCallback}, otherwise uses a null callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param adminUsage specifies whether a callback should be created or not.
   * @throws Exception
   */
  private void doCustomGetWithBadUriTest(AdminBlobStorageService adminBlobStorageService, AdminUsage adminUsage)
      throws Exception {
    AdminOperationCallback<ReadableStreamChannel> handleGetCallback = null;
    if (AdminUsage.WithCallback.equals(adminUsage)) {
      handleGetCallback = new AdminOperationCallback<ReadableStreamChannel>();
    }
    try {
      // bad input - no operation specified
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null);
      doGet(adminBlobStorageService, restRequest, handleGetCallback);
      fail("Exception should have been thrown because no operation has been specified");
    } catch (Exception e) {
      matchRestServiceErrorCode(e, RestServiceErrorCode.UnsupportedOperation);
    }
  }
}

/**
 * Enum used to select whether to use the callback or non callback variant of {@link AdminBlobStorageService} APIs.
 */
enum AdminUsage {
  WithCallback,
  WithoutCallback
}

/**
 * Class that can be used to receive callbacks from {@link AdminBlobStorageService}.
 * <p/>
 * On callback, stores the result and exception to be retrieved for later use.
 * @param <T> the type of result expected.
 */
class AdminOperationCallback<T> implements Callback<T> {
  private CountDownLatch callbackReceived = new CountDownLatch(1);
  private T result = null;
  private Exception exception = null;

  /**
   * Contains the result of the operation for which this was set as callback.
   * <p/>
   * If there was no result or if this was called before callback is received, it will return null.
   * @return the result of the operation (if any) for which this was set as callback.
   */
  public T getResult() {
    return result;
  }

  /**
   * Stores any exception thrown by the operation for which this was set as callback.
   * <p/>
   * If there was no exception or if this was called before callback is received, it will return null.
   * @return the exception encountered while performing the operation (if any) for which this was set as callback.
   */
  public Exception getException() {
    return exception;
  }

  @Override
  public void onCompletion(T result, Exception exception) {
    this.result = result;
    this.exception = exception;
    callbackReceived.countDown();
  }

  /**
   * Waits for the callback to be received.
   * @param timeout the time to wait for.
   * @param timeUnit the time unit of {@code timeout}.
   * @return {@code true} if callback was received within the timeout, {@code false} otherwise.
   * @throws InterruptedException if the wait for callback was interrupted.
   */
  public boolean awaitCallback(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return callbackReceived.await(timeout, timeUnit);
  }

  /**
   * Reset the state of this callback.
   */
  public void reset() {
    callbackReceived = new CountDownLatch(1);
    result = null;
    exception = null;
  }
}
