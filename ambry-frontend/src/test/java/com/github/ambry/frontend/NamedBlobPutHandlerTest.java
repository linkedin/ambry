/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.DatasetBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class NamedBlobPutHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final Account REF_ACCOUNT;
  private static final Container REF_CONTAINER;
  private static final Container REF_CONTAINER_WITH_TTL_REQUIRED;
  private static final ClusterMap CLUSTER_MAP;

  private static final int TIMEOUT_SECS = 10;
  private static final String SERVICE_ID = "test-app";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String OWNER_ID = "tester";
  private static final String CONVERTED_ID = "/abcdef";
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String NAMED_BLOB_PREFIX = "/named";
  private static final String SLASH = "/";
  private static final String BLOBNAME = "ambry_blob_name";
  private static final String DATASET_NAME = "testDataset";
  private static final String VERSION = "1";

  private final NamedBlobDb namedBlobDb;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      Account account = ACCOUNT_SERVICE.createAndAddRandomAccount();
      Container publicContainer = account.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
      Container ttlRequiredContainer = new ContainerBuilder(publicContainer).setTtlRequired(true).build();
      Account newAccount = new AccountBuilder(account).addOrUpdateContainer(ttlRequiredContainer).build();
      ACCOUNT_SERVICE.updateAccounts(Collections.singleton(newAccount));
      REF_ACCOUNT = ACCOUNT_SERVICE.getAccountById(newAccount.getId());
      REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
      REF_CONTAINER_WITH_TTL_REQUIRED = REF_ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
      ACCOUNT_SERVICE.addDataset(
          new DatasetBuilder(REF_ACCOUNT.getName(), REF_CONTAINER.getName(), DATASET_NAME).setRetentionTimeInSeconds(-1)
              .build());
      ACCOUNT_SERVICE.addDataset(new DatasetBuilder(REF_ACCOUNT.getName(), REF_CONTAINER_WITH_TTL_REQUIRED.getName(),
          DATASET_NAME).setRetentionTimeInSeconds(-1).build());
      ACCOUNT_SERVICE.addDatasetVersion(REF_ACCOUNT.getName(), REF_CONTAINER.getName(), DATASET_NAME, VERSION, -1,
          System.currentTimeMillis(), false);
      ACCOUNT_SERVICE.addDatasetVersion(REF_ACCOUNT.getName(), REF_CONTAINER_WITH_TTL_REQUIRED.getName(), DATASET_NAME,
          VERSION, -1, System.currentTimeMillis(), false);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final MockTime time = new MockTime();
  private final FrontendMetrics metrics;
  private final InMemoryRouter router;
  private final FrontendTestIdConverterFactory idConverterFactory;
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final AccountAndContainerInjector injector;
  private final IdSigningService idSigningService;
  private FrontendConfig frontendConfig;
  private NamedBlobPutHandler namedBlobPutHandler;
  private final String request_path;
  private final String dataset_version_request_path;

  public NamedBlobPutHandlerTest() throws Exception {
    idConverterFactory = new FrontendTestIdConverterFactory();
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    Properties props = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    router = new InMemoryRouter(verifiableProperties, CLUSTER_MAP);
    FrontendConfig frontendConfig = new FrontendConfig(verifiableProperties);
    metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    idSigningService = new AmbryIdSigningService();
    request_path =
        NAMED_BLOB_PREFIX + SLASH + REF_ACCOUNT.getName() + SLASH + REF_CONTAINER.getName() + SLASH + BLOBNAME;
    dataset_version_request_path =
        NAMED_BLOB_PREFIX + SLASH + REF_ACCOUNT.getName() + SLASH + REF_CONTAINER.getName() + SLASH + DATASET_NAME
            + SLASH + VERSION;
    NamedBlobDbFactory namedBlobDbFactory =
        new TestNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    initNamedBlobPutHandler(props);
  }

  /**
   * Test put Named Blob.
   */
  @Test
  public void putNamedBlobTest() throws Exception {
    idConverterFactory.returnInputIfTranslationNull = true;
    putBlobAndVerify(null, TestUtils.TTL_SECS);
    putBlobAndVerify(null, Utils.Infinite_Time);
  }

  /**
   * Tests for different combinations of container and frontend config w.r.t TTL enforcement.
   * @throws Exception
   */
  @Test
  public void ttlRequiredEnforcementTest() throws Exception {
    idConverterFactory.returnInputIfTranslationNull = true;
    Properties properties = new Properties();

    // ttl required in container, config asks not to fail. If TTL does not conform, look for non compliance warning
    properties.setProperty(FrontendConfig.FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY, "false");
    initNamedBlobPutHandler(properties);
    // ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1, true);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired, true);
    // not ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, Utils.Infinite_Time, true);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1, true);

    // ttl required in container, config asks to fail. If TTL does not conform, look for failure
    properties.setProperty(FrontendConfig.FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY, "true");
    initNamedBlobPutHandler(properties);
    // ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1, true);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired, true);
    // not ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, Utils.Infinite_Time, false);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1, false);

    // ttl not required in container, any ttl works and no exceptions or warnings
    doTtlRequiredEnforcementTest(REF_CONTAINER, Utils.Infinite_Time, false);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1, true);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired, true);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1, false);
  }

  /**
   * Test flows related to the stitch dataset version operation.
   * @throws Exception
   */
  @Test
  public void stitchDatasetVersionTest() throws Exception {
    idConverterFactory.translation = CONVERTED_ID;
    String uploadSession = UUID.randomUUID().toString();
    long creationTimeMs = System.currentTimeMillis();
    String[] prefixToTest = new String[]{"/" + CLUSTER_NAME, ""};
    for (String prefix : prefixToTest) {

      // multiple chunks
      List<ChunkInfo> chunksToStitch = uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 45, 10, 200, 19, 0, 50);
      List<String> signedChunkIds = chunksToStitch.stream()
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, uploadSession))
          .collect(Collectors.toList());
      stitchDatasetVersionAndVerify(getStitchRequestBody(signedChunkIds), chunksToStitch);
    }
  }

  /**
   * Test flows related to the stitch named blob operation.
   * @throws Exception
   */
  @Test
  public void stitchNamedBlobTest() throws Exception {
    idConverterFactory.translation = CONVERTED_ID;
    long creationTimeMs = System.currentTimeMillis();
    time.setCurrentMilliseconds(creationTimeMs);
    //success case
    String uploadSession = UUID.randomUUID().toString();
    String[] prefixToTest = new String[]{"/" + CLUSTER_NAME, ""};
    for (String prefix : prefixToTest) {

      // multiple chunks
      List<ChunkInfo> chunksToStitch = uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 45, 10, 200, 19, 0, 50);
      List<String> signedChunkIds = chunksToStitch.stream()
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, uploadSession))
          .collect(Collectors.toList());
      stitchBlobAndVerify(getStitchRequestBody(signedChunkIds), chunksToStitch, null);

      // one chunk
      chunksToStitch = uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 45);
      signedChunkIds = chunksToStitch.stream()
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, uploadSession))
          .collect(Collectors.toList());
      stitchBlobAndVerify(getStitchRequestBody(signedChunkIds), chunksToStitch, null);

      // failure cases
      // invalid json input
      stitchBlobAndVerify("badjsonbadjson".getBytes(StandardCharsets.UTF_8), null,
          restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
      // no chunk ids in request
      stitchBlobAndVerify(getStitchRequestBody(Collections.emptyList()), null,
          restServiceExceptionChecker(RestServiceErrorCode.MissingArgs));
      stitchBlobAndVerify(new JSONObject().toString().getBytes(StandardCharsets.UTF_8), null,
          restServiceExceptionChecker(RestServiceErrorCode.MissingArgs));
      // differing session IDs
      signedChunkIds = uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 45, 22).stream()
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, UUID.randomUUID().toString()))
          .collect(Collectors.toList());
      stitchBlobAndVerify(getStitchRequestBody(signedChunkIds), null,
          restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
      // differing containers
      signedChunkIds = Stream.concat(uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 50, 50).stream(),
          uploadChunksViaRouter(creationTimeMs, REF_CONTAINER_WITH_TTL_REQUIRED, 50).stream())
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, uploadSession))
          .collect(Collectors.toList());
      stitchBlobAndVerify(getStitchRequestBody(signedChunkIds), null,
          restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
      // differing accounts
      Container altAccountContainer =
          ACCOUNT_SERVICE.createAndAddRandomAccount().getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
      signedChunkIds = Stream.concat(uploadChunksViaRouter(creationTimeMs, REF_CONTAINER, 50, 50).stream(),
          uploadChunksViaRouter(creationTimeMs, altAccountContainer, 50).stream())
          .map(chunkInfo -> prefix + getSignedId(chunkInfo, uploadSession))
          .collect(Collectors.toList());
      stitchBlobAndVerify(getStitchRequestBody(signedChunkIds), null,
          restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
      // invalid blob ID
      stitchBlobAndVerify(
          getStitchRequestBody(Collections.singletonList(getSignedId(new ChunkInfo("abcd", 200, -1), uploadSession))),
          null, restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
      // unsigned ID
      stitchBlobAndVerify(getStitchRequestBody(Collections.singletonList("/notASignedId")), null,
          restServiceExceptionChecker(RestServiceErrorCode.BadRequest));
    }
  }

  /**
   * @param errorCode the expected error code.
   * @return a {@link ThrowingConsumer} that will check that the {@link ExecutionException} was caused by a
   *         {@link RestServiceException} with the specified error code.
   */
  private static ThrowingConsumer<ExecutionException> restServiceExceptionChecker(RestServiceErrorCode errorCode) {
    return executionException -> assertEquals("Unexpected error code", errorCode,
        ((RestServiceException) executionException.getCause()).getErrorCode());
  }

  /**
   * Does the TTL required enforcement test by selecting the right verification methods based on container and frontend
   * config
   * @param container the {@link Container} to upload to
   * @param blobTtlSecs the TTL to set for the blob
   * @throws Exception
   */
  private void doTtlRequiredEnforcementTest(Container container, long blobTtlSecs, boolean hasNamedBlobVersion) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, blobTtlSecs, !container.isCacheable(), SERVICE_ID,
        CONTENT_TYPE, OWNER_ID, null, null, null);
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = getRestRequest(headers, request_path, content);

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, future::done);
    if (container.isTtlRequired() && (blobTtlSecs == Utils.Infinite_Time
        || blobTtlSecs > frontendConfig.maxAcceptableTtlSecsIfTtlRequired)) {
      if (frontendConfig.failIfTtlRequiredButNotProvided) {
        try {
          future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
          fail("Post should have failed");
        } catch (ExecutionException e) {
          RestServiceException rootCause = (RestServiceException) Utils.getRootCause(e);
          assertNotNull("Root cause should be a RestServiceException", rootCause);
          assertEquals("Incorrect RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, rootCause.getErrorCode());
        }
      } else {
        verifySuccessResponseOnTtlEnforcement(future, content, blobTtlSecs, restResponseChannel, true);
      }
    } else {
      verifySuccessResponseOnTtlEnforcement(future, content, blobTtlSecs, restResponseChannel, false);
    }

    if (hasNamedBlobVersion && request.getRestMethod() == RestMethod.PUT && RestUtils.getRequestPath(request)
        .matchesOperation(Operations.NAMED_BLOB)) {
      assertTrue(request.getArgs().containsKey(RestUtils.InternalKeys.NAMED_BLOB_VERSION));
    } else {
      assertFalse(request.getArgs().containsKey(RestUtils.InternalKeys.NAMED_BLOB_VERSION));
    }
  }

  /**
   * Verifies successful put when subject to enforcement of TTL
   * @param postFuture the {@link FutureResult} returned from the POST call
   * @param content the content being POSTed
   * @param blobTtlSecs the ttl of the blob (in secs)
   * @param restResponseChannel the {@link RestResponseChannel} over which the response is expected
   * @param expectWarning {@code true} if a warning header for non compliance is expected. {@code false} otherwise.
   * @throws Exception
   */
  private void verifySuccessResponseOnTtlEnforcement(FutureResult<Void> postFuture, byte[] content, long blobTtlSecs,
      RestResponseChannel restResponseChannel, boolean expectWarning) throws Exception {
    postFuture.get(TIMEOUT_SECS, TimeUnit.SECONDS);
    InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(idConverterFactory.lastInput);

    assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());
    assertEquals("Unexpected ttl stored", blobTtlSecs, blob.getBlobProperties().getTimeToLiveInSeconds());

    if (expectWarning) {
      assertNotNull("There should be a non compliance warning",
          restResponseChannel.getHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING));
    } else {
      assertNull("There should be no warning", restResponseChannel.getHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING));
    }
  }

  /**
   * Make a stitch call using {@link NamedBlobPutHandler} and verify the result of the operation.
   * @param requestBody the body of the stitch request to supply.
   * @param expectedStitchedChunks the expected chunks stitched together.
   * @throws Exception
   */
  private void stitchDatasetVersionAndVerify(byte[] requestBody, List<ChunkInfo> expectedStitchedChunks)
      throws Exception {
    for (long ttl : new long[]{TestUtils.TTL_SECS, Utils.Infinite_Time}) {
      JSONObject headers = new JSONObject();
      FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, ttl, !REF_CONTAINER.isCacheable(), SERVICE_ID,
          CONTENT_TYPE, OWNER_ID, null, null, null);
      headers.put(RestUtils.Headers.DATASET_VERSION_QUERY_ENABLED, true);
      headers.put(RestUtils.Headers.UPLOAD_NAMED_BLOB_MODE, "STITCH");
      RestRequest request = getRestRequest(headers, dataset_version_request_path, requestBody);
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      FutureResult<Void> future = new FutureResult<>();
      idConverterFactory.lastInput = null;
      idConverterFactory.lastBlobInfo = null;
      idConverterFactory.lastConvertedId = null;
      namedBlobPutHandler.handle(request, restResponseChannel, future::done);
      future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
      assertEquals("Unexpected location header", idConverterFactory.lastConvertedId,
          restResponseChannel.getHeader(RestUtils.Headers.LOCATION));
      InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(idConverterFactory.lastInput);
      assertEquals("List of chunks stitched does not match expected", expectedStitchedChunks, blob.getStitchedChunks());
      ByteArrayOutputStream expectedContent = new ByteArrayOutputStream();
      expectedStitchedChunks.stream()
          .map(chunkInfo -> router.getActiveBlobs().get(chunkInfo.getBlobId()).getBlob().array())
          .forEach(buf -> expectedContent.write(buf, 0, buf.length));
      assertEquals("Unexpected blob content stored", ByteBuffer.wrap(expectedContent.toByteArray()), blob.getBlob());
      //check actual size of stitched blob
      assertEquals("Unexpected blob size", Long.toString(getStitchedBlobSize(expectedStitchedChunks)),
          restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
      assertEquals("Unexpected TTL in named blob DB", -1,
          idConverterFactory.lastBlobInfo.getBlobProperties().getTimeToLiveInSeconds());
      assertEquals("Unexpected TTL in blob", -1, blob.getBlobProperties().getTimeToLiveInSeconds());
    }
  }

  /**
   * Make a stitch blob call using {@link PostBlobHandler} and verify the result of the operation.
   * @param requestBody the body of the stitch request to supply.
   * @param expectedStitchedChunks the expected chunks stitched together.
   * @param errorChecker if non-null, expect an exception to be thrown by the post flow and verify it using this
   *                     {@link ThrowingConsumer}.
   * @throws Exception
   */
  private void stitchBlobAndVerify(byte[] requestBody, List<ChunkInfo> expectedStitchedChunks,
      ThrowingConsumer<ExecutionException> errorChecker) throws Exception {
    // unlike stitch for blob IDs, one step permanent stitch named blob can be supported through the built-in update TTL
    // call
    for (long ttl : new long[]{TestUtils.TTL_SECS, Utils.Infinite_Time}) {
      JSONObject headers = new JSONObject();
      FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, ttl, !REF_CONTAINER.isCacheable(), SERVICE_ID,
          CONTENT_TYPE, OWNER_ID, null, null, "STITCH");
      RestRequest request = getRestRequest(headers, request_path, requestBody);
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      FutureResult<Void> future = new FutureResult<>();
      idConverterFactory.lastInput = null;
      idConverterFactory.lastBlobInfo = null;
      idConverterFactory.lastConvertedId = null;
      namedBlobPutHandler.handle(request, restResponseChannel, future::done);
      if (errorChecker == null) {
        future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
        assertEquals("Unexpected location header", idConverterFactory.lastConvertedId,
            restResponseChannel.getHeader(RestUtils.Headers.LOCATION));
        InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(idConverterFactory.lastInput);
        assertEquals("List of chunks stitched does not match expected", expectedStitchedChunks,
            blob.getStitchedChunks());
        ByteArrayOutputStream expectedContent = new ByteArrayOutputStream();
        expectedStitchedChunks.stream()
            .map(chunkInfo -> router.getActiveBlobs().get(chunkInfo.getBlobId()).getBlob().array())
            .forEach(buf -> expectedContent.write(buf, 0, buf.length));
        assertEquals("Unexpected blob content stored", ByteBuffer.wrap(expectedContent.toByteArray()), blob.getBlob());
        //check actual size of stitched blob
        assertEquals("Unexpected blob size", Long.toString(getStitchedBlobSize(expectedStitchedChunks)),
            restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
        assertEquals("Unexpected TTL in named blob DB", ttl,
            idConverterFactory.lastBlobInfo.getBlobProperties().getTimeToLiveInSeconds());
        assertEquals("Unexpected TTL in blob", ttl, blob.getBlobProperties().getTimeToLiveInSeconds());
      } else {
        TestUtils.assertException(ExecutionException.class, () -> future.get(TIMEOUT_SECS, TimeUnit.SECONDS),
            errorChecker);
      }
    }
  }

  /**
   * @param signedChunkIds the list of signed chunk IDs to include in the JSON body.
   * @return a valid JSON body for a stitch request, encoded as UTF-8.
   */
  private byte[] getStitchRequestBody(List<String> signedChunkIds) {
    return StitchRequestSerDe.toJson(signedChunkIds).toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Caculate the blob size for a stitched blob from the individual chunks.
   * @param stitchedChunks list of chunks stitched to form the blob.
   * @return size of the stitched blob
   */
  private long getStitchedBlobSize(List<ChunkInfo> stitchedChunks) {
    long blobSize = 0;
    for (ChunkInfo chunkInfo : stitchedChunks) {
      blobSize += chunkInfo.getChunkSizeInBytes();
    }
    return blobSize;
  }

  /**
   * Upload chunks using the router directly.
   * @param creationTimeMs the creation time to set for the chunks.
   * @param container the {@link Container} to create the chunks in.
   * @param chunkSizes the sizes for each chunk to upload.
   * @return a list of {@link ChunkInfo} objects that contains metadata about each chunk uploaded.
   */
  private List<ChunkInfo> uploadChunksViaRouter(long creationTimeMs, Container container, int... chunkSizes)
      throws Exception {
    long blobTtlSecs = TimeUnit.DAYS.toSeconds(1);
    List<ChunkInfo> chunks = new ArrayList<>();
    for (int chunkSize : chunkSizes) {
      byte[] content = TestUtils.getRandomBytes(chunkSize);
      BlobProperties blobProperties =
          new BlobProperties(-1, SERVICE_ID, OWNER_ID, CONTENT_TYPE, !container.isCacheable(), blobTtlSecs,
              creationTimeMs, container.getParentAccountId(), container.getId(), container.isEncrypted(), null, null,
              null, null);
      String blobId =
          router.putBlob(blobProperties, null, new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content)),
              new PutBlobOptionsBuilder().chunkUpload(true).build()).get(TIMEOUT_SECS, TimeUnit.SECONDS);

      chunks.add(new ChunkInfo(blobId, chunkSize, Utils.addSecondsToEpochTime(creationTimeMs, blobTtlSecs)));
    }
    return chunks;
  }

  /**
   * Generate a signed ID for a data chunk based on the provided arguments.
   * @param chunkInfo the {@link ChunkInfo} containing the blob ID and additional metadata.
   * @param uploadSession the session ID to include in the metadata.
   * @return the signed ID.
   */
  private String getSignedId(ChunkInfo chunkInfo, String uploadSession) {
    Map<String, String> metadata = new HashMap<>(3);
    metadata.put(RestUtils.Headers.BLOB_SIZE, Long.toString(chunkInfo.getChunkSizeInBytes()));
    metadata.put(RestUtils.Headers.SESSION, uploadSession);
    metadata.put(PostBlobHandler.EXPIRATION_TIME_MS_KEY, Long.toString(chunkInfo.getExpirationTimeInMs()));
    try {
      return "/" + idSigningService.getSignedId(chunkInfo.getBlobId(), metadata);
    } catch (RestServiceException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Make a stitch blob call using {@link PostBlobHandler} and verify the result of the operation.
   * @param errorChecker if non-null, expect an exception to be thrown by the post flow and verify it using this
   *                     {@link ThrowingConsumer}.
   * @param ttl the ttl used for the blob
   * @throws Exception
   */
  private void putBlobAndVerify(ThrowingConsumer<ExecutionException> errorChecker, long ttl) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, ttl, !REF_CONTAINER.isCacheable(), SERVICE_ID,
        CONTENT_TYPE, OWNER_ID, null, null, null);
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = getRestRequest(headers, request_path, content);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    idConverterFactory.lastInput = null;
    idConverterFactory.lastBlobInfo = null;
    idConverterFactory.lastConvertedId = null;
    namedBlobPutHandler.handle(request, restResponseChannel, future::done);
    if (errorChecker == null) {
      future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
      assertEquals("Unexpected location header", idConverterFactory.lastConvertedId,
          restResponseChannel.getHeader(RestUtils.Headers.LOCATION));
      InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(idConverterFactory.lastInput);
      assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());
      assertEquals("Unexpected TTL in blob", ttl, blob.getBlobProperties().getTimeToLiveInSeconds());
      assertEquals("Unexpected TTL in named blob DB", ttl,
          idConverterFactory.lastBlobInfo.getBlobProperties().getTimeToLiveInSeconds());
      assertEquals("Unexpected response status", restResponseChannel.getStatus(), ResponseStatus.Ok);
    } else {
      TestUtils.assertException(ExecutionException.class, () -> future.get(TIMEOUT_SECS, TimeUnit.SECONDS),
          errorChecker);
    }
  }

  /**
   * Initates a {@link NamedBlobPutHandler}
   * @param properties the properties to use to init the {@link NamedBlobPutHandler}
   */
  private void initNamedBlobPutHandler(Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    namedBlobPutHandler = new NamedBlobPutHandler(securityServiceFactory.getSecurityService(), namedBlobDb,
        idConverterFactory.getIdConverter(), idSigningService, router, injector, frontendConfig, metrics, CLUSTER_NAME,
        QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
  }

  /**
   * Method to easily create a PUT {@link RestRequest}. This will set {@link RestUtils.InternalKeys#REQUEST_PATH} to a
   * valid {@link RequestPath} object.
   * @param headers any associated headers as a {@link JSONObject}.
   * @param path the path for the request.
   * @param requestBody the body of the request.
   * @return A {@link RestRequest} object that defines the request required by the input.
   */
  private RestRequest getRestRequest(JSONObject headers, String path, byte[] requestBody)
      throws UnsupportedEncodingException, URISyntaxException, RestServiceException {
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(requestBody), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    return request;
  }
}
