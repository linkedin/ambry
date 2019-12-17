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
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
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
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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


/**
 * Tests for {@link PostBlobHandler}. PostBlob functionality was recently extracted into this handler class from
 * {@link FrontendRestRequestService}, so the test cases are still in {@link FrontendRestRequestServiceTest}.
 *
 * @todo extract post related test cases from FrontendRestRequestServiceTest
 */
public class PostBlobHandlerTest {
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

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    Account account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    Container publicContainer = account.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    Container ttlRequiredContainer = new ContainerBuilder(publicContainer).setTtlRequired(true).build();
    Account newAccount = new AccountBuilder(account).addOrUpdateContainer(ttlRequiredContainer).build();
    if (!ACCOUNT_SERVICE.updateAccounts(Collections.singleton(newAccount))) {
      throw new IllegalStateException("Account could not be created");
    }
    REF_ACCOUNT = ACCOUNT_SERVICE.getAccountById(newAccount.getId());
    REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
    REF_CONTAINER_WITH_TTL_REQUIRED = REF_ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
  }

  private final MockTime time = new MockTime();
  private final FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
  private final InMemoryRouter router;
  private final FrontendTestIdConverterFactory idConverterFactory;
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final AccountAndContainerInjector injector;
  private final IdSigningService idSigningService;
  private FrontendConfig frontendConfig;
  private PostBlobHandler postBlobHandler;

  public PostBlobHandlerTest() {
    idConverterFactory = new FrontendTestIdConverterFactory();
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    Properties props = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    router = new InMemoryRouter(verifiableProperties, CLUSTER_MAP);
    injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, new FrontendConfig(verifiableProperties));
    idSigningService = new AmbryIdSigningService();
    initPostBlobHandler(props);
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
    initPostBlobHandler(properties);
    // ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired);
    // not ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, Utils.Infinite_Time);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1);

    // ttl required in container, config asks to fail. If TTL does not conform, look for failure
    properties.setProperty(FrontendConfig.FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY, "true");
    initPostBlobHandler(properties);
    // ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired);
    // not ok
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, Utils.Infinite_Time);
    doTtlRequiredEnforcementTest(REF_CONTAINER_WITH_TTL_REQUIRED, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1);

    // ttl not required in container, any ttl works and no exceptions or warnings
    doTtlRequiredEnforcementTest(REF_CONTAINER, Utils.Infinite_Time);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired - 1);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired);
    doTtlRequiredEnforcementTest(REF_CONTAINER, frontendConfig.maxAcceptableTtlSecsIfTtlRequired + 1);
  }

  /**
   * Test flows related to chunk uploads (for stitched uploads)
   * @throws Exception
   */
  @Test
  public void chunkUploadTest() throws Exception {
    idConverterFactory.translation = CONVERTED_ID;
    // valid request arguments
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), 1025, frontendConfig.chunkUploadInitialChunkTtlSecs,
        null);
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), 1024, frontendConfig.chunkUploadInitialChunkTtlSecs,
        null);
    // blob exceeds max blob size
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), 1023, 7200,
        routerExceptionChecker(RouterErrorCode.BlobTooLarge));
    // no session header
    doChunkUploadTest(1024, true, null, 1025, 7200, restServiceExceptionChecker(RestServiceErrorCode.MissingArgs));
    // missing max blob size
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), null, 7200,
        restServiceExceptionChecker(RestServiceErrorCode.MissingArgs));
    // invalid TTL
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), 1025, Utils.Infinite_Time,
        restServiceExceptionChecker(RestServiceErrorCode.InvalidArgs));
    doChunkUploadTest(1024, true, UUID.randomUUID().toString(), 1025, frontendConfig.chunkUploadInitialChunkTtlSecs + 1,
        restServiceExceptionChecker(RestServiceErrorCode.InvalidArgs));
    // ensure that the chunk upload request requirements are not enforced for non chunk uploads.
    doChunkUploadTest(1024, false, null, null, Utils.Infinite_Time, null);
  }

  /**
   * Test flows related to the {@link Operations#STITCH} operation.
   * @throws Exception
   */
  @Test
  public void stitchedUploadTest() throws Exception {
    idConverterFactory.translation = CONVERTED_ID;
    String uploadSession = UUID.randomUUID().toString();
    long creationTimeMs = System.currentTimeMillis();
    time.setCurrentMilliseconds(creationTimeMs);

    String[] prefixToTest = new String[]{"/" + CLUSTER_NAME, ""};
    for (String prefix : prefixToTest) {
      // success cases
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

  // helpers
  // general

  /**
   * Initates a {@link PostBlobHandler}
   * @param properties the properties to use to init the {@link PostBlobHandler}
   */
  private void initPostBlobHandler(Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    postBlobHandler =
        new PostBlobHandler(securityServiceFactory.getSecurityService(), idConverterFactory.getIdConverter(),
            idSigningService, router, injector, time, frontendConfig, metrics, CLUSTER_NAME);
  }

  // ttlRequiredEnforcementTest() helpers

  /**
   * Does the TTL required enforcement test by selecting the right verification methods based on container and frontend
   * config
   * @param container the {@link Container} to upload to
   * @param blobTtlSecs the TTL to set for the blob
   * @throws Exception
   */
  private void doTtlRequiredEnforcementTest(Container container, long blobTtlSecs) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, blobTtlSecs, !container.isCacheable(), SERVICE_ID,
        CONTENT_TYPE, OWNER_ID, REF_ACCOUNT.getName(), container.getName());
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = getRestRequest(headers, "/", content);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    postBlobHandler.handle(request, restResponseChannel, future::done);
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

    String id = (String) restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    assertNotNull("There should be a blob ID returned", id);
    InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(id);
    assertNotNull("No blob with ID " + id, blob);
    assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());
    assertEquals("Unexpected ttl stored", blobTtlSecs, blob.getBlobProperties().getTimeToLiveInSeconds());

    if (expectWarning) {
      assertNotNull("There should be a non compliance warning",
          restResponseChannel.getHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING));
    } else {
      assertNull("There should be no warning", restResponseChannel.getHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING));
    }
  }

  // chunkUploadTest() helpers

  /**
   * Make a post request and verify behavior related to chunk uploads (for stitched blobs).
   * @param contentLength the size of the blob to upload.
   * @param chunkUpload {@code true} to send the "x-ambry-chunk-upload" header, or {@code false} to test full uploads.
   * @param uploadSession the value for the "x-ambry-chunk-upload-session" request header, or null to not set it.
   * @param maxUploadSize the value for the "x-ambry-max-upload-size" request header, or null to not set it.
   * @param blobTtlSecs the blob TTL to use.
   * @param errorChecker if non-null, expect an exception to be thrown by the post flow and verify it using this
   *                     {@link ThrowingConsumer}.
   * @throws Exception
   */
  private void doChunkUploadTest(int contentLength, boolean chunkUpload, String uploadSession, Integer maxUploadSize,
      long blobTtlSecs, ThrowingConsumer<ExecutionException> errorChecker) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, blobTtlSecs, !REF_CONTAINER.isCacheable(), SERVICE_ID,
        CONTENT_TYPE, OWNER_ID, REF_ACCOUNT.getName(), REF_CONTAINER.getName());
    if (chunkUpload) {
      headers.put(RestUtils.Headers.CHUNK_UPLOAD, true);
    }
    if (uploadSession != null) {
      headers.put(RestUtils.Headers.SESSION, uploadSession);
    }
    if (maxUploadSize != null) {
      headers.put(RestUtils.Headers.MAX_UPLOAD_SIZE, maxUploadSize);
    }
    byte[] content = TestUtils.getRandomBytes(contentLength);
    RestRequest request = getRestRequest(headers, "/", content);
    long creationTimeMs = System.currentTimeMillis();
    time.setCurrentMilliseconds(creationTimeMs);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    idConverterFactory.lastInput = null;
    postBlobHandler.handle(request, restResponseChannel, future::done);
    if (errorChecker == null) {
      future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
      assertEquals("Unexpected converted ID", CONVERTED_ID, restResponseChannel.getHeader(RestUtils.Headers.LOCATION));
      Object metadata = request.getArgs().get(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY);
      if (chunkUpload) {
        Map<String, String> expectedMetadata = new HashMap<>(3);
        expectedMetadata.put(RestUtils.Headers.BLOB_SIZE, Integer.toString(contentLength));
        expectedMetadata.put(RestUtils.Headers.SESSION, uploadSession);
        expectedMetadata.put(PostBlobHandler.EXPIRATION_TIME_MS_KEY,
            Long.toString(Utils.addSecondsToEpochTime(creationTimeMs, blobTtlSecs)));
        assertEquals("Unexpected signed ID metadata", expectedMetadata, metadata);
      } else {
        assertNull("Signed id metadata should not be set on non-chunk uploads", metadata);
      }
      InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(idConverterFactory.lastInput);
      assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());
      assertEquals("Unexpected ttl stored", blobTtlSecs, blob.getBlobProperties().getTimeToLiveInSeconds());
      //check that blob size matches the actual upload size
      assertEquals("Invalid blob size", Integer.toString(contentLength),
          restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    } else {
      TestUtils.assertException(ExecutionException.class, () -> future.get(TIMEOUT_SECS, TimeUnit.SECONDS),
          errorChecker);
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
   * @param errorCode the expected error code.
   * @return a {@link ThrowingConsumer} that will check that the {@link ExecutionException} was caused by a
   *         {@link RouterException} with the specified error code.
   */
  private static ThrowingConsumer<ExecutionException> routerExceptionChecker(RouterErrorCode errorCode) {
    return executionException -> assertEquals("Unexpected error code", errorCode,
        ((RouterException) executionException.getCause()).getErrorCode());
  }

  // stitchedUploadTest() helpers

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
              creationTimeMs, container.getParentAccountId(), container.getId(), container.isEncrypted(), null);
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
   * Make a stitch blob call using {@link PostBlobHandler} and verify the result of the operation.
   * @param requestBody the body of the stitch request to supply.
   * @param expectedStitchedChunks the expected chunks stitched together.
   * @param errorChecker if non-null, expect an exception to be thrown by the post flow and verify it using this
   *                     {@link ThrowingConsumer}.
   * @throws Exception
   */
  private void stitchBlobAndVerify(byte[] requestBody, List<ChunkInfo> expectedStitchedChunks,
      ThrowingConsumer<ExecutionException> errorChecker) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, !REF_CONTAINER.isCacheable(),
        SERVICE_ID, CONTENT_TYPE, OWNER_ID, REF_ACCOUNT.getName(), REF_CONTAINER.getName());
    RestRequest request = getRestRequest(headers, "/" + Operations.STITCH, requestBody);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    idConverterFactory.lastInput = null;
    postBlobHandler.handle(request, restResponseChannel, future::done);
    if (errorChecker == null) {
      future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
      assertEquals("Unexpected converted ID", CONVERTED_ID, restResponseChannel.getHeader(RestUtils.Headers.LOCATION));
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
    } else {
      TestUtils.assertException(ExecutionException.class, () -> future.get(TIMEOUT_SECS, TimeUnit.SECONDS),
          errorChecker);
    }
  }

  /**
   * Method to easily create a POST {@link RestRequest}. This will set {@link RestUtils.InternalKeys#REQUEST_PATH} to a
   * valid {@link RequestPath} object.
   * @param headers any associated headers as a {@link JSONObject}.
   * @param path the path for the request.
   * @param requestBody the body of the request.
   * @return A {@link RestRequest} object that defines the request required by the input.
   */
  private RestRequest getRestRequest(JSONObject headers, String path, byte[] requestBody)
      throws UnsupportedEncodingException, URISyntaxException, RestServiceException {
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(requestBody), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    return request;
  }
}
