/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3Constants;
import com.github.ambry.frontend.s3.S3DeleteHandler;
import com.github.ambry.frontend.s3.S3MultipartAbortUploadHandler;
import com.github.ambry.frontend.s3.S3MultipartUploadHandler;
import com.github.ambry.frontend.s3.S3PostHandler;
import com.github.ambry.frontend.s3.S3PutHandler;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ByteBufferRSC;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static org.junit.Assert.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;


public class S3MultipartUploadTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String S3_PREFIX = "/s3";
  private static final String SLASH = "/";
  private final Account account;
  private final Container container;
  private FrontendConfig frontendConfig;
  private S3PutHandler s3PutHandler;
  private NamedBlobDb namedBlobDb;
  private GetBlobHandler getBlobHandler;
  private DeleteBlobHandler deleteBlobHandler;
  private S3MultipartUploadHandler s3MultipartUploadHandler;
  private S3MultipartAbortUploadHandler s3MultipartAbortHandler;
  private S3PostHandler s3PostHandler;
  private S3DeleteHandler s3DeleteHandler;

  private final ObjectMapper xmlMapper;

  public S3MultipartUploadTest() throws Exception {
    account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    container = new ContainerBuilder().setName("container-a")
        .setId((short) 10)
        .setParentAccountId(account.getId())
        .setStatus(Container.ContainerStatus.ACTIVE)
        .setNamedBlobMode(Container.NamedBlobMode.OPTIONAL)
        .build();
    account.updateContainerMap(Collections.singletonList(container));
    xmlMapper = new XmlMapper();
    setup();
  }

  @Test
  public void multiPartUploadTest() throws Exception {
    // 1. CreateMultipartUpload
    String accountName = account.getName();
    String containerName = container.getName();
    String blobName = "MyDirectory/MyKey";
    String uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploads";
    JSONObject headers = new JSONObject();
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, headers, null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> postResult = new FutureResult<>();
    s3PostHandler.handle(request, restResponseChannel, postResult::done);
    ReadableStreamChannel readableStreamChannel = postResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    InitiateMultipartUploadResult initUploadResult =
        xmlMapper.readValue(byteBuffer.array(), InitiateMultipartUploadResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE,
        restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    assertEquals("Mismatch on InitiateMultipartUploadResult.bucket", containerName, initUploadResult.getBucket());
    assertEquals("Mismatch on InitiateMultipartUploadResult.key", blobName, initUploadResult.getKey());
    assertNotNull("UploadId must be present", initUploadResult.getUploadId());

    // 2. Multipart Upload Parts
    // 2.1 part1
    headers = new JSONObject();
    String uploadId = initUploadResult.getUploadId();
    int size = 8024;
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId
        + "&partNumber=1";
    headers.put(Headers.CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
    headers.put(Headers.CONTENT_LENGTH, size);
    byte[] content1 = TestUtils.getRandomBytes(size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content1), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    s3PutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
    String etag1 = (String) restResponseChannel.getHeader(Headers.ETAG);
    String location1 = (String) restResponseChannel.getHeader(Headers.LOCATION);
    assertEquals("Mismatch on response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertNotNull("Etag must be present", etag1);
    assertNotNull("Location must be present", location1);

    // 2.2 part2
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId
        + "&partNumber=2";
    headers.put(Headers.CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
    headers.put(Headers.CONTENT_LENGTH, size);
    byte[] content2 = TestUtils.getRandomBytes(size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content2), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    putResult = new FutureResult<>();
    s3PutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
    String etag2 = (String) restResponseChannel.getHeader(Headers.ETAG);
    String location2 = (String) restResponseChannel.getHeader(Headers.LOCATION);
    assertEquals("Mismatch on response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertNotNull("Etag must be present", etag2);
    assertNotNull("Location must be present", location2);

    // 3. CompleteMultipartUpload
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId;
    headers = new JSONObject();
    Part part1 = new Part("1", etag1);
    Part part2 = new Part("2", etag2);
    // intentionally add part1 and part2 in wrong order.
    // when do the stitch, we re-order the chunk based on the partNumber.
    Part parts[] = {part2, part1};
    CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload(parts);
    XmlMapper xmlMapper = new XmlMapper();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    xmlMapper.writeValue(byteArrayOutputStream, completeMultipartUpload);
    String completeMultipartStr = byteArrayOutputStream.toString();
    byte[] content = completeMultipartStr.getBytes(StandardCharsets.UTF_8);
    size = content.length;
    headers.put(Headers.CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
    headers.put(Headers.CONTENT_LENGTH, size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    postResult = new FutureResult<>();
    s3PostHandler.handle(request, restResponseChannel, postResult::done);
    // Verify Upsert header is set by default for S3 multipart uploads.
    assertTrue("Upsert header must be present", request.getArgs().containsKey(NAMED_UPSERT));
    readableStreamChannel = postResult.get();
    byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    byte[] byteArray = byteBuffer.array();
    CompleteMultipartUploadResult completeMultipartUploadResult =
        xmlMapper.readValue(byteArray, CompleteMultipartUploadResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    assertEquals("Mismatch on CompleteMultipartUploadResult.bucket", containerName,
        completeMultipartUploadResult.getBucket());
    assertEquals("Mismatch on CompleteMultipartUploadResult.key", blobName, completeMultipartUploadResult.getKey());
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName;
    assertEquals("Mismatch on CompleteMultipartUploadResult.location", uri,
        completeMultipartUploadResult.getLocation());
    assertNotNull("ETag must be present", completeMultipartUploadResult.geteTag());

    // 4. Verify blob name exists in named blob DB with correct TTL
    NamedBlobRecord namedBlobRecord = namedBlobDb.get(accountName, containerName, blobName).get();
    assertEquals("Mismatch in blob name to blob id mapping", completeMultipartUploadResult.geteTag(),
        namedBlobRecord.getBlobId());
    assertEquals("TTL wasn't set properly", Utils.Infinite_Time, namedBlobRecord.getExpirationTimeMs());

    // 5. Verify that getting a s3 blob should work
    headers = new JSONObject();
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, uri, headers, null);
    RequestPath requestPath = RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME);
    request.setArg(InternalKeys.REQUEST_PATH, requestPath);
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> getResult = new FutureResult<>();
    getBlobHandler.handle(requestPath, request, restResponseChannel, getResult::done);
    readableStreamChannel = getResult.get();
    assertEquals("Mismatch on response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    byte[] dataContent = new byte[content1.length + content2.length];
    ByteBuffer buffer = ByteBuffer.wrap(dataContent);
    buffer.put(content1);
    buffer.put(content2);
    assertArrayEquals("Mismatch in blob content", buffer.array(),
        ((ByteBufferRSC) readableStreamChannel).getBuffer().array());

    // 6. Verify AbortMultipart will return no content after completion
    headers = new JSONObject();
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId;
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.DELETE, uri, headers, null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> deleteResult = new FutureResult<>();
    s3DeleteHandler.handle(request, restResponseChannel, deleteResult::done);
    assertEquals("Mismatch on status", ResponseStatus.NoContent, restResponseChannel.getStatus());
  }

  @Test
  public void testDuplicatePartNumbers() throws Exception {
    Part part1 = new Part("1", "etag1");
    Part part2 = new Part("1", "etag2");
    Part[] parts = {part2, part1};
    String expectedMessage = String.format(S3Constants.ERR_DUPLICATE_PART_NUMBER, 1);
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  @Test
  public void testDuplicateEtags() throws Exception {
    Part part1 = new Part("1", "etag1");
    Part part2 = new Part("2", "etag1");
    Part[] parts = {part2, part1};
    String expectedMessage = String.format(S3Constants.ERR_DUPLICATE_ETAG, "etag1");
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  @Test
  public void testInvalidPartNumLessThanMin() throws Exception {
    Part part1 = new Part("0", "etag1");
    Part part2 = new Part("1", "etag2");
    Part[] parts = {part2, part1};
    String expectedMessage = String.format(S3Constants.ERR_INVALID_PART_NUMBER, 0, S3Constants.MIN_PART_NUM, S3Constants.MAX_PART_NUM);
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  @Test
  public void testPartNumberInvalidExceedsMax() throws Exception {
    int invalidPartNumber = S3Constants.MAX_PART_NUM + 1;
    Part part1 = new Part("2", "etag1");
    Part part2 = new Part(String.valueOf(invalidPartNumber), "etag2");
    Part[] parts = {part2, part1};
    String expectedMessage = String.format(S3Constants.ERR_INVALID_PART_NUMBER, invalidPartNumber, S3Constants.MIN_PART_NUM, S3Constants.MAX_PART_NUM);
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  @Test
  public void testExceedMaxParts() throws Exception {
    Part[] parts = new Part[S3Constants.MAX_LIST_SIZE + 1];
    for (int i = 1; i <= S3Constants.MAX_LIST_SIZE + 1; i++) {
      parts[i - 1] = new Part(String.valueOf(i), "eTag" + i);
    }
    String expectedMessage = S3Constants.ERR_PART_LIST_TOO_LONG;
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  @Test
  public void testEmptyPartList() throws Exception {
    Part[] parts = {};
    String expectedMessage = S3Constants.ERR_EMPTY_REQUEST_BODY;
    testMultipartUploadWithInvalidParts(parts, expectedMessage);
  }

  private void testMultipartUploadWithInvalidParts(Part[] parts, String expectedErrorMessage) throws Exception {
    String accountName = account.getName();
    String containerName = container.getName();
    String blobName = "MyDirectory/MyKey";
    String uploadId = "uploadId";
    String uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId;
    JSONObject headers = new JSONObject();

    CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload(parts);
    XmlMapper xmlMapper = new XmlMapper();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    xmlMapper.writeValue(byteArrayOutputStream, completeMultipartUpload);
    String completeMultipartStr = byteArrayOutputStream.toString();
    byte[] content = completeMultipartStr.getBytes(StandardCharsets.UTF_8);
    int size = content.length;

    headers.put(Headers.CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
    headers.put(Headers.CONTENT_LENGTH, size);

    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    s3PostHandler.handle(request, restResponseChannel, (r, e) -> {
      assertNotNull("Expected an exception, but none was thrown.", e);
      assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().contains(expectedErrorMessage));
    });
  }



  /**
   * Initiates a {@link S3PutHandler}
   */
  private void setup() throws Exception {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    ClusterMap clusterMap = new MockClusterMap();
    AccountAndContainerInjector injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    IdSigningService idSigningService = new AmbryIdSigningService();
    FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
    SecurityService securityService = securityServiceFactory.getSecurityService();
    NamedBlobDbFactory namedBlobDbFactory =
        new TestNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    IdConverter idConverter = ambryIdConverterFactory.getIdConverter();
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, clusterMap, ambryIdConverterFactory);
    NamedBlobPutHandler namedBlobPutHandler =
        new NamedBlobPutHandler(securityService, namedBlobDb, idConverter, idSigningService, router, injector,
            frontendConfig, metrics, CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    QuotaManager quotaManager = QuotaTestUtils.createDummyQuotaManager();
    getBlobHandler =
        new GetBlobHandler(frontendConfig, router, securityService, idConverter, injector, metrics, clusterMap,
            quotaManager, ACCOUNT_SERVICE);
    deleteBlobHandler =
        new DeleteBlobHandler(router, securityService, idConverter, injector, metrics, clusterMap, quotaManager, ACCOUNT_SERVICE);
    s3MultipartUploadHandler = new S3MultipartUploadHandler(securityService, metrics, injector, frontendConfig,
            namedBlobDb, idConverter, router, quotaManager);

    s3PostHandler = new S3PostHandler(s3MultipartUploadHandler);
    s3PutHandler = new S3PutHandler(namedBlobPutHandler, s3MultipartUploadHandler, metrics);
    s3DeleteHandler = new S3DeleteHandler(deleteBlobHandler, s3MultipartUploadHandler, metrics);
  }
}
