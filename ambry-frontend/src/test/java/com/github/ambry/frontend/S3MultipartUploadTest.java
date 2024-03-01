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
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
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
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteBufferRSC;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.github.ambry.frontend.s3.S3Payload.*;


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
  private S3MultipartUploadHandler s3MultipartUploadHandler;
  private S3PostHandler s3PostHandler;

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
  public void testPutPermanentBlob() throws Exception {
    multiPartUploadTest(Utils.Infinite_Time);
  }

  @Test
  public void testPutTempBlob() throws Exception {
    multiPartUploadTest(3600);
  }

  public void multiPartUploadTest(long expirationTime) throws Exception {
    // 1. CreateMultipartUpload
    String accountName = account.getName();
    String containerName = container.getName();
    String blobName = "MyDirectory/MyKey";
    String uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploads";
    JSONObject headers = new JSONObject();
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, headers, null);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> postResult = new FutureResult<>();
    s3PostHandler.handle(request, restResponseChannel, postResult::done);
    ReadableStreamChannel readableStreamChannel = postResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    InitiateMultipartUploadResult initUploadResult =
        xmlMapper.readValue(byteBuffer.array(), InitiateMultipartUploadResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", "application/xml",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Mismatch on InitiateMultipartUploadResult.bucket", accountName, initUploadResult.getBucket());
    assertEquals("Mismatch on InitiateMultipartUploadResult.key", blobName, initUploadResult.getKey());
    assertNotNull("UploadId must be present", initUploadResult.getUploadId());

    // 2. Multipart Upload Parts
    // 2.1 part1
    headers = new JSONObject();
    String uploadId = initUploadResult.getUploadId();
    int size = 8024;
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId
        + "&partNumber=1";
    headers.put(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(RestUtils.Headers.CONTENT_LENGTH, size);
    headers.put(RestUtils.Headers.TTL, expirationTime);
    byte[] content1 = TestUtils.getRandomBytes(size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content1), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    s3PutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
    String etag1 = (String) restResponseChannel.getHeader(RestUtils.Headers.ETAG);
    String location1 = (String) restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    assertEquals("Mismatch on response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertNotNull("Etag must be present", etag1);
    assertNotNull("Location must be present", location1);

    // 2.2 part2
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName + "?uploadId=" + uploadId
        + "&partNumber=2";
    headers.put(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(RestUtils.Headers.CONTENT_LENGTH, size);
    headers.put(RestUtils.Headers.TTL, expirationTime);
    byte[] content2 = TestUtils.getRandomBytes(size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content2), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    putResult = new FutureResult<>();
    s3PutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
    String etag2 = (String) restResponseChannel.getHeader(RestUtils.Headers.ETAG);
    String location2 = (String) restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
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
    headers.put(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(RestUtils.Headers.CONTENT_LENGTH, size);
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    postResult = new FutureResult<>();
    s3PostHandler.handle(request, restResponseChannel, postResult::done);
    readableStreamChannel = postResult.get();
    byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    byte[] byteArray = byteBuffer.array();
    CompleteMultipartUploadResult completeMultipartUploadResult =
        xmlMapper.readValue(byteArray, CompleteMultipartUploadResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", "application/xml",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Mismatch on CompleteMultipartUploadResult.bucket", accountName,
        completeMultipartUploadResult.getBucket());
    assertEquals("Mismatch on CompleteMultipartUploadResult.key", blobName, completeMultipartUploadResult.getKey());
    uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName;
    assertEquals("Mismatch on CompleteMultipartUploadResult.location", uri,
        completeMultipartUploadResult.getLocation());
    assertNotNull("ETag must be present", completeMultipartUploadResult.geteTag());

    // 4. Verify blob name exists in named blob DB
    NamedBlobRecord namedBlobRecord = namedBlobDb.get(accountName, containerName, blobName).get();
    // Etag starts with leading "/", for example "/AAYQ_wMcAAoAAQAAAAAAAAAEo8Hyvs1LTbqsXwFykRZW4w".
    assertEquals("Mismatch in blob name to blob id mapping", completeMultipartUploadResult.geteTag(),
        "/" + namedBlobRecord.getBlobId());

    // 5. Verify that getting a s3 blob should work
    headers = new JSONObject();
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, uri, headers, null);
    RequestPath requestPath = RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, requestPath);
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
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, clusterMap);
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
    NamedBlobPutHandler namedBlobPutHandler =
        new NamedBlobPutHandler(securityService, namedBlobDb, idConverter, idSigningService, router, injector,
            frontendConfig, metrics, CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    QuotaManager quotaManager = QuotaTestUtils.createDummyQuotaManager();
    getBlobHandler =
        new GetBlobHandler(frontendConfig, router, securityService, idConverter, injector, metrics, clusterMap,
            quotaManager, ACCOUNT_SERVICE);
    s3MultipartUploadHandler = new S3MultipartUploadHandler(securityService, metrics, injector, frontendConfig,
            namedBlobDb, idConverter, router, quotaManager);
    s3PostHandler = new S3PostHandler(s3MultipartUploadHandler);
    s3PutHandler = new S3PutHandler(namedBlobPutHandler, s3MultipartUploadHandler, metrics);
  }
}
