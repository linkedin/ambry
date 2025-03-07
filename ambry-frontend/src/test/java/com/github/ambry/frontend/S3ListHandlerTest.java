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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3ListHandler;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;
import static org.junit.Assert.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;


public class S3ListHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final String SERVICE_ID = "test-app";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String OWNER_ID = "tester";
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String NAMED_BLOB_PREFIX = "/named";
  private static final String S3_PREFIX = "/s3";
  private static final String SLASH = "/";
  private final Account account;
  private final Container container;
  private FrontendConfig frontendConfig;
  private NamedBlobPutHandler namedBlobPutHandler;
  private S3ListHandler s3ListHandler;
  private final ObjectMapper xmlMapper;

  public S3ListHandlerTest() throws Exception {
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
  public void listObjectsTest() throws Exception {

    // 1. Put a named blob
    String PREFIX = "directory-name";
    String KEY_NAME = PREFIX + SLASH + "key_name";
    String request_path =
        NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + KEY_NAME;
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
        SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, null, null);
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, request_path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 2. Get list of blobs by sending matching s3 request
    String s3_list_request_uri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + "?prefix=" + PREFIX + "&Marker=/"
            + "&max-keys=1" + "&encoding-type=url";
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(LIST_REQUEST, "true");
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 3. Verify results
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    ListBucketResult listBucketResult = xmlMapper.readValue(byteBuffer.array(), ListBucketResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    Contents contents = listBucketResult.getContents().get(0);
    assertEquals("Mismatch in key name", KEY_NAME, contents.getKey());
    assertEquals("Mismatch in key count", 1, listBucketResult.getKeyCount());
    assertEquals("Mismatch in prefix", PREFIX, listBucketResult.getPrefix());
    assertEquals("Mismatch in max key count", 1, listBucketResult.getMaxKeys());
    assertEquals("Mismatch in encoding type", "url", listBucketResult.getEncodingType());

    // 4. Put another named blob
    String KEY_NAME1 = PREFIX + SLASH + "key_name1";
    request_path = NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + KEY_NAME1;
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, request_path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 5. Get list of blobs with continuation-token
    s3_list_request_uri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + "?prefix=" + PREFIX + "&marker="
            + KEY_NAME + "&max-keys=1" + "&encoding-type=url";
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(LIST_REQUEST, "true");
    restResponseChannel = new MockRestResponseChannel();
    futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 5. Verify results
    readableStreamChannel = futureResult.get();
    byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    listBucketResult = xmlMapper.readValue(byteBuffer.array(), ListBucketResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    assertEquals("Mismatch in bucket name", container.getName(), listBucketResult.getName());
    assertEquals("Mismatch in key name", KEY_NAME, listBucketResult.getContents().get(0).getKey());
    assertEquals("Mismatch in key count", 1, listBucketResult.getKeyCount());
    assertEquals("Mismatch in next token", KEY_NAME, listBucketResult.getMarker());
    assertEquals("Mismatch in next token", KEY_NAME1, listBucketResult.getNextMarker());
    assertTrue("Mismatch in IsTruncated", listBucketResult.getIsTruncated());
  }

  @Test
  public void listObjectsV2Test() throws Exception {
    // 1. Put a named blob
    String PREFIX = "directory-name";
    String KEY_NAME = PREFIX + SLASH + "key_name";
    int BLOB_SIZE = 1024;
    String request_path =
        NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + KEY_NAME;
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
        SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, null, null);
    byte[] content = TestUtils.getRandomBytes(BLOB_SIZE);
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, request_path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 2. Put another named blob
    String KEY_NAME1 = PREFIX + SLASH + "key_name1";
    request_path = NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + KEY_NAME1;
    request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, request_path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 3. Get list of blobs by sending matching s3 list object v2 request
    String s3_list_request_uri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + "?list-type=2" + "&prefix="
            + "&continuation-token=/" + "&encoding-type=url";
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(LIST_REQUEST, "true");
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 3. Verify results
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    ListBucketResultV2 listBucketResultV2 = xmlMapper.readValue(byteBuffer.array(), ListBucketResultV2.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    assertEquals("Mismatch in bucket name", container.getName(), listBucketResultV2.getName());
    assertEquals("Mismatch in key name", KEY_NAME, listBucketResultV2.getContents().get(0).getKey());
    assertEquals("Mismatch in key name", KEY_NAME1, listBucketResultV2.getContents().get(1).getKey());
    assertEquals("Mismatch in key count", 2, listBucketResultV2.getKeyCount());
    assertEquals("Mismatch in encoding type", "url", listBucketResultV2.getEncodingType());
    assertEquals("Mismatch in size", BLOB_SIZE, listBucketResultV2.getContents().get(0).getSize());
    assertNull("No common prefixes should be present", listBucketResultV2.getCommonPrefixes());
    // Verify the modified timestamp is formatted correctly
    String lastModified = listBucketResultV2.getContents().get(0).getLastModified();
    assertNotEquals("Last modified should not be -1", "-1", lastModified);
    // Attempt to parse the string. This should throw DateTimeParseException if the format is incorrect.
    ZonedDateTime.parse(lastModified, S3ListHandler.TIMESTAMP_FORMATTER);

    // 4. Get list of blobs with continuation-token
    s3_list_request_uri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + "?list-type=2" + "&prefix="
            + "&continuation-token=" + KEY_NAME + "&max-keys=1" + "&encoding-type=url";
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(LIST_REQUEST, "true");
    restResponseChannel = new MockRestResponseChannel();
    futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 5. Verify results
    readableStreamChannel = futureResult.get();
    byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    listBucketResultV2 = xmlMapper.readValue(byteBuffer.array(), ListBucketResultV2.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    assertEquals("Mismatch in key name", KEY_NAME, listBucketResultV2.getContents().get(0).getKey());
    assertEquals("Mismatch in key count", 1, listBucketResultV2.getKeyCount());
    assertEquals("Mismatch in next token", KEY_NAME, listBucketResultV2.getContinuationToken());
    assertEquals("Mismatch in next token", KEY_NAME1, listBucketResultV2.getNextContinuationToken());
  }

  @Test
  public void testListObjectsWithoutPrefix() throws Exception {
    // 1. Put a named blob
    String PREFIX = "directory-name";
    String KEY_NAME = PREFIX + SLASH + "key_name";
    String request_path =
        NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + KEY_NAME;
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
        SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, null, null);
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, request_path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 2. Get list of blobs by sending matching s3 request
    String s3_list_request_uri = S3_PREFIX + SLASH + account.getName() + SLASH + container.getName();
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(LIST_REQUEST, "true");
    request.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 3. Verify results
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    ListBucketResult listBucketResult = xmlMapper.readValue(byteBuffer.array(), ListBucketResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE, restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    Contents contents = listBucketResult.getContents().get(0);
    assertEquals("Mismatch in key name", KEY_NAME, contents.getKey());
    assertEquals("Mismatch in key count", 1, listBucketResult.getKeyCount());
  }

  @Test
  public void listObjectsV2EmptyResultTest() throws Exception {
    // Use a prefix that does not match any blobs.
    String prefix = "nonexistent/";
    String listUri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + "/?list-type=2&max-keys=10&prefix="
            + prefix;
    RestRequest listRequest =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, listUri, new JSONObject(), null);
    listRequest.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest.setArg(LIST_REQUEST, "true");
    listRequest.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    RestResponseChannel listResponse = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest, listResponse, futureResult::done);

    ReadableStreamChannel channel = futureResult.get();
    ByteBuffer buffer = ((ByteBufferReadableStreamChannel) channel).getContent();
    ListBucketResultV2 result = xmlMapper.readValue(buffer.array(), ListBucketResultV2.class);

    // Expect zero entries and no NextContinuationToken.
    assertEquals("Expected zero entries", 0, result.getKeyCount());
    assertNull("Expected no next token", result.getNextContinuationToken());
  }

  @Test
  public void listObjectsV2DirectoryGroupingTest() throws Exception {
    // This test creates blobs in 5 directories:
    //   dir1/file1.txt, dir1/file2.txt,
    //   dir2/file1.txt, dir2/file2.txt,
    //   dir3/file1.txt, dir3/file2.txt,
    //   dir4/file1.txt, dir4/file2.txt,
    //   dir5/file1.txt, dir5/file2.txt
    // With grouping enabled (empty prefix and delimiter="/"), S3 ListObjectsV2 would group them into:
    //   dir1/, dir2/, dir3/, dir4/, dir5/
    // We set max-keys=2 so that the first aggregated page contains two directory entries,
    // and the NextContinuationToken is the blobName of the first unprocessed directory.

    int numDirectories = 5;
    int filesPerDirectory = 2;
    String baseDir = "dir";
    for (int i = 1; i <= numDirectories; i++) {
      for (int j = 1; j <= filesPerDirectory; j++) {
        String key = baseDir + i + SLASH + "file" + j + ".txt";
        String requestPath = NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + key;
        JSONObject headers = new JSONObject();
        FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
            SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, null, null);
        byte[] content = TestUtils.getRandomBytes(1024);
        RestRequest putRequest = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, requestPath, headers,
            new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
        putRequest.setArg(InternalKeys.REQUEST_PATH,
            RequestPath.parse(putRequest, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
        RestResponseChannel putResponse = new MockRestResponseChannel();
        FutureResult<Void> putResult = new FutureResult<>();
        namedBlobPutHandler.handle(putRequest, putResponse, putResult::done);
        putResult.get();
      }
    }

    // Issue a GET request that maps to an S3 ListObjectsV2 request.
    // Using max-keys=2 and delimiter="/" (with empty prefix) will trigger directory grouping via listRecursively.
    String s3ListRequestUri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + "/?list-type=2&max-keys=2&delimiter=/";
    RestRequest listRequest =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3ListRequestUri, new JSONObject(), null);
    listRequest.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest.setArg(LIST_REQUEST, "true");
    listRequest.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    RestResponseChannel listResponse = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest, listResponse, futureResult::done);

    // Deserialize XML response.
    ReadableStreamChannel channel = futureResult.get();
    ByteBuffer buffer = ((ByteBufferReadableStreamChannel) channel).getContent();
    ListBucketResultV2 resultPage1 = xmlMapper.readValue(buffer.array(), ListBucketResultV2.class);

    // Expect the first page to contain 2 directory entries: "dir1/" and "dir2/".
    Set<String> expectedDirsPage1 = new HashSet<>(Arrays.asList("dir1/", "dir2/"));
    Set<String> actualDirsPage1 = new HashSet<>();
    resultPage1.getCommonPrefixes().forEach(p -> actualDirsPage1.add(p.getPrefix()));
    assertEquals("First page directory grouping mismatch", expectedDirsPage1, actualDirsPage1);

    // NextContinuationToken should be "dir3/".
    assertNotNull("Expected NextContinuationToken", resultPage1.getNextContinuationToken());
    assertEquals("NextContinuationToken mismatch", "dir3/file1.txt", resultPage1.getNextContinuationToken());

    // Now, simulate fetching the second page by using the NextContinuationToken.
    String s3ListRequestUri2 = S3_PREFIX + SLASH + account.getName() + SLASH + container.getName()
        + "/?list-type=2&max-keys=2&delimiter=/&continuation-token=dir3/file1.txt";
    RestRequest listRequest2 =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3ListRequestUri2, new JSONObject(), null);
    listRequest2.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest2, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest2.setArg(LIST_REQUEST, "true");
    listRequest2.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    listResponse = new MockRestResponseChannel();
    futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest2, listResponse, futureResult::done);
    channel = futureResult.get();
    buffer = ((ByteBufferReadableStreamChannel) channel).getContent();
    ListBucketResultV2 resultPage2 = xmlMapper.readValue(buffer.array(), ListBucketResultV2.class);

    // Expect the second page to contain "dir3/" and "dir4/" and NextContinuationToken "dir5/".
    Set<String> expectedDirsPage2 = new HashSet<>(Arrays.asList("dir3/", "dir4/"));
    Set<String> actualDirsPage2 = new HashSet<>();
    resultPage2.getCommonPrefixes().forEach(p -> actualDirsPage2.add(p.getPrefix()));
    assertEquals("Second page directory grouping mismatch", expectedDirsPage2, actualDirsPage2);
    assertEquals("Second page token mismatch", "dir5/file1.txt", resultPage2.getNextContinuationToken());

    // Finally, fetch the third page using token "dir5/".
    String s3ListRequestUri3 = S3_PREFIX + SLASH + account.getName() + SLASH + container.getName()
        + "/?list-type=2&max-keys=2&delimiter=/&continuation-token=dir5/file1.txt";
    RestRequest listRequest3 =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3ListRequestUri3, new JSONObject(), null);
    listRequest3.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest3, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest3.setArg(LIST_REQUEST, "true");
    listRequest3.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    listResponse = new MockRestResponseChannel();
    futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest3, listResponse, futureResult::done);
    channel = futureResult.get();
    buffer = ((ByteBufferReadableStreamChannel) channel).getContent();
    ListBucketResultV2 resultPage3 = xmlMapper.readValue(buffer.array(), ListBucketResultV2.class);

    // Expect the third page to contain only "dir5/" and no NextContinuationToken.
    Set<String> expectedDirsPage3 = new HashSet<>(Collections.singletonList("dir5/"));
    Set<String> actualDirsPage3 = new HashSet<>();
    resultPage3.getCommonPrefixes().forEach(p -> actualDirsPage3.add(p.getPrefix()));
    assertEquals("Third page directory grouping mismatch", expectedDirsPage3, actualDirsPage3);
    assertNull("Expected no NextContinuationToken on third page", resultPage3.getNextContinuationToken());
  }

  @Test
  public void listObjectsV2MixedGroupingTest() throws Exception {
    // Insert blobs that yield both grouped and ungrouped entries.
    // Grouped keys (will be merged into directories):
    //   "group1/file1.txt" and "group1/file2.txt" → aggregated as "group1/"
    //   "group2/file1.txt" and "group2/file2.txt" → aggregated as "group2/"
    // Ungrouped keys (no '/' in key, so appear as-is):
    //   "ungrouped1.txt"
    //   "ungrouped2.txt"

    String[] keysToPut =
        {"group1/file1.txt", "group1/file2.txt", "group2/file1.txt", "group2/file2.txt", "ungrouped1.txt",
            "ungrouped2.txt"};

    for (String key : keysToPut) {
      String requestPath = NAMED_BLOB_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + key;
      JSONObject headers = new JSONObject();
      FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
          SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, null, null);
      byte[] content = TestUtils.getRandomBytes(1024);
      RestRequest putRequest = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, requestPath, headers,
          new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
      putRequest.setArg(InternalKeys.REQUEST_PATH,
          RequestPath.parse(putRequest, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
      RestResponseChannel putResponse = new MockRestResponseChannel();
      FutureResult<Void> putResult = new FutureResult<>();
      namedBlobPutHandler.handle(putRequest, putResponse, putResult::done);
      putResult.get();
    }

    // Issue an S3 GET request with grouping enabled.
    // Use list-type=2, delimiter="/", max-keys=3, and empty prefix.
    String listUri =
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + "/?list-type=2&max-keys=3&delimiter=/";
    RestRequest listRequest =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, listUri, new JSONObject(), null);
    listRequest.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest.setArg(LIST_REQUEST, "true");
    listRequest.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    RestResponseChannel listResponse = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest, listResponse, futureResult::done);

    // Deserialize XML response into ListBucketResultV2.
    ReadableStreamChannel channel = futureResult.get();
    ByteBuffer buffer = ((ByteBufferReadableStreamChannel) channel).getContent();
    ListBucketResultV2 resultPage1 = xmlMapper.readValue(buffer.array(), ListBucketResultV2.class);

    // With max-keys=3 and grouping enabled, the first page should contain:
    //   - Aggregated directory "group1/"
    //   - Aggregated directory "group2/"
    //   - Individual key "ungrouped1.txt"
    // and NextContinuationToken should be "ungrouped2.txt".
    Set<String> expectedEntriesPage1 = new HashSet<>(Arrays.asList("group1/", "group2/", "ungrouped1.txt"));
    Set<String> actualEntriesPage1 = new HashSet<>();
    if (resultPage1.getContents() != null) {
      resultPage1.getContents().forEach(c -> actualEntriesPage1.add(c.getKey()));
    }
    if (resultPage1.getCommonPrefixes() != null) {
      resultPage1.getCommonPrefixes().forEach(p -> actualEntriesPage1.add(p.getPrefix()));
    }

    assertEquals("Mismatch in number of directory entries", 2, resultPage1.getCommonPrefixes().size());
    assertEquals("Mismatch in number of key entries", 1, resultPage1.getContents().size());
    assertEquals("Mismatch in values of entries in page 1", expectedEntriesPage1, actualEntriesPage1);
    assertNotNull("Expected NextContinuationToken on page 1", resultPage1.getNextContinuationToken());
    assertEquals("Page 1 NextContinuationToken mismatch", "ungrouped2.txt", resultPage1.getNextContinuationToken());

    // Now, fetch the second page using the continuation token.
    String continuationToken = resultPage1.getNextContinuationToken();
    String listUri2 = S3_PREFIX + SLASH + account.getName() + SLASH + container.getName()
        + "/?list-type=2&max-keys=3&delimiter=/&continuation-token=" + continuationToken;
    RestRequest listRequest2 =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, listUri2, new JSONObject(), null);
    listRequest2.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(listRequest2, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    listRequest2.setArg(LIST_REQUEST, "true");
    listRequest2.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
    listResponse = new MockRestResponseChannel();
    futureResult = new FutureResult<>();
    s3ListHandler.handle(listRequest2, listResponse, futureResult::done);
    ReadableStreamChannel channel2 = futureResult.get();
    ByteBuffer buffer2 = ((ByteBufferReadableStreamChannel) channel2).getContent();
    ListBucketResultV2 resultPage2 = xmlMapper.readValue(buffer2.array(), ListBucketResultV2.class);

    // Expect the second page to contain the remaining entry: "ungrouped2.txt"
    Set<String> expectedEntriesPage2 = new HashSet<>(Collections.singletonList("ungrouped2.txt"));
    Set<String> actualEntriesPage2 = new HashSet<>();
    if (resultPage2.getContents() != null) {
      resultPage2.getContents().forEach(c -> actualEntriesPage2.add(c.getKey()));
    }
    assertNull("Directory entries must not be present", resultPage2.getCommonPrefixes());
    assertEquals("Mismatch in number of key entries on page 2", 1, resultPage2.getContents().size());
    assertEquals("Aggregated page 2 entries mismatch", expectedEntriesPage2, actualEntriesPage2);
    // There should be no NextContinuationToken.
    assertNull("Expected no NextContinuationToken on page 2", resultPage2.getNextContinuationToken());
  }

  /**
   * Initates a {@link NamedBlobPutHandler} and a {@link S3ListHandler}
   */
  private void setup() throws Exception {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    AccountAndContainerInjector injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    IdSigningService idSigningService = new AmbryIdSigningService();
    FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
    NamedBlobDbFactory namedBlobDbFactory =
        new TestNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    NamedBlobDb namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, new MockClusterMap(), ambryIdConverterFactory);
    namedBlobPutHandler = new NamedBlobPutHandler(securityServiceFactory.getSecurityService(), namedBlobDb,
        ambryIdConverterFactory.getIdConverter(), idSigningService, router, injector, frontendConfig, metrics,
        CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    NamedBlobListHandler namedBlobListHandler =
        new NamedBlobListHandler(securityServiceFactory.getSecurityService(), namedBlobDb, injector, metrics,
            frontendConfig);
    s3ListHandler = new S3ListHandler(namedBlobListHandler, metrics);
  }
}
