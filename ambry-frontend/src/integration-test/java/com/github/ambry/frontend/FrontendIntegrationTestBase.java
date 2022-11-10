/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 */
package com.github.ambry.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.handler.codec.http.multipart.MemoryFileUpload;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


public class FrontendIntegrationTestBase {
  static final int PLAINTEXT_SERVER_PORT = 1174;
  static final int SSL_SERVER_PORT = 1175;
  static final int MAX_MULTIPART_POST_SIZE_BYTES = 10 * 10 * 1024;
  static final String DATA_CENTER_NAME = "localDc";
  static final String HOST_NAME = "localhost";
  static final int PORT = 12345;
  static final String CLUSTER_NAME = "Cluster-name";
  static final int NAMED_BLOB_LIST_RESULT_MAX = 5;
  protected FrontendConfig frontendConfig;
  protected NettyClient nettyClient;
  protected QuotaConfig quotaConfig;

  public FrontendIntegrationTestBase(FrontendConfig frontendConfig, NettyClient nettyClient) {
    this.frontendConfig = frontendConfig;
    this.nettyClient = nettyClient;
    this.quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
  }

  /**
   * Method to easily create a request.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link HttpHeaders} object. Can be null.
   * @param content the content that accompanies the request. Can be null.
   * @return A {@link FullHttpRequest} object that defines the request required by the input.
   */
  FullHttpRequest buildRequest(HttpMethod httpMethod, String uri, HttpHeaders headers, ByteBuffer content) {
    ByteBuf contentBuf;
    if (content != null) {
      contentBuf = Unpooled.wrappedBuffer(content);
    } else {
      contentBuf = Unpooled.buffer(0);
    }
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, contentBuf);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    if ((HttpMethod.POST.equals(httpMethod) || HttpMethod.PUT.equals(httpMethod)) && !HttpUtil.isContentLengthSet(
        httpRequest)) {
      HttpUtil.setTransferEncodingChunked(httpRequest, true);
    }
    return httpRequest;
  }

  /**
   * Combines all the parts in {@code contents} into one {@link ByteBuffer}.
   * @param contents the content of the response.
   * @param expectedContentLength the length of the contents in bytes.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  ByteBuffer getContent(Queue<HttpObject> contents, long expectedContentLength) {
    ByteBuffer buffer = ByteBuffer.allocate((int) expectedContentLength);
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      buffer.put(content.content().nioBuffer());
      endMarkerFound = object instanceof LastHttpContent;
      content.release();
    }
    assertEquals("Content length did not match expected", expectedContentLength, buffer.position());
    assertTrue("End marker was not found", endMarkerFound);
    buffer.flip();
    return buffer;
  }

  // BeforeClass helpers

  /**
   * Discards all the content in {@code contents} and checks none of the chunks have actual content
   * @param contents the content to discard.
   * @param expectedDiscardCount the number of {@link HttpObject}s that are expected to discarded.
   */
  void assertNoContent(Queue<HttpObject> contents, int expectedDiscardCount) {
    assertEquals("Objects that will be discarded differ from expected", expectedDiscardCount, contents.size());
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      assertEquals("No content expected ", 0, content.content().readableBytes());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(object);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
  }

// postGetHeadUpdateDeleteTest() and multipartPostGetHeadUpdateDeleteTest() helpers

  /**
   * Utility to test blob POST, GET, HEAD and DELETE operations for a specified size
   * @param contentSize the size of the blob to be tested
   * @param toPostAccount the {@link Account} to use in post headers. Can be {@code null} if only using service ID.
   * @param toPostContainer the {@link Container} to use in post headers. Can be {@code null} if only using service ID.
   * @param serviceId the serviceId to use for the POST
   * @param isPrivate the isPrivate flag to pass as part of the POST
   * @param expectedAccountName the expected account name in some response.
   * @param expectedContainerName the expected container name in some responses.
   * @param multipartPost {@code true} if multipart POST is desired, {@code false} otherwise.
   * @throws Exception
   */
  void doPostGetHeadUpdateDeleteUndeleteTest(int contentSize, Account toPostAccount, Container toPostContainer,
      String serviceId, boolean isPrivate, String expectedAccountName, String expectedContainerName,
      boolean multipartPost) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentSize));
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    String accountNameInPost = toPostAccount != null ? toPostAccount.getName() : null;
    String containerNameInPost = toPostContainer != null ? toPostContainer.getName() : null;
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeadersForPut(headers, TTL_SECS, isPrivate, serviceId, contentType, ownerId, accountNameInPost,
        containerNameInPost);
    String blobId;
    byte[] usermetadata = null;
    if (multipartPost) {
      usermetadata = TestUtils.getRandomString(32).getBytes();
      blobId = multipartPostBlobAndVerify(headers, content, ByteBuffer.wrap(usermetadata));
    } else {
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
      if (toPostContainer != null && !toPostContainer.getUserMetadataKeysToNotPrefixInResponse().isEmpty()) {
        for (String key : toPostContainer.getUserMetadataKeysToNotPrefixInResponse()) {
          headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key, "value");
        }
      }
      blobId = postBlobAndVerify(headers, content, contentSize);
    }
    headers.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
    headers.add(RestUtils.Headers.LIFE_VERSION, "0");
    doVariousGetAndVerify(blobId, headers, isPrivate, content, contentSize, expectedAccountName, expectedContainerName,
        usermetadata, toPostContainer);
    updateBlobTtlAndVerify(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata,
        toPostContainer);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD, TTL update and DELETE after delete.
    verifyOperationsAfterDelete(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, content,
        usermetadata, false, toPostContainer);
    // Undelete it
    headers.add(RestUtils.Headers.LIFE_VERSION, "1");
    undeleteBlobAndVerify(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata,
        toPostContainer);
  }

  /**
   * Put operation to a container that disabled named blob, and this PUT operation should fail.
   * @param account The {@link Account}.
   * @param container The {@link Container} that disabled named blob.
   * @throws Exception
   */
  void doNamedBlobPutOnDisabledContainer(Account account, Container container) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(100));
    String contentType = "application/octet-stream";
    String ownerId = "namedBlobTest";
    String accountName = account.getName();
    String containerName = container.getName();
    String blobName = "zzzz" + TestUtils.getRandomString(10);
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeadersForPut(headers, TTL_SECS, false, account.getName(), contentType, ownerId, null, null);
    putNamedBlobAndFail(headers, content, accountName, containerName, blobName, HttpResponseStatus.BAD_REQUEST);
  }

  /**
   * Put a named blob and fail.
   * @param headers The http headers to send
   * @param content The content to send
   * @param accountName The account name.
   * @param containerName The container name.
   * @param blobName The blob name.
   * @param expectedStatus The expected status.
   * @throws Exception
   */
  private void putNamedBlobAndFail(HttpHeaders headers, ByteBuffer content, String accountName, String containerName,
      String blobName, HttpResponseStatus expectedStatus) throws Exception {
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.PUT, buildUriForNamedBlob(accountName, containerName, blobName), headers, content);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", expectedStatus, response.status());
    verifyTrackingHeaders(response);
    assertNoContent(responseParts.queue, 1);
  }

  /**
   * Utility to test named blob PUT, GET, HEAD and DELETE operations
   * @param account the {@link Account} to use in post headers.
   * @param container the {@link Container} to use in post headers.
   * @throws Exception
   */
  void doNamedBlobPutGetHeadDeleteTest(Account account, Container container) throws Exception {
    // Test second container, test get and delete
    String accountName = account.getName();
    String containerName = container.getName();
    String blobName = "zzzz" + TestUtils.getRandomString(10);
    HttpHeaders headers = new DefaultHttpHeaders();
    String contentType = "application/octet-stream";
    String ownerId = "namedBlobTest";
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    setAmbryHeadersForPut(headers, TTL_SECS, false, accountName, contentType, ownerId, null, null);
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(100));
    String blobId = putNamedBlobAndVerify(headers, content, 100, accountName, containerName, blobName);

    headers.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
    headers.add(RestUtils.Headers.LIFE_VERSION, "0");
    headers.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    headers.add(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
    // This is the blob id for the given blob name, we should be able to do all get operations on this blob id.
    doVariousGetAndVerify(blobId, headers, false, content, 100, accountName, containerName, null, container);

    String fakeBlobId = buildUriForNamedBlob(accountName, containerName, blobName);
    // check GET, HEAD, DELETE
    doVariousGetAndVerify(fakeBlobId, headers, false, content, 100, accountName, containerName, null, container);
    deleteBlobAndVerify(fakeBlobId);

    // check GET after DELETE
    verifyOperationsAfterDelete(fakeBlobId, headers, false, accountName, containerName, content, null, true, container);
    verifyOperationsAfterDelete(blobId, headers, false, accountName, containerName, content, null, true, container);
  }

  /**
   * Utility to test named blob LIST  operations.
   * @param account the {@link Account} to use in post headers.
   * @param container the {@link Container} to use in post headers.
   * @throws Exception
   */
  void doNamedBlobPutListDeleteTest(Account account, Container container) throws Exception {
    int listMaxResults = NAMED_BLOB_LIST_RESULT_MAX;
    // upload three batches of blobs,
    // listMaxResults-1 blobs that start with aaaa
    // listMaxResults * 2 + listMaxResults-2 blobs that start with bbbb
    // listMaxResults blobs that start with cccc
    List<String> allBlobNames = new ArrayList<>();
    String accountName = account.getName();
    String containerName = container.getName();
    String contentType = "application/octet-stream";
    String ownerId = "namedBlobTest";
    for (Pair<String, Integer> pair : Arrays.asList(new Pair<>("aaaa", listMaxResults - 1),
        new Pair<>("bbbb", 3 * listMaxResults - 2), new Pair<>("cccc", listMaxResults))) {
      for (int i = 0; i < pair.getSecond(); i++) {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
        headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
        setAmbryHeadersForPut(headers, TTL_SECS, false, accountName, contentType, ownerId, null, null);
        ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(100));
        String blobName = pair.getFirst() + String.format("%02d", i);
        putNamedBlobAndVerify(headers, content, 100, accountName, containerName, blobName);
        allBlobNames.add(blobName);
      }
    }

    // Check the LIST operation
    verifyListNamedBlobs(accountName, containerName, ownerId, "aaaa", listMaxResults - 1);
    verifyListNamedBlobs(accountName, containerName, ownerId, "bbbb", 3 * listMaxResults - 2);
    verifyListNamedBlobs(accountName, containerName, ownerId, "cccc", listMaxResults);

    // DELETE all the named blobs
    for (String blobName : allBlobNames) {
      String blobId = buildUriForNamedBlob(accountName, containerName, blobName);
      deleteBlobAndVerify(blobId);
    }
  }

  /**
   * Put a named blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @param contentSize the length of the given content.
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name.
   * @return blob id returned by the server.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  String putNamedBlobAndVerify(HttpHeaders headers, ByteBuffer content, long contentSize, String accountName,
      String containerName, String blobName) throws Exception {
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.PUT, buildUriForNamedBlob(accountName, containerName, blobName), headers, content);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertNotNull("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null));
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);
    assertNotNull("Blob ID from POST should not be null", blobId);
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    assertEquals("Correct blob size should be returned in response", Long.toString(contentSize),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    verifyTrackingHeaders(response);
    verifyPostRequestCostHeaders(response, contentSize);
    return blobId;
  }

  /**
   * Verify Named blob List operation.
   * @param accountName The account name.
   * @param containerName The container name
   * @param ownerId The owner id
   * @param prefix The prefix of blob name.
   * @param numberOfBlobs The expected number of named blobs returned by list operation.
   * @throws Exception
   */
  void verifyListNamedBlobs(String accountName, String containerName, String ownerId, String prefix, int numberOfBlobs)
      throws Exception {
    String nextPageToken = null;
    boolean isFirst = true;
    while (numberOfBlobs > 0) {
      int expectedReturnNumber =
          numberOfBlobs > NAMED_BLOB_LIST_RESULT_MAX ? NAMED_BLOB_LIST_RESULT_MAX : numberOfBlobs;
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.add(RestUtils.Headers.SERVICE_ID, accountName);
      headers.add(RestUtils.Headers.OWNER_ID, ownerId);
      headers.add("prefix", prefix);
      if (!isFirst) {
        assertNotNull("nextPageToken should not be null", nextPageToken);
        headers.add("page", nextPageToken);
      }
      FullHttpRequest httpRequest =
          buildRequest(HttpMethod.GET, buildUriForNamedBlobList(accountName, containerName), headers, null);
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
      assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
      assertEquals("Content-Type does not match", "application/json",
          response.headers().get(HttpHeaderNames.CONTENT_TYPE));
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
      int contentLength = response.headers().getInt(HttpHeaderNames.CONTENT_LENGTH);
      byte[] responseContentArray = getContent(responseParts.queue, contentLength).array();
      ObjectMapper objectMapper = new ObjectMapper();
      ListNamedBlobResponse listResponse = objectMapper.readValue(responseContentArray, ListNamedBlobResponse.class);
      assertEquals("Named blob entry number don't match", expectedReturnNumber, listResponse.entries.size());
      nextPageToken = listResponse.getNextPageToken();
      isFirst = false;
      numberOfBlobs -= NAMED_BLOB_LIST_RESULT_MAX;
    }
  }

  /**
   * The http request uri for named blob.
   * @param accountName The account name.
   * @param containerName The container name.
   * @param blobName the blob name.
   * @return The http request uri.
   */
  String buildUriForNamedBlob(String accountName, String containerName, String blobName) {
    return String.format("/named/%s/%s/%s", accountName, containerName, blobName);
  }

  /**
   * The http request uri for list named blob.
   * @param accountName The account name.
   * @param containerName The container name.
   * @return The http request uri.
   */
  String buildUriForNamedBlobList(String accountName, String containerName) {
    return String.format("/named/%s/%s", accountName, containerName);
  }

  /**
   * Do various GET operations and verify the responses. It includes GET content with different byte range and get options.
   * It includes HEAD operations. It includes GET usermetadata and GET blob info.
   * @param blobId The blob id.
   * @param headers The http headers to send
   * @param isPrivate {@code true} is the container is private.
   * @param content The expected content
   * @param contentSize The length of content.
   * @param expectedAccountName The expected account name.
   * @param expectedContainerName The expected container named.
   * @param usermetadata The expected usermetadata.
   * @throws Exception
   */
  void doVariousGetAndVerify(String blobId, HttpHeaders headers, boolean isPrivate, ByteBuffer content,
      long contentSize, String expectedAccountName, String expectedContainerName, byte[] usermetadata,
      Container container) throws Exception {
    getBlobAndVerify(blobId, null, null, false, headers, isPrivate, content, expectedAccountName, expectedContainerName,
        container);
    getHeadAndVerify(blobId, null, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    getBlobAndVerify(blobId, null, GetOption.None, false, headers, isPrivate, content, expectedAccountName,
        expectedContainerName, container);
    getHeadAndVerify(blobId, null, GetOption.None, headers, isPrivate, expectedAccountName, expectedContainerName);
    ByteRange range = ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(content.capacity() + 1));
    headers.add(RestUtils.Headers.BLOB_SIZE, range.getRangeSize());
    getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
        expectedContainerName, container);
    getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    if (contentSize > 0) {
      range = ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(content.capacity()));
      getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
          expectedContainerName, container);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
      long random1 = ThreadLocalRandom.current().nextLong(content.capacity());
      long random2 = ThreadLocalRandom.current().nextLong(content.capacity());
      range = ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
      getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
          expectedContainerName, container);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    }
    getNotModifiedBlobAndVerify(blobId, null, isPrivate, container);
    getUserMetadataAndVerify(blobId, null, headers, usermetadata, container);
    getBlobInfoAndVerify(blobId, null, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata,
        container);
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param httpHeaders the {@link HttpHeaders} where the headers should be set.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @param targetAccountName sets the {@link RestUtils.Headers#TARGET_ACCOUNT_NAME} header. Can be {@code null}.
   * @param targetContainerName sets the {@link RestUtils.Headers#TARGET_CONTAINER_NAME} header. Can be {@code null}.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   */
  void setAmbryHeadersForPut(HttpHeaders httpHeaders, Long ttlInSecs, boolean isPrivate, String serviceId,
      String contentType, String ownerId, String targetAccountName, String targetContainerName) {
    if (httpHeaders != null && serviceId != null && contentType != null) {
      if (ttlInSecs != null && ttlInSecs > -1) {
        httpHeaders.add(RestUtils.Headers.TTL, ttlInSecs);
      }
      httpHeaders.add(RestUtils.Headers.SERVICE_ID, serviceId);
      httpHeaders.add(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
      httpHeaders.add(RestUtils.Headers.NAMED_UPSERT, true);
      if (targetAccountName != null) {
        httpHeaders.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, targetAccountName);
      }
      if (targetContainerName != null) {
        httpHeaders.add(RestUtils.Headers.TARGET_CONTAINER_NAME, targetContainerName);
      } else {
        httpHeaders.add(RestUtils.Headers.PRIVATE, isPrivate);
      }
      if (ownerId != null) {
        httpHeaders.add(RestUtils.Headers.OWNER_ID, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  String postBlobAndVerify(HttpHeaders headers, ByteBuffer content, long contentSize)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, "/", headers, content);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    return verifyPostAndReturnBlobId(responseParts, contentSize, false);
  }

  /**
   * Verifies a POST and returns the blob ID.
   * @param responseParts the response received from the server.
   * @param contentSize the size of the blob
   * @param isStitch True is the POST request is a stitch operation.
   * @returnn the blob ID of the blob.
   */
  String verifyPostAndReturnBlobId(NettyClient.ResponseParts responseParts, long contentSize, boolean isStitch) {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertNotNull("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null));
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);
    assertNotNull("Blob ID from POST should not be null", blobId);
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    assertEquals("Correct blob size should be returned in response", Long.toString(contentSize),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    verifyTrackingHeaders(response);
    verifyPostRequestCostHeaders(response, isStitch ? 0 : contentSize);
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param range the {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param resolveRangeOnEmptyBlob {@code true} to send the {@link RestUtils.Headers#RESOLVE_RANGE_ON_EMPTY_BLOB}
   *                                header.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param expectedContent the expected content of the blob.
   * @param accountName the account name that should be in the response
   * @param containerName the container name that should be in the response
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void getBlobAndVerify(String blobId, ByteRange range, GetOption getOption, boolean resolveRangeOnEmptyBlob,
      HttpHeaders expectedHeaders, boolean isPrivate, ByteBuffer expectedContent, String accountName,
      String containerName, Container container) throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (range != null) {
      headers.add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    if (resolveRangeOnEmptyBlob) {
      headers.add(RestUtils.Headers.RESOLVE_RANGE_ON_EMPTY_BLOB, true);
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    verifyGetBlobResponse(responseParts, range, resolveRangeOnEmptyBlob, expectedHeaders, isPrivate, expectedContent,
        accountName, containerName, container);
  }

  /**
   * Verifies the GET blob response.
   * @param responseParts the response received from the server.
   * @param range the {@link ByteRange} for the request.
   * @param resolveRangeOnEmptyBlob {@code true} if the {@link RestUtils.Headers#RESOLVE_RANGE_ON_EMPTY_BLOB} header was
   *                                sent.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param expectedContent the expected content of the blob.
   * @param accountName the account name that should be in the response
   * @param containerName the container name that should be in the response
   * @throws RestServiceException
   */
  void verifyGetBlobResponse(NettyClient.ResponseParts responseParts, ByteRange range, boolean resolveRangeOnEmptyBlob,
      HttpHeaders expectedHeaders, boolean isPrivate, ByteBuffer expectedContent, String accountName,
      String containerName, Container container) throws RestServiceException {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertEquals("Accept-Ranges not set correctly", "bytes", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertEquals(RestUtils.Headers.LIFE_VERSION + " does not match",
        expectedHeaders.get(RestUtils.Headers.LIFE_VERSION), response.headers().get(RestUtils.Headers.LIFE_VERSION));
    byte[] expectedContentArray = expectedContent.array();
    if (range != null) {
      long blobSize = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
      assertEquals("Content-Range header not set correctly",
          RestUtils.buildContentRangeAndLength(range, blobSize, resolveRangeOnEmptyBlob).getFirst(),
          response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      ByteRange resolvedRange = range.toResolvedByteRange(blobSize, resolveRangeOnEmptyBlob);
      expectedContentArray = Arrays.copyOfRange(expectedContentArray, (int) resolvedRange.getStartOffset(),
          (int) resolvedRange.getEndOffset() + 1);
    } else {
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    }
    if (expectedContentArray.length < frontendConfig.chunkedGetResponseThresholdInBytes) {
      assertEquals("Content-length not as expected", expectedContentArray.length, HttpUtil.getContentLength(response));
    }
    verifyCacheHeaders(isPrivate, response, frontendConfig.cacheValiditySeconds,
        container != null ? container.getCacheTtlInSecond() : null);
    byte[] responseContentArray = getContent(responseParts.queue, expectedContentArray.length).array();
    assertArrayEquals("GET content does not match original content", expectedContentArray, responseContentArray);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    verifyTrackingHeaders(response);
    verifyBlobProperties(expectedHeaders, isPrivate, response);
    verifyAccountAndContainerHeaders(accountName, containerName, response);
    verifyUserMetadata(expectedHeaders, response, null, null, container);
    verifyGetRequestCostHeaders(response, expectedContentArray.length);
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @param getOption the options to use while getting the blob.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @throws Exception
   */
  void getNotModifiedBlobAndVerify(String blobId, GetOption getOption, boolean isPrivate, Container container)
      throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    headers.add(RestUtils.Headers.IF_MODIFIED_SINCE, new Date());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    verifyGetNotModifiedBlobResponse(response, isPrivate, responseParts, container);
  }

  void verifyGetNotModifiedBlobResponse(HttpResponse response, boolean isPrivate,
      NettyClient.ResponseParts responseParts, Container container) {
    assertEquals("Unexpected response status", HttpResponseStatus.NOT_MODIFIED, response.status());
    assertNotNull("Date header should be set", response.headers().get(RestUtils.Headers.DATE));
    assertNotNull("Last-Modified header should be set", response.headers().get("Last-Modified"));
    assertNull("Content-Length should not be set", response.headers().get(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Accept-Ranges should not be set", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    assertNull("Life-Version header should not be set", response.headers().get(RestUtils.Headers.LIFE_VERSION));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", response.headers().get(RestUtils.Headers.CONTENT_TYPE));
    verifyTrackingHeaders(response);
    verifyCacheHeaders(isPrivate, response, frontendConfig.cacheValiditySeconds,
        container != null ? container.getCacheTtlInSecond() : null);
    assertNoContent(responseParts.queue, 1);
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void getUserMetadataAndVerify(String blobId, GetOption getOption, HttpHeaders expectedHeaders, byte[] usermetadata,
      Container container) throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    verifyUserMetadataResponse(response, expectedHeaders, usermetadata, responseParts, container);
  }

  void verifyUserMetadataResponse(HttpResponse response, HttpHeaders expectedHeaders, byte[] usermetadata,
      NettyClient.ResponseParts responseParts, Container container) {
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    verifyTrackingHeaders(response);
    checkCommonGetHeadHeaders(response.headers());
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts.queue, container);
    if (usermetadata == null) {
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      assertNoContent(responseParts.queue, 1);
    }
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void getBlobInfoAndVerify(String blobId, GetOption getOption, HttpHeaders expectedHeaders, boolean isPrivate,
      String accountName, String containerName, byte[] usermetadata, Container container)
      throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    verifyGetBlobInfoResponse(response, expectedHeaders, isPrivate, accountName, containerName, usermetadata,
        responseParts, container);
  }

  void verifyGetBlobInfoResponse(HttpResponse response, HttpHeaders expectedHeaders, boolean isPrivate,
      String accountName, String containerName, byte[] usermetadata, NettyClient.ResponseParts responseParts,
      Container container) {
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    checkCommonGetHeadHeaders(response.headers());
    verifyTrackingHeaders(response);
    verifyBlobProperties(expectedHeaders, isPrivate, response);
    verifyAccountAndContainerHeaders(accountName, containerName, response);
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts.queue, container);
    if (usermetadata == null) {
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      assertNoContent(responseParts.queue, 1);
    }
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    assertEquals(RestUtils.Headers.LIFE_VERSION + " does not match",
        expectedHeaders.get(RestUtils.Headers.LIFE_VERSION), response.headers().get(RestUtils.Headers.LIFE_VERSION));
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param range the {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in the response.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void getHeadAndVerify(String blobId, ByteRange range, GetOption getOption, HttpHeaders expectedHeaders,
      boolean isPrivate, String accountName, String containerName)
      throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (range != null) {
      headers.add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    verifyGetHeadResponse(response, expectedHeaders, range, isPrivate, accountName, containerName, responseParts);
  }

  void verifyGetHeadResponse(HttpResponse response, HttpHeaders expectedHeaders, ByteRange range, boolean isPrivate,
      String accountName, String containerName, NettyClient.ResponseParts responseParts) throws RestServiceException {
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    long contentLength = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength, false);
      assertEquals("Content-Range header not set correctly", rangeAndLength.getFirst(),
          response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      contentLength = rangeAndLength.getSecond();
    } else {
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    }
    assertEquals("Accept-Ranges not set correctly", "bytes", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertEquals(RestUtils.Headers.CONTENT_LENGTH + " does not match expected", contentLength,
        HttpUtil.getContentLength(response));
    assertEquals(RestUtils.Headers.CONTENT_TYPE + " does not match " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(RestUtils.Headers.LIFE_VERSION + " does not match",
        expectedHeaders.get(RestUtils.Headers.LIFE_VERSION), response.headers().get(RestUtils.Headers.LIFE_VERSION));
    verifyBlobProperties(expectedHeaders, isPrivate, response);
    verifyAccountAndContainerHeaders(accountName, containerName, response);
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    verifyTrackingHeaders(response);
    verifyHeadRequestCostHeaders(response);
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param response the {@link HttpResponse} that contains the headers.
   */
  void verifyBlobProperties(HttpHeaders expectedHeaders, boolean isPrivate, HttpResponse response) {
    assertEquals("Blob size does not match", Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)),
        Long.parseLong(response.headers().get(RestUtils.Headers.BLOB_SIZE)));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        response.headers().get(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", isPrivate,
        Boolean.valueOf(response.headers().get(RestUtils.Headers.PRIVATE)));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertNotNull("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null));
    if (expectedHeaders.get(RestUtils.Headers.TTL) != null
        && Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          response.headers().get(RestUtils.Headers.TTL));
    } else {
      assertFalse("There should be no TTL in the response", response.headers().contains(RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          response.headers().get(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verifies the account and container headers in the response
   * @param accountName the expected account name in {@code response}.
   * @param containerName the expected container name in {@code response}.
   * @param response the response received from Ambry.
   */
  void verifyAccountAndContainerHeaders(String accountName, String containerName, HttpResponse response) {
    String accountNameInResponse = response.headers().get(RestUtils.Headers.TARGET_ACCOUNT_NAME);
    String containerNameInResponse = response.headers().get(RestUtils.Headers.TARGET_CONTAINER_NAME);
    if (accountName != null && containerName != null) {
      assertEquals("Account name does not match that to which blob was uploaded", accountName, accountNameInResponse);
      assertEquals("Container name does not match that to which blob was uploaded", containerName,
          containerNameInResponse);
    } else {
      assertNull("Response should not have any account name - has " + accountNameInResponse, accountNameInResponse);
      assertNull("Response should not have any container name - has " + containerNameInResponse,
          containerNameInResponse);
    }
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param response the {@link HttpResponse} which contains the headers of the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @param content the content accompanying the response.
   */
  void verifyUserMetadata(HttpHeaders expectedHeaders, HttpResponse response, byte[] usermetadata,
      Queue<HttpObject> content, Container container) {
    if (usermetadata == null) {
      Set<String> keysToNotPrefix =
          container != null ? container.getUserMetadataKeysToNotPrefixInResponse() : Collections.emptySet();
      for (Map.Entry<String, String> header : expectedHeaders) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          String keyWithoutPrefix = key.substring(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.length());
          if (keysToNotPrefix.contains(keyWithoutPrefix)) {
            key = keyWithoutPrefix;
          }
          assertEquals("Value for " + key + " does not match in user metadata", header.getValue(),
              response.headers().get(key));
        }
      }
      for (Map.Entry<String, String> header : response.headers()) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertTrue("Key " + key + " does not exist in expected headers", expectedHeaders.contains(key));
        }
        if (keysToNotPrefix.contains(key)) {
          assertTrue("Key " + key + " does not exist in expected headers",
              expectedHeaders.contains(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key));
        }
      }
    } else {
      assertEquals("Content-Length is not as expected", usermetadata.length, HttpUtil.getContentLength(response));
      byte[] receivedMetadata = getContent(content, HttpUtil.getContentLength(response)).array();
      assertArrayEquals("User metadata does not match original", usermetadata, receivedMetadata);
    }
  }

  /**
   * Updates the TTL of the given {@code blobId} and verifies it by doing a BlobInfo.
   * @param blobId the blob ID of the blob to update and verify.
   * @param getExpectedHeaders the expected headers in the getBlobInfo response after the TTL update.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void updateBlobTtlAndVerify(String blobId, HttpHeaders getExpectedHeaders, boolean isPrivate, String accountName,
      String containerName, byte[] usermetadata, Container container) throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(RestUtils.Headers.BLOB_ID, blobId);
    headers.set(RestUtils.Headers.SERVICE_ID, "updateBlobTtlAndVerify");
    FullHttpRequest httpRequest = buildRequest(HttpMethod.PUT, "/" + Operations.UPDATE_TTL, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    verifyUpdateBlobTtlResponse(responseParts);
    getExpectedHeaders.remove(RestUtils.Headers.TTL);
    getBlobInfoAndVerify(blobId, GetOption.None, getExpectedHeaders, isPrivate, accountName, containerName,
        usermetadata, container);
  }

  /**
   * Verifies the response received after updating the TTL of a blob
   * @param responseParts the parts of the response received
   */
  void verifyUpdateBlobTtlResponse(NettyClient.ResponseParts responseParts) {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    verifyTrackingHeaders(response);
    verifyTtlUpdateRequestCostHeaders(response);
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void deleteBlobAndVerify(String blobId) throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Undelete given {@code blobId} and verifies it by doing a BlobInfo.
   * @param blobId the blob ID of the blob to update and verify.
   * @param getExpectedHeaders the expected headers in the getBlobInfo response after the TTL update.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void undeleteBlobAndVerify(String blobId, HttpHeaders getExpectedHeaders, boolean isPrivate, String accountName,
      String containerName, byte[] usermetadata, Container container) throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(RestUtils.Headers.BLOB_ID, blobId);
    headers.set(RestUtils.Headers.SERVICE_ID, "updateBlobTtlAndVerify");
    FullHttpRequest httpRequest = buildRequest(HttpMethod.PUT, "/" + Operations.UNDELETE, headers, null);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    verifyUndeleteBlobResponse(responseParts);
    getBlobInfoAndVerify(blobId, GetOption.None, getExpectedHeaders, isPrivate, accountName, containerName,
        usermetadata, container);
  }

  /**
   * Verifies right response headers for undelete blob.
   * @param responseParts {@link com.github.ambry.rest.NettyClient.ResponseParts} object.
   */
  void verifyUndeleteBlobResponse(NettyClient.ResponseParts responseParts) {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    verifyTrackingHeaders(response);
  }

  /**
   * Verifies that the right response code is returned for GET, HEAD, TTL update and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @param expectedHeaders the expected headers in the response if the right options are provided.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in {@code response}.
   * @param containerName the expected container name in {@code response}.
   * @param expectedContent the expected content of the blob if the right options are provided.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @param avoidTtlUpdate {@code true} to avoid doing ttl update.
   * @throws Exception
   */
  void verifyOperationsAfterDelete(String blobId, HttpHeaders expectedHeaders, boolean isPrivate, String accountName,
      String containerName, ByteBuffer expectedContent, byte[] usermetadata, boolean avoidTtlUpdate,
      Container container) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders().add(RestUtils.Headers.GET_OPTION, GetOption.None.toString());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);
    httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);
    httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    if (avoidTtlUpdate) {
      headers = new DefaultHttpHeaders().set(RestUtils.Headers.BLOB_ID, blobId)
          .set(RestUtils.Headers.SERVICE_ID, "verifyOperationsAfterDelete");
      httpRequest = buildRequest(HttpMethod.PUT, "/" + Operations.UPDATE_TTL, headers, null);
      verifyDeleted(httpRequest, HttpResponseStatus.GONE);
    }

    httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);

    GetOption[] options = {GetOption.Include_Deleted_Blobs, GetOption.Include_All};
    for (GetOption option : options) {
      getBlobAndVerify(blobId, null, option, false, expectedHeaders, isPrivate, expectedContent, accountName,
          containerName, container);
      getNotModifiedBlobAndVerify(blobId, option, isPrivate, container);
      getUserMetadataAndVerify(blobId, option, expectedHeaders, usermetadata, container);
      getBlobInfoAndVerify(blobId, option, expectedHeaders, isPrivate, accountName, containerName, usermetadata,
          container);
      getHeadAndVerify(blobId, null, option, expectedHeaders, isPrivate, accountName, containerName);
    }
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param httpRequest the {@link FullHttpRequest} to send to the server.
   * @param expectedStatusCode the expected {@link HttpResponseStatus}.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void verifyDeleted(FullHttpRequest httpRequest, HttpResponseStatus expectedStatusCode)
      throws ExecutionException, InterruptedException {
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", expectedStatusCode, response.status());
    assertNotNull("No Date header", response.headers().get(HttpHeaderNames.DATE, null));
    assertNoContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    verifyTrackingHeaders(response);
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   */
  void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders) {
    assertNotNull("No Date header", receivedHeaders.get(HttpHeaderNames.DATE));
    assertNotNull("No Last-Modified header", receivedHeaders.get(HttpHeaderNames.LAST_MODIFIED));
  }

  /**
   * Verifies that the right cache headers are returned.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param response the {@link HttpResponse}.
   */
  void verifyCacheHeaders(boolean isPrivate, HttpResponse response, long cacheValiditySeconds,
      Long cacheTtlFromContainer) {
    if (isPrivate) {
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          response.headers().get(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache", response.headers().get(RestUtils.Headers.PRAGMA));
    } else {
      String expiresValue = response.headers().get(RestUtils.Headers.EXPIRES);
      assertNotNull("Expires value should be non null", expiresValue);
      assertTrue("Expires value should be in future",
          RestUtils.getTimeFromDateString(expiresValue) > System.currentTimeMillis());
      if (cacheTtlFromContainer != null) {
        cacheValiditySeconds = cacheTtlFromContainer.longValue();
      }
      Assert.assertEquals("Cache-Control value not as expected", "max-age=" + cacheValiditySeconds,
          response.headers().get(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertNull("Pragma value should not have been set", response.headers().get(RestUtils.Headers.PRAGMA));
    }
  }

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @param usermetadata the {@link ByteBuffer} that represents user metadata
   * @return the blob ID of the blob.
   * @throws Exception
   */
  String multipartPostBlobAndVerify(HttpHeaders headers, ByteBuffer content, ByteBuffer usermetadata) throws Exception {
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", headers);
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, content, usermetadata);
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(encoder.finalizeRequest(), encoder, null).get();
    return verifyPostAndReturnBlobId(responseParts, content.capacity(), false);
  }

  /**
   * Creates a {@link HttpPostRequestEncoder} that encodes the given {@code request} and {@code blobContent}.
   * @param request the {@link HttpRequest} containing headers and other metadata about the request.
   * @param blobContent the {@link ByteBuffer} that represents the content of the blob.
   * @param usermetadata the {@link ByteBuffer} that represents user metadata
   * @return a {@link HttpPostRequestEncoder} that can encode the {@code request} and {@code blobContent}.
   * @throws HttpPostRequestEncoder.ErrorDataEncoderException
   * @throws IOException
   */
  HttpPostRequestEncoder createEncoder(HttpRequest request, ByteBuffer blobContent, ByteBuffer usermetadata)
      throws HttpPostRequestEncoder.ErrorDataEncoderException, IOException {
    HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
    HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(httpDataFactory, request, true);
    FileUpload fileUpload = new MemoryFileUpload(RestUtils.MultipartPost.BLOB_PART, RestUtils.MultipartPost.BLOB_PART,
        "application/octet-stream", "", StandardCharsets.UTF_8, blobContent.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(blobContent));
    encoder.addBodyHttpData(fileUpload);
    fileUpload =
        new MemoryFileUpload(RestUtils.MultipartPost.USER_METADATA_PART, RestUtils.MultipartPost.USER_METADATA_PART,
            "application/octet-stream", "", StandardCharsets.UTF_8, usermetadata.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(usermetadata));
    encoder.addBodyHttpData(fileUpload);
    return encoder;
  }

  /**
   * @param responseParts a {@link NettyClient.ResponseParts}.
   * @return the first response part, which should be a {@link HttpResponse}.
   * @throws IllegalStateException if the response part queue is empty.
   */
  HttpResponse getHttpResponse(NettyClient.ResponseParts responseParts) {
    HttpResponse httpResponse = (HttpResponse) responseParts.queue.poll();
    if (httpResponse == null) {
      throw new IllegalStateException(
          "Should have received response. completion context: " + responseParts.completionContext);
    }
    return httpResponse;
  }

  /**
   * Verify the tracking headers were attached to the response properly.
   * @param response the {@link HttpResponse} to be verified.
   */
  void verifyTrackingHeaders(HttpResponse response) {
    Assert.assertEquals("Unexpected or missing tracking header for datacenter name", DATA_CENTER_NAME,
        response.headers().get(RestUtils.TrackingHeaders.DATACENTER_NAME));
    Assert.assertEquals("Unexpected or missing tracking header for hostname", HOST_NAME,
        response.headers().get(RestUtils.TrackingHeaders.FRONTEND_NAME));
  }

  /**
   * Verify the request cost headers were attached to the post response properly.
   * @param response the {@link HttpResponse} to be verified.
   * @param contentSize size of the content posted.
   */
  protected void verifyPostRequestCostHeaders(HttpResponse response, long contentSize) {
    double cuCost = contentSize / SimpleAmbryCostModelPolicy.CU_COST_UNIT;
    cuCost = Math.max(cuCost, 1);
    double storageCost = contentSize / (double) QuotaUtils.BYTES_IN_GB;
    verifyCommonRequestCostHeaders(response, cuCost, storageCost, false);
  }

  /**
   * Verify the request cost headers were attached to the get response properly.
   * @param response the {@link HttpResponse} to be verified.
   * @param contentSize size of the blob.
   */
  private void verifyGetRequestCostHeaders(HttpResponse response, long contentSize) {
    double cuCost = Math.ceil(contentSize / SimpleAmbryCostModelPolicy.CU_COST_UNIT);
    cuCost = Math.max(cuCost, 1);
    verifyCommonRequestCostHeaders(response, cuCost, 0, true);
  }

  /**
   * Verify the request cost headers were attached to the HEAD response properly.
   * @param response the {@link HttpResponse} to be verified.
   */
  private void verifyHeadRequestCostHeaders(HttpResponse response) {
    verifyCommonRequestCostHeaders(response, 1, 0, true);
  }

  /**
   * Verify the request cost headers were attached to the HEAD response properly.
   * @param response the {@link HttpResponse} to be verified.
   */
  private void verifyTtlUpdateRequestCostHeaders(HttpResponse response) {
    verifyCommonRequestCostHeaders(response, 1, 0.0, false);
  }

  /**
   * Verify the request cost headers were attached to the response properly.
   * @param response the {@link HttpResponse} to be verified.
   */
  private void verifyCommonRequestCostHeaders(HttpResponse response, double expectedCuCost, double expectedStorageCost,
      boolean isRead) {
    String cuUnitName = isRead ? QuotaName.READ_CAPACITY_UNIT.name() : QuotaName.WRITE_CAPACITY_UNIT.name();
    Assert.assertTrue("Request cost header should be present",
        response.headers().contains(RestUtils.RequestCostHeaders.REQUEST_COST));
    Map<String, String> costMap = RestUtils.KVHeaderValueEncoderDecoder.decodeKVHeaderValue(
        response.headers().get(RestUtils.RequestCostHeaders.REQUEST_COST));
    Assert.assertEquals("There should be two entries in request cost map", 2, costMap.size());
    Assert.assertTrue(cuUnitName + " should be present in cost map", costMap.containsKey(cuUnitName));
    Assert.assertTrue(QuotaName.STORAGE_IN_GB.name() + " should be present in cost map",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    Assert.assertEquals("Invalid " + QuotaName.STORAGE_IN_GB.name() + " cost.", expectedStorageCost,
        Double.parseDouble(costMap.get(QuotaName.STORAGE_IN_GB.name())), 0.000001);
    Assert.assertEquals("Invalid " + cuUnitName + " cost.", expectedCuCost, Double.parseDouble(costMap.get(cuUnitName)),
        0.000001);
  }

  /**
   * Call the {@code POST /accounts} API to update account metadata and verify that the update succeeded.
   * @param accountService {@link AccountService} object.
   * @param accounts the accounts to replace or add using the {@code POST /accounts} call.
   */
  void updateAccountsAndVerify(AccountService accountService, Account... accounts) throws Exception {
    byte[] accountUpdateJson = AccountCollectionSerde.serializeAccountsInJson(Arrays.asList(accounts));
    FullHttpRequest request =
        buildRequest(HttpMethod.POST, Operations.ACCOUNTS, null, ByteBuffer.wrap(accountUpdateJson));
    NettyClient.ResponseParts responseParts = nettyClient.sendRequest(request, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    verifyTrackingHeaders(response);
    assertNoContent(responseParts.queue, 1);

    for (Account account : accounts) {
      assertEquals("Update not reflected in AccountService", account, accountService.getAccountById(account.getId()));
    }
  }

  /**
   * A utility class to represent the response structure returned by List named blob.
   */
  static class NamedBlobEntry {
    private String blobName;
    private long expirationTimeMs;

    public NamedBlobEntry() {
    }

    public NamedBlobEntry(String blobName, long expiration) {
      this.blobName = blobName;
      this.expirationTimeMs = expiration;
    }

    public String getBlobName() {
      return blobName;
    }

    public void setBlobName(String blobName) {
      this.blobName = blobName;
    }

    public long getExpirationTimeMs() {
      return expirationTimeMs;
    }

    public void setExpirationTimeMs(long expirationTimeMs) {
      this.expirationTimeMs = expirationTimeMs;
    }
  }

  /**
   * A utility class to represent the response structure returned by List named blob.
   */
  static class ListNamedBlobResponse {
    private List<NamedBlobEntry> entries;
    private String nextPageToken;

    public ListNamedBlobResponse() {
    }

    public ListNamedBlobResponse(List<NamedBlobEntry> entries, String nextPageToken) {
      this.entries = entries;
      this.nextPageToken = nextPageToken;
    }

    public List<NamedBlobEntry> getEntries() {
      return entries;
    }

    public void setEntries(List<NamedBlobEntry> entries) {
      this.entries = entries;
    }

    public String getNextPageToken() {
      return nextPageToken;
    }

    public void setNextPageToken(String nextPageToken) {
      this.nextPageToken = nextPageToken;
    }
  }
}
