/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration tests for Admin.
 */
public class AdminIntegrationTest {
  private static final int SERVER_PORT = 16503;
  private static final ClusterMap CLUSTER_MAP;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RestServer adminRestServer = null;
  private static NettyClient nettyClient = null;

  /**
   * Sets up an admin server.
   * @throws InstantiationException
   * @throws InterruptedException
   */
  @BeforeClass
  public static void setup()
      throws InstantiationException, InterruptedException {
    adminRestServer = new RestServer(buildAdminVProps(), CLUSTER_MAP, new LoggingNotificationSystem());
    adminRestServer.start();
    nettyClient = new NettyClient("localhost", SERVER_PORT);
  }

  /**
   * Shuts down the admin server.
   */
  @AfterClass
  public static void teardown() {
    if (nettyClient != null) {
      nettyClient.close();
    }
    if (adminRestServer != null) {
      adminRestServer.shutdown();
    }
  }

  /**
   * Tests the {@link AdminBlobStorageService#ECHO} operation. Checks to see that the echo matches input text.
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws JSONException
   */
  @Test
  public void echoTest()
      throws ExecutionException, InterruptedException, JSONException {
    String inputText = "loremIpsum";
    String uri = AdminBlobStorageService.ECHO + "?" + EchoHandler.TEXT_KEY + "=" + inputText;
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, uri, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected status", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Unexpected Content-Type", "application/json",
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
    ByteBuffer buffer = getContent(response, responseParts);
    String echoedText = new JSONObject(new String(buffer.array())).getString(EchoHandler.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  /**
   * Tests the {@link AdminBlobStorageService#GET_REPLICAS_FOR_BLOB_ID} operation.
   * <p/>
   * For a random {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created. The string representation
   * is sent to the server as a part of request. The returned replica list is checked for equality against a locally
   * obtained replica list.
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws JSONException
   */
  @Test
  public void getReplicasForBlobIdTest()
      throws ExecutionException, InterruptedException, JSONException {
    List<PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds();
    PartitionId partitionId = partitionIds.get(new Random().nextInt(partitionIds.size()));
    String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
    BlobId blobId = new BlobId(partitionId);
    String uri =
        AdminBlobStorageService.GET_REPLICAS_FOR_BLOB_ID + "?" + GetReplicasForBlobIdHandler.BLOB_ID_KEY + "=" + blobId;
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, uri, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected status", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Unexpected Content-Type", "application/json",
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
    ByteBuffer buffer = getContent(response, responseParts);
    JSONObject responseObj = new JSONObject(new String(buffer.array()));
    String returnedReplicasStr = responseObj.getString(GetReplicasForBlobIdHandler.REPLICAS_KEY).replace("\"", "");
    assertEquals("Returned response for the BlobId do no match with the replicas IDs of partition", originalReplicaStr,
        returnedReplicasStr);
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void postGetHeadDeleteTest()
      throws ExecutionException, InterruptedException {
    ByteBuffer content = ByteBuffer.wrap(RestTestUtils.getRandomBytes(1024));
    String serviceId = "postGetHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeaders(headers, content.capacity(), 7200, false, serviceId, contentType, ownerId);
    headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.capacity());
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");

    String blobId = postBlobAndVerify(headers, content);
    getBlobAndVerify(blobId, headers, content);
    getHeadAndVerify(blobId, headers);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId);
  }

  // helpers
  // general

  /**
   * Method to easily create a request.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link HttpHeaders} object. Can be null.
   * @param content the content that accompanies the request. Can be null.
   * @return A {@link FullHttpRequest} object that defines the request required by the input.
   */
  private FullHttpRequest buildRequest(HttpMethod httpMethod, String uri, HttpHeaders headers, ByteBuffer content) {
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
    return httpRequest;
  }

  /**
   * Combines all the parts in {@code contents} into one {@link ByteBuffer}.
   * @param response the {@link HttpResponse} containing headers.
   * @param contents the content of the response.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  private ByteBuffer getContent(HttpResponse response, Queue<HttpObject> contents) {
    long contentLength = HttpHeaders.getContentLength(response, -1);
    if (contentLength == -1) {
      contentLength = HttpHeaders.getIntHeader(response, RestUtils.Headers.BLOB_SIZE, 0);
    }
    ByteBuffer buffer = ByteBuffer.allocate((int) contentLength);
    for (HttpObject object : contents) {
      HttpContent content = (HttpContent) object;
      buffer.put(content.content().nioBuffer());
      ReferenceCountUtil.release(content);
    }
    return buffer;
  }

  /**
   * Discards all the content in {@code contents}.
   * @param contents the content to discard.
   * @param expectedDiscardCount the number of {@link HttpObject}s that are expected to discarded.
   */
  private void discardContent(Queue<HttpObject> contents, int expectedDiscardCount) {
    assertEquals("Objects that will be discarded differ from expected", expectedDiscardCount, contents.size());
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been only a single end marker", endMarkerFound);
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(object);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
  }

  // BeforeClass helpers

  /**
   * Builds properties required to start a {@link RestServer} as an Admin server.
   * @return a {@link VerifiableProperties} with the parameters for an Admin server.
   */
  private static VerifiableProperties buildAdminVProps() {
    Properties properties = new Properties();
    properties.put("rest.server.blob.storage.service.factory", "com.github.ambry.admin.AdminBlobStorageServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("netty.server.port", Integer.toString(SERVER_PORT));
    return new VerifiableProperties(properties);
  }

  // postGetHeadDeleteTest() helpers

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param httpHeaders the {@link HttpHeaders} where the headers should be set.
   * @param contentLength sets the {@link RestUtils.Headers#BLOB_SIZE} header. Required.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   */
  private void setAmbryHeaders(HttpHeaders httpHeaders, long contentLength, long ttlInSecs, boolean isPrivate,
      String serviceId, String contentType, String ownerId) {
    if (httpHeaders != null && contentLength >= 0 && ttlInSecs >= -1 && serviceId != null && contentType != null) {
      httpHeaders.add(RestUtils.Headers.BLOB_SIZE, contentLength);
      httpHeaders.add(RestUtils.Headers.TTL, ttlInSecs);
      httpHeaders.add(RestUtils.Headers.PRIVATE, isPrivate);
      httpHeaders.add(RestUtils.Headers.SERVICE_ID, serviceId);
      httpHeaders.add(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
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
  private String postBlobAndVerify(HttpHeaders headers, ByteBuffer content)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, "/", headers, content);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts, 1);
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        HttpHeaders.getHeader(response, RestUtils.Headers.CREATION_TIME, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpHeaders.getContentLength(response));
    String blobId = HttpHeaders.getHeader(response, HttpHeaders.Names.LOCATION, null);

    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobAndVerify(String blobId, HttpHeaders expectedHeaders, ByteBuffer expectedContent)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers(), expectedHeaders);
    ByteBuffer responseContent = getContent(response, responseParts);
    assertArrayEquals("GET content does not match original content", expectedContent.array(), responseContent.array());
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getHeadAndVerify(String blobId, HttpHeaders expectedHeaders)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts, 1);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers(), expectedHeaders);
    assertEquals("Content-Length does not match blob size",
        Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)), HttpHeaders.getContentLength(response));
    assertEquals("Blob size does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        HttpHeaders.getHeader(response, RestUtils.Headers.BLOB_SIZE));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        HttpHeaders.getHeader(response, RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", expectedHeaders.get(RestUtils.Headers.PRIVATE),
        HttpHeaders.getHeader(response, RestUtils.Headers.PRIVATE));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        HttpHeaders.getHeader(response, RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        HttpHeaders.getHeader(response, RestUtils.Headers.CREATION_TIME, null) != null);
    if (Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          HttpHeaders.getHeader(response, RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          HttpHeaders.getHeader(response, RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void deleteBlobAndVerify(String blobId)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Verifies that the right response code is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void verifyOperationsAfterDelete(String blobId)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param httpRequest the {@link FullHttpRequest} to send to the server.
   * @param expectedStatusCode the expected {@link HttpResponseStatus}.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void verifyDeleted(FullHttpRequest httpRequest, HttpResponseStatus expectedStatusCode)
      throws ExecutionException, InterruptedException {
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts, 1);
    assertEquals("Unexpected response status", expectedStatusCode, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   * @param expectedHeaders the expected headers.
   */
  private void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders, HttpHeaders expectedHeaders) {
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        receivedHeaders.get(HttpHeaders.Names.CONTENT_TYPE));
    assertTrue("No Date header", receivedHeaders.get(HttpHeaders.Names.DATE) != null);
    assertTrue("No Last-Modified header", receivedHeaders.get(HttpHeaders.Names.LAST_MODIFIED) != null);
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        receivedHeaders.get(RestUtils.Headers.BLOB_SIZE));
  }
}
