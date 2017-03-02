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
package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for Ambry frontend.
 */
@RunWith(Parameterized.class)
public class FrontendIntegrationTest {
  private static final int PLAINTEXT_SERVER_PORT = 1174;
  private static final int SSL_SERVER_PORT = 1175;
  private static final ClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final VerifiableProperties SSL_CLIENT_VERIFIABLE_PROPS;
  private static final FrontendConfig FRONTEND_CONFIG;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVProps(trustStoreFile);
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RestServer ambryRestServer = null;
  private static NettyClient plaintextNettyClient = null;
  private static NettyClient sslNettyClient = null;

  private final NettyClient nettyClient;

  /**
   * Running it many times so that keep-alive bugs are caught.
   * We also want to test using both the SSL and plaintext ports.
   * @return a list of arrays that represent the constructor arguments for that run of the test.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    List<Object[]> parameters = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      parameters.add(new Object[]{false});
      parameters.add(new Object[]{true});
    }
    return parameters;
  }

  /**
   * Sets up an Ambry frontend server.
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    ambryRestServer = new RestServer(FRONTEND_VERIFIABLE_PROPS, CLUSTER_MAP, new LoggingNotificationSystem(),
        new SSLFactory(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    ambryRestServer.start();
    plaintextNettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
    sslNettyClient =
        new NettyClient("localhost", SSL_SERVER_PORT, new SSLFactory(new SSLConfig(SSL_CLIENT_VERIFIABLE_PROPS)));
  }

  /**
   * Shuts down the Ambry frontend server.
   */
  @AfterClass
  public static void teardown() {
    if (plaintextNettyClient != null) {
      plaintextNettyClient.close();
    }
    if (sslNettyClient != null) {
      sslNettyClient.close();
    }
    if (ambryRestServer != null) {
      ambryRestServer.shutdown();
    }
  }

  /**
   * @param useSSL {@code true} if SSL should be tested.
   */
  public FrontendIntegrationTest(boolean useSSL) {
    nettyClient = useSSL ? sslNettyClient : plaintextNettyClient;
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadDeleteTest() throws Exception {
    doPostGetHeadDeleteTest(0, false);
    doPostGetHeadDeleteTest(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes - 1, false);
    doPostGetHeadDeleteTest(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes, false);
    doPostGetHeadDeleteTest(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes * 3, false);
  }

  /**
   * Tests multipart POST and verifies it via GET operations.
   * @throws Exception
   */
  @Test
  public void multipartPostGetHeadTest() throws Exception {
    doPostGetHeadDeleteTest(0, true);
    doPostGetHeadDeleteTest(1024, true);
  }

  /*
   * Tests health check request
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void healthCheckRequestTest() throws ExecutionException, InterruptedException, IOException {
    FullHttpRequest httpRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthCheck", Unpooled.buffer(0));
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    final String expectedResponseBody = "GOOD";
    ByteBuffer content = getContent(responseParts, expectedResponseBody.length());
    assertEquals("GET content does not match original content", expectedResponseBody, new String(content.array()));
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
   * @param contents the content of the response.
   * @param expectedContentLength the length of the contents in bytes.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  private ByteBuffer getContent(Queue<HttpObject> contents, long expectedContentLength) {
    ByteBuffer buffer = ByteBuffer.allocate((int) expectedContentLength);
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      buffer.put(content.content().nioBuffer());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(content);
    }
    assertEquals("Content length did not match expected", expectedContentLength, buffer.position());
    assertTrue("End marker was not found", endMarkerFound);
    buffer.flip();
    return buffer;
  }

  /**
   * Verifies that no content has been sent as part of the response or readable bytes is equivalent to 0
   * @param contents the content of the response.
   */
  private void assertNoContent(Queue<HttpObject> contents) {
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      assertEquals("No content expected ", 0, content.content().readableBytes());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(content);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
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
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(object);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
  }

  // BeforeClass helpers

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.put("rest.server.blob.storage.service.factory",
        "com.github.ambry.frontend.AmbryBlobStorageServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("netty.server.port", Integer.toString(PLAINTEXT_SERVER_PORT));
    properties.put("netty.server.ssl.port", Integer.toString(SSL_SERVER_PORT));
    properties.put("netty.server.enable.ssl", "true");
    // to test that backpressure does not impede correct operation.
    properties.put("netty.server.request.buffer.watermark", "1");
    TestSSLUtils.addSSLProperties(properties, "", SSLFactory.Mode.SERVER, trustStoreFile, "frontend");
    return new VerifiableProperties(properties);
  }

  // postGetHeadDeleteTest() and multipartPostGetHeadTest() helpers

  /**
   * Utility to test blob POST, GET, HEAD and DELETE operations for a specified size
   * @param contentSize the size of the blob to be tested
   * @param multipartPost {@code true} if multipart POST is desired, {@code false} otherwise.
   * @throws Exception
   */
  private void doPostGetHeadDeleteTest(int contentSize, boolean multipartPost) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentSize));
    String serviceId = "postGetHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeaders(headers, content.capacity(), 7200, false, serviceId, contentType, ownerId);
    headers.set(HttpHeaderNames.CONTENT_LENGTH, content.capacity());
    String blobId;
    byte[] usermetadata = null;
    if (multipartPost) {
      usermetadata = UtilsTest.getRandomString(32).getBytes();
      blobId = multipartPostBlobAndVerify(headers, content, ByteBuffer.wrap(usermetadata));
    } else {
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
      blobId = postBlobAndVerify(headers, content);
    }
    getBlobAndVerify(blobId, null, headers, content);
    getHeadAndVerify(blobId, null, headers);
    ByteRange range = ByteRange.fromLastNBytes(ThreadLocalRandom.current().nextLong(content.capacity() + 1));
    getBlobAndVerify(blobId, range, headers, content);
    getHeadAndVerify(blobId, range, headers);
    if (contentSize > 0) {
      range = ByteRange.fromStartOffset(ThreadLocalRandom.current().nextLong(content.capacity()));
      getBlobAndVerify(blobId, range, headers, content);
      getHeadAndVerify(blobId, range, headers);
      long random1 = ThreadLocalRandom.current().nextLong(content.capacity());
      long random2 = ThreadLocalRandom.current().nextLong(content.capacity());
      range = ByteRange.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
      getBlobAndVerify(blobId, range, headers, content);
      getHeadAndVerify(blobId, range, headers);
    }
    getNotModifiedBlobAndVerify(blobId, false);
    getUserMetadataAndVerify(blobId, headers, usermetadata);
    getBlobInfoAndVerify(blobId, headers, usermetadata);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId);
  }

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
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);

    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param range the {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobAndVerify(String blobId, ByteRange range, HttpHeaders expectedHeaders, ByteBuffer expectedContent)
      throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = null;
    if (range != null) {
      headers = new DefaultHttpHeaders().add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertEquals("Accept-Ranges not set correctly", "bytes", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    byte[] expectedContentArray = expectedContent.array();
    if (range != null) {
      long blobSize = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
      assertEquals("Content-Range header not set correctly",
          RestUtils.buildContentRangeAndLength(range, blobSize).getFirst(),
          response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      ByteRange resolvedRange = range.toResolvedByteRange(blobSize);
      expectedContentArray = Arrays.copyOfRange(expectedContentArray, (int) resolvedRange.getStartOffset(),
          (int) resolvedRange.getEndOffset() + 1);
    } else {
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    }
    if (expectedContentArray.length < FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes) {
      assertEquals("Content-length not as expected", expectedContentArray.length, HttpUtil.getContentLength(response));
    }
    verifyCacheHeaders(Boolean.parseBoolean(expectedHeaders.get(RestUtils.Headers.PRIVATE)), response);
    byte[] responseContentArray = getContent(responseParts, expectedContentArray.length).array();
    assertArrayEquals("GET content does not match original content", expectedContentArray, responseContentArray);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @throws Exception
   */
  private void getNotModifiedBlobAndVerify(String blobId, boolean isPrivate) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.IF_MODIFIED_SINCE, new Date());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.NOT_MODIFIED, response.status());
    assertNotNull("Date header should be set", response.headers().get(RestUtils.Headers.DATE));
    assertNotNull("Last-Modified header should be set", response.headers().get("Last-Modified"));
    assertNull("Content-Length should not be set", response.headers().get(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Accept-Ranges should not be set", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", response.headers().get(RestUtils.Headers.CONTENT_TYPE));
    verifyCacheHeaders(isPrivate, response);
    assertNoContent(responseParts);
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getUserMetadataAndVerify(String blobId, HttpHeaders expectedHeaders, byte[] usermetadata)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    checkCommonGetHeadHeaders(response.headers());
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobInfoAndVerify(String blobId, HttpHeaders expectedHeaders, byte[] usermetadata)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, null, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    checkCommonGetHeadHeaders(response.headers());
    verifyBlobProperties(expectedHeaders, response);
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param range the {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getHeadAndVerify(String blobId, ByteRange range, HttpHeaders expectedHeaders)
      throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = null;
    if (range != null) {
      headers = new DefaultHttpHeaders().add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    long contentLength = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength);
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
    verifyBlobProperties(expectedHeaders, response);
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param response the {@link HttpResponse} that contains the headers.
   */
  private void verifyBlobProperties(HttpHeaders expectedHeaders, HttpResponse response) {
    assertEquals("Blob size does not match", Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)),
        Long.parseLong(response.headers().get(RestUtils.Headers.BLOB_SIZE)));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        response.headers().get(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", expectedHeaders.get(RestUtils.Headers.PRIVATE),
        response.headers().get(RestUtils.Headers.PRIVATE));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null) != null);
    if (Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          response.headers().get(RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          response.headers().get(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param response the {@link HttpResponse} which contains the headers of the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @param content the content accompanying the response.
   */
  private void verifyUserMetadata(HttpHeaders expectedHeaders, HttpResponse response, byte[] usermetadata,
      Queue<HttpObject> content) {
    if (usermetadata == null) {
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      for (Map.Entry<String, String> header : expectedHeaders) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertEquals("Value for " + key + " does not match in user metadata", header.getValue(),
              response.headers().get(key));
        }
      }
      for (Map.Entry<String, String> header : response.headers()) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertTrue("Key " + key + " does not exist in expected headers", expectedHeaders.contains(key));
        }
      }
      discardContent(content, 1);
    } else {
      assertEquals("Content-Length is not as expected", usermetadata.length, HttpUtil.getContentLength(response));
      byte[] receivedMetadata = getContent(content, HttpUtil.getContentLength(response)).array();
      assertArrayEquals("User metadata does not match original", usermetadata, receivedMetadata);
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void deleteBlobAndVerify(String blobId) throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Verifies that the right response code is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void verifyOperationsAfterDelete(String blobId) throws ExecutionException, InterruptedException {
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
    assertEquals("Unexpected response status", expectedStatusCode, response.status());
    assertTrue("No Date header", response.headers().get(HttpHeaderNames.DATE, null) != null);
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   */
  private void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders) {
    assertTrue("No Date header", receivedHeaders.get(HttpHeaderNames.DATE) != null);
    assertTrue("No Last-Modified header", receivedHeaders.get(HttpHeaderNames.LAST_MODIFIED) != null);
  }

  /**
   * Verifies that the right cache headers are returned.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param response the {@link HttpResponse}.
   */
  private void verifyCacheHeaders(boolean isPrivate, HttpResponse response) {
    if (isPrivate) {
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          response.headers().get(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache", response.headers().get(RestUtils.Headers.PRAGMA));
    } else {
      String expiresValue = response.headers().get(RestUtils.Headers.EXPIRES);
      assertNotNull("Expires value should be non null", expiresValue);
      assertTrue("Expires value should be in future",
          RestUtils.getTimeFromDateString(expiresValue) > System.currentTimeMillis());
      Assert.assertEquals("Cache-Control value not as expected",
          "max-age=" + FRONTEND_CONFIG.frontendCacheValiditySeconds,
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
  private String multipartPostBlobAndVerify(HttpHeaders headers, ByteBuffer content, ByteBuffer usermetadata)
      throws Exception {
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", headers);
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, content, usermetadata);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(encoder.finalizeRequest(), encoder, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.status());
    assertTrue("No Date header", response.headers().get(HttpHeaderNames.DATE, null) != null);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);

    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    return blobId;
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
  private HttpPostRequestEncoder createEncoder(HttpRequest request, ByteBuffer blobContent, ByteBuffer usermetadata)
      throws HttpPostRequestEncoder.ErrorDataEncoderException, IOException {
    HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
    HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(httpDataFactory, request, true);
    FileUpload fileUpload = new MemoryFileUpload(RestUtils.MultipartPost.BLOB_PART, RestUtils.MultipartPost.BLOB_PART,
        "application/octet-stream", "", Charset.forName("UTF-8"), blobContent.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(blobContent));
    encoder.addBodyHttpData(fileUpload);
    fileUpload =
        new MemoryFileUpload(RestUtils.MultipartPost.USER_METADATA_PART, RestUtils.MultipartPost.USER_METADATA_PART,
            "application/octet-stream", "", Charset.forName("UTF-8"), usermetadata.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(usermetadata));
    encoder.addBodyHttpData(fileUpload);
    return encoder;
  }
}
