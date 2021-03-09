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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.utils.TestUtils;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for Ambry frontend with quota.
 */
@RunWith(Parameterized.class)
public class FrontendQuotaIntegrationTest extends FrontendIntegrationTestBase {
  private static final String DEFAULT_PARTITION_CLASS = "default-partition-class";
  private static final MockClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final File TRUST_STORE_FILE;
  private static final FrontendConfig FRONTEND_CONFIG;
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static Account ACCOUNT;
  private static Container CONTAINER;
  private static RestServer ambryRestServer = null;
  private final boolean throttleRequest;

  /**
   * @param throttleRequest {@code true} if quota manager should reject quota requests.
   */
  public FrontendQuotaIntegrationTest(boolean throttleRequest) {
    super(null, null);
    this.throttleRequest = throttleRequest;
  }

  /**
   * @return a list of arrays that represent the constructor argument to make quota manager accept or reject requests.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{true}, {false}});
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVPropsForQuota(File trustStoreFile, String quotaEnforcerStr,
      boolean isRequestQuotaEnabled, QuotaMode quotaMode) throws IOException, GeneralSecurityException {
    Properties properties = buildFrontendVProps(trustStoreFile, true, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT);
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON, quotaEnforcerStr);
    properties.setProperty(QuotaConfig.THROTTLING_MODE, quotaMode.name());
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, String.valueOf(isRequestQuotaEnabled));
    return new VerifiableProperties(properties);
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    return new VerifiableProperties(buildFrontendVProps(trustStoreFile, true, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT));
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param enableUndelete enable undelete in frontend when it's true.
   * @param plaintextServerPort server port number to support plaintext protocol
   * @param sslServerPort server port number to support ssl protocol
   * @return a {@link Properties} with the parameters for an Ambry frontend server.
   */
  private static Properties buildFrontendVProps(File trustStoreFile, boolean enableUndelete, int plaintextServerPort,
      int sslServerPort) throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.put("rest.server.rest.request.service.factory",
        "com.github.ambry.frontend.FrontendRestRequestServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("rest.server.account.service.factory", "com.github.ambry.account.InMemAccountServiceFactory");
    properties.put("netty.server.port", Integer.toString(plaintextServerPort));
    properties.put("netty.server.ssl.port", Integer.toString(sslServerPort));
    properties.put("netty.server.enable.ssl", "true");
    properties.put(NettyConfig.SSL_FACTORY_KEY, NettySslFactory.class.getName());
    // to test that backpressure does not impede correct operation.
    properties.put("netty.server.request.buffer.watermark", "1");
    // to test that multipart requests over a certain size fail
    properties.put("netty.multipart.post.max.size.bytes", Long.toString(MAX_MULTIPART_POST_SIZE_BYTES));
    CommonTestUtils.populateRequiredRouterProps(properties);
    TestSSLUtils.addSSLProperties(properties, "", SSLFactory.Mode.SERVER, trustStoreFile, "frontend");
    // add key for singleKeyManagementService
    properties.put("kms.default.container.key", TestUtils.getRandomKey(32));
    properties.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    properties.setProperty("clustermap.datacenter.name", DATA_CENTER_NAME);
    properties.setProperty("clustermap.host.name", HOST_NAME);
    properties.setProperty("clustermap.port", String.valueOf(PORT));
    properties.setProperty(FrontendConfig.ENABLE_UNDELETE, Boolean.toString(enableUndelete));
    return properties;
  }

  /**
   * Build the default quota enforcer and source pair json.
   * @return Json string.
   */
  private static String buildDefaultQuotaEnforcerSourceInfoPairConfig(boolean throttleRequest) {
    JSONObject jsonObject = new JSONObject();
    if (throttleRequest) {
      jsonObject.put(QuotaConfig.ENFORCER_STR, "com.github.ambry.quota.RejectQuotaEnforcerFactory");
      jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.DummyQuotaSourceFactory");
    } else {
      jsonObject.put(QuotaConfig.ENFORCER_STR,
          "com.github.ambry.quota.capacityunit.AmbryCapacityUnitQuotaEnforcerFactory");
      jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.capacityunit.UnlimitedQuotaSourceFactory");
    }
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(jsonObject);
    return new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray).toString();
  }

  /**
   * Sets up an Ambry frontend server.
   * @throws Exception
   */
  @Before
  public void setup() throws Exception {
    ambryRestServer = new RestServer(FRONTEND_VERIFIABLE_PROPS, CLUSTER_MAP, new LoggingNotificationSystem(),
        SSLFactory.getNewInstance(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    String quotaEnforcerSourceInfoPairConfig = buildDefaultQuotaEnforcerSourceInfoPairConfig(throttleRequest);
    VerifiableProperties quotaProps =
        buildFrontendVPropsForQuota(TRUST_STORE_FILE, quotaEnforcerSourceInfoPairConfig, true, QuotaMode.THROTTLING);
    ambryRestServer = new RestServer(quotaProps, CLUSTER_MAP, new LoggingNotificationSystem(),
        SSLFactory.getNewInstance(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    ambryRestServer.start();
    this.frontendConfig = FRONTEND_CONFIG;
    this.nettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
  }

  /**
   * Shuts down the Ambry frontend server.
   */
  @After
  public void teardown() {
    if (nettyClient != null) {
      nettyClient.close();
    }
    if (ambryRestServer != null) {
      ambryRestServer.shutdown();
    }
  }

  /**
   * Tests blob POST, GET, HEAD, TTL update and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadUpdateDeleteUndeleteTest() throws Exception {
    int refContentSize = FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes * 3;
    ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount();
    CONTAINER = ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    doPostGetHeadUpdateDeleteUndeleteTest(refContentSize, ACCOUNT, CONTAINER, ACCOUNT.getName(),
        !CONTAINER.isCacheable(), ACCOUNT.getName(), CONTAINER.getName(), false);
  }

  @Override
  String postBlobAndVerify(HttpHeaders headers, ByteBuffer content, long contentSize)
      throws ExecutionException, InterruptedException {
    if (!throttleRequest) {
      return super.postBlobAndVerify(headers, content, contentSize);
    } else {
      FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, "/", headers, content);
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      return verifyPostRejectsAndReturnRandomBlobId(responseParts, contentSize);
    }
  }

  /**
   * Verifies a POST and returns the blob ID.
   * @param responseParts the response received from the server.
   * @returnn the blob ID of the blob.
   */
  String verifyPostRejectsAndReturnRandomBlobId(NettyClient.ResponseParts responseParts, long contentSize) {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertNull("No " + RestUtils.Headers.CREATION_TIME, response.headers().get(RestUtils.Headers.CREATION_TIME, null));
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);
    assertNull("Blob ID from POST should be null", blobId);
    assertNoContent(responseParts.queue, 1);
    assertFalse("Channel should be inactive", HttpUtil.isKeepAlive(response));
    assertEquals("No blob size should be returned in response", null,
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    verifyTrackingHeaders(response);
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, ACCOUNT.getId(), CONTAINER.getId(),
        new Partition(0L, DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE, 1073741824), false,
        BlobId.BlobDataType.SIMPLE).getID();
  }

  @Override
  void verifyGetBlobResponse(NettyClient.ResponseParts responseParts, ByteRange range, boolean resolveRangeOnEmptyBlob,
      HttpHeaders expectedHeaders, boolean isPrivate, ByteBuffer expectedContent, String accountName,
      String containerName) throws RestServiceException {
    if (!throttleRequest) {
      super.verifyGetBlobResponse(responseParts, range, resolveRangeOnEmptyBlob, expectedHeaders, isPrivate,
          expectedContent, accountName, containerName);
    } else {
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("Date header should be present", response.headers().contains(HttpHeaderNames.DATE));
      assertFalse("Last-Modified header should not be present",
          response.headers().contains(HttpHeaderNames.LAST_MODIFIED));
      assertTrue("Content-Type should not be null", response.headers().contains(HttpHeaderNames.CONTENT_TYPE));
      assertFalse(RestUtils.Headers.BLOB_SIZE + " should not be present",
          response.headers().contains(RestUtils.Headers.BLOB_SIZE));
      assertFalse(RestUtils.Headers.LIFE_VERSION + " should not be present",
          response.headers().contains(RestUtils.Headers.LIFE_VERSION));
      assertFalse("Content-Range header should not be present",
          response.headers().contains(RestUtils.Headers.CONTENT_RANGE));
      assertEquals("Content-length should be 0", 0, HttpUtil.getContentLength(response));
      verifyCacheHeadersAbsent(response);
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
      verifyBlobPropertiesHeadersAbsent(response);
      verifyAccountAndContainerHeaders(null, null, response);
      verifyUserMetadataHeadersAbsent(response);
    }
  }

  @Override
  void verifyGetHeadResponse(HttpResponse response, HttpHeaders expectedHeaders, ByteRange range, boolean isPrivate,
      String accountName, String containerName, NettyClient.ResponseParts responseParts) throws RestServiceException {
    if (!throttleRequest) {
      super.verifyGetHeadResponse(response, expectedHeaders, range, isPrivate, accountName, containerName,
          responseParts);
    } else {
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("Date header should be present", response.headers().contains(HttpHeaderNames.DATE));
      assertFalse("Last-Modified header should not be present",
          response.headers().contains(HttpHeaderNames.LAST_MODIFIED));
      assertFalse(RestUtils.Headers.BLOB_SIZE + "should not be present",
          response.headers().contains(RestUtils.Headers.BLOB_SIZE));
      assertFalse("Content-Range header should not be set",
          response.headers().contains(RestUtils.Headers.CONTENT_RANGE));
      assertFalse("Accept-Ranges should not be set", response.headers().contains(RestUtils.Headers.ACCEPT_RANGES));
      assertEquals(RestUtils.Headers.CONTENT_LENGTH + " should not be 0", 0, HttpUtil.getContentLength(response));
      assertFalse(RestUtils.Headers.LIFE_VERSION + " should not be present",
          response.headers().contains(RestUtils.Headers.LIFE_VERSION));
      verifyBlobPropertiesHeadersAbsent(response);
      verifyAccountAndContainerHeaders(null, null, response);
      assertNoContent(responseParts.queue, 1);
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
    }
  }

  @Override
  void verifyGetNotModifiedBlobResponse(HttpResponse response, boolean isPrivate,
      NettyClient.ResponseParts responseParts) {
    if (!throttleRequest) {
      super.verifyGetNotModifiedBlobResponse(response, isPrivate, responseParts);
    } else {
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("Date header should be present", response.headers().contains(HttpHeaderNames.DATE));
      assertFalse("Last-Modified header should not be present",
          response.headers().contains(HttpHeaderNames.LAST_MODIFIED));
      assertNull("Accept-Ranges should not be set", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      assertNull("Life-Version header should not be set", response.headers().get(RestUtils.Headers.LIFE_VERSION));
      assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
          response.headers().get(RestUtils.Headers.BLOB_SIZE));
      verifyTrackingHeaders(response);
      verifyCacheHeadersAbsent(response);
      assertNoContent(responseParts.queue, 1);
    }
  }

  @Override
  void verifyUserMetadataResponse(HttpResponse response, HttpHeaders expectedHeaders, byte[] usermetadata,
      NettyClient.ResponseParts responseParts) {
    if (!throttleRequest) {
      super.verifyUserMetadataResponse(response, expectedHeaders, usermetadata, responseParts);
    } else {
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      verifyTrackingHeaders(response);
      assertTrue("Date header should be present", response.headers().contains(HttpHeaderNames.DATE));
      assertFalse("Last-Modified header should not be present",
          response.headers().contains(HttpHeaderNames.LAST_MODIFIED));
      verifyUserMetadataHeadersAbsent(response);
      if (usermetadata == null) {
        assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
        assertNoContent(responseParts.queue, 1);
      }
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    }
  }

  @Override
  void verifyGetBlobInfoResponse(HttpResponse response, HttpHeaders expectedHeaders, boolean isPrivate,
      String accountName, String containerName, byte[] usermetadata, NettyClient.ResponseParts responseParts) {
    if (!throttleRequest) {
      super.verifyGetBlobInfoResponse(response, expectedHeaders, isPrivate, accountName, containerName, usermetadata,
          responseParts);
    } else {
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("Date header should be present", response.headers().contains(HttpHeaderNames.DATE));
      assertFalse("Last-Modified header should not be present",
          response.headers().contains(HttpHeaderNames.LAST_MODIFIED));
      verifyTrackingHeaders(response);
      verifyBlobPropertiesHeadersAbsent(response);
      verifyAccountAndContainerHeaders(null, null, response);
      verifyUserMetadataHeadersAbsent(response);
      if (usermetadata == null) {
        assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
        assertNoContent(responseParts.queue, 1);
      }
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      assertFalse(RestUtils.Headers.LIFE_VERSION + " should not be present",
          response.headers().contains(RestUtils.Headers.LIFE_VERSION));
    }
  }

  @Override
  void verifyUpdateBlobTtlResponse(NettyClient.ResponseParts responseParts) {
    if (!throttleRequest) {
      super.verifyUpdateBlobTtlResponse(responseParts);
    } else {
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      assertNoContent(responseParts.queue, 1);
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
    }
  }

  @Override
  void verifyDeleted(FullHttpRequest httpRequest, HttpResponseStatus expectedStatusCode)
      throws ExecutionException, InterruptedException {
    if (!throttleRequest) {
      super.verifyDeleted(httpRequest, expectedStatusCode);
    } else {
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertNotNull("No Date header", response.headers().get(HttpHeaderNames.DATE, null));
      assertNoContent(responseParts.queue, 1);
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
    }
  }

  @Override
  void verifyUndeleteBlobResponse(NettyClient.ResponseParts responseParts) {
    if (!throttleRequest) {
      super.verifyUndeleteBlobResponse(responseParts);
    } else {
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
      assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      assertNoContent(responseParts.queue, 1);
      assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
      verifyTrackingHeaders(response);
    }
  }

  /**
   * Verify tht cache headers are absent.
   * @param response {@link HttpResponse} object to get cache headers from.
   */
  private void verifyCacheHeadersAbsent(HttpResponse response) {
    assertNull("Cache-Control value should be null", response.headers().get(RestUtils.Headers.CACHE_CONTROL));
    assertFalse("Pragma value should not be present", response.headers().contains(RestUtils.Headers.PRAGMA));
    assertFalse("Expires value should not be present", response.headers().contains(RestUtils.Headers.EXPIRES));
  }

  /**
   * Verifies blob properties from output, to that sent in during input.
   * @param response the {@link HttpResponse} that contains the headers.
   */
  void verifyBlobPropertiesHeadersAbsent(HttpResponse response) {
    assertFalse("Blob size should not be present", response.headers().contains(RestUtils.Headers.BLOB_SIZE));
    assertFalse("There should be no " + RestUtils.Headers.PRIVATE,
        response.headers().contains(RestUtils.Headers.PRIVATE));
    assertFalse("There should be no " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        response.headers().contains(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertFalse("There should be no " + RestUtils.Headers.CREATION_TIME,
        response.headers().contains(RestUtils.Headers.CREATION_TIME));
    assertFalse("There should be no TTL in the response", response.headers().contains(RestUtils.Headers.TTL));
    assertFalse("There should be no " + RestUtils.Headers.OWNER_ID,
        response.headers().contains(RestUtils.Headers.OWNER_ID));
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param response the {@link HttpResponse} which contains the headers of the response.
   */
  void verifyUserMetadataHeadersAbsent(HttpResponse response) {
    for (Map.Entry<String, String> header : response.headers()) {
      String key = header.getKey();
      if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
        fail("Key " + key + " should not be present in headers");
      }
    }
  }

  static {
    try {
      TRUST_STORE_FILE = File.createTempFile("truststore", ".jks");
      CLUSTER_MAP = new MockClusterMap();
      TRUST_STORE_FILE.deleteOnExit();
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVProps(TRUST_STORE_FILE);
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
