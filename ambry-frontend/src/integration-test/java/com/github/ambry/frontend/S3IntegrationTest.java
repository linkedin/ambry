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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3MessagePayload;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.utils.TestUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.frontend.s3.S3MessagePayload.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static com.github.ambry.rest.RestUtils.*;
import static org.junit.Assert.*;


/**
 * Integration tests for Ambry S3 support.
 */
public class S3IntegrationTest extends FrontendIntegrationTestBase {
  private static final MockClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final VerifiableProperties SSL_CLIENT_VERIFIABLE_PROPS;
  private static final FrontendConfig FRONTEND_CONFIG;
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final Account ACCOUNT;
  private static final String DATA_CENTER_NAME = "localDc";
  private static final String HOST_NAME = "localhost";
  private static final String CLUSTER_NAME = "Cluster-name";
  private static RestServer ambryRestServer = null;
  private static NettyClient plaintextNettyClient = null;
  private static NettyClient sslNettyClient = null;
  private final ObjectMapper xmlMapper;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
      ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVPropsForQuota(trustStoreFile, ACCOUNT);
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  /**
   * Constructor for {@link S3IntegrationTest}.
   */
  public S3IntegrationTest() {
    super(FRONTEND_CONFIG, sslNettyClient);
    xmlMapper = new XmlMapper();
  }

  /**
   * Sets up an Ambry frontend server.
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    ambryRestServer = new RestServer(FRONTEND_VERIFIABLE_PROPS, CLUSTER_MAP, new LoggingNotificationSystem(),
        SSLFactory.getNewInstance(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    ambryRestServer.start();
    plaintextNettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
    sslNettyClient = new NettyClient("localhost", SSL_SERVER_PORT,
        SSLFactory.getNewInstance(new SSLConfig(SSL_CLIENT_VERIFIABLE_PROPS)));
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

  @Test
  public void multipartUploadTest() throws Exception {
    for (Container container : ACCOUNT.getAllContainers()) {
      int partCount = TestUtils.RANDOM.nextInt(15) + 1;
      int partSize = TestUtils.RANDOM.nextInt(1024) + 1;
      doMultipartUploadTest(ACCOUNT, container, partCount, partSize);
    }
  }

  private void doMultipartUploadTest(Account account, Container container, int partCount, int partSize)
      throws Exception {
    // Prepare data
    byte[][] contents = new byte[partCount][partSize];
    for (int i = 0; i < partCount; i++) {
      contents[i] = TestUtils.getRandomBytes(partSize);
    }
    String[] eTags = new String[partCount];
    String[] locations = new String[partCount];

    // Prepare account etc.
    String endpoint = String.format("/s3/%s", account.getName());
    String bucket = container.getName();
    String key = "1/2/3/4/5";
    String uploadId = null;

    {
      // 1. Initiate multipart upload
      String uri =  String.format("%s/%s/%s?uploads", endpoint, bucket, key);
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.add(CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
      headers.add(CONTENT_LENGTH, 0);
      FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, uri, headers, null);
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected status", HttpResponseStatus.OK, response.status());
      assertEquals("Unexpected content type", XML_CONTENT_TYPE, response.headers().get(CONTENT_TYPE));
      InitiateMultipartUploadResult initUploadResult = xmlMapper.readValue(
          getContent(responseParts.queue, HttpUtil.getContentLength(response)).array(),
          S3MessagePayload.InitiateMultipartUploadResult.class);
      assertEquals("Unexpected bucket", bucket, initUploadResult.getBucket());
      assertEquals("Unexpected key", key, initUploadResult.getKey());
      uploadId = initUploadResult.getUploadId();
      assertNotNull("uploadid not found", uploadId);
    } {
      // 2. Upload parts
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.add(CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
      headers.add(CONTENT_LENGTH, partSize);
      for (int i = 0; i < contents.length; i++) {
        String uri = String.format("%s/%s/%s?uploadId=%s&partNumber=%d", endpoint, bucket, key, uploadId, i + 1);
        FullHttpRequest httpRequest = buildRequest(HttpMethod.PUT, uri, headers, ByteBuffer.wrap(contents[i]));
        NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
        HttpResponse response = getHttpResponse(responseParts);
        eTags[i] = response.headers().get(ETAG);
        locations[i] = response.headers().get(LOCATION);
        assertEquals("Unexpected status", HttpResponseStatus.OK, response.status());
        assertNotNull("Etag not found", eTags[i]);
        assertNotNull("Location not found", locations[i]);
      }
    } {
      // 3. Complete multipart upload
      String uri =  String.format("%s/%s/%s?uploadId=%s", endpoint, bucket, key, uploadId);
      Part[] parts = new Part[contents.length];
      for (int i = 0; i < contents.length; i++) {
        parts[i] = new Part(String.valueOf(i + 1), eTags[i]);
      }
      CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload(parts);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      xmlMapper.writeValue(byteArrayOutputStream, completeMultipartUpload);
      String completeMultipartStr = byteArrayOutputStream.toString();
      byte[] content = completeMultipartStr.getBytes(StandardCharsets.UTF_8);
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.add(CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
      headers.add(CONTENT_LENGTH, content.length);
      FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, uri, headers, ByteBuffer.wrap(content));
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      CompleteMultipartUploadResult uploadResult = xmlMapper.readValue(
          getContent(responseParts.queue, HttpUtil.getContentLength(response)).array(),
          S3MessagePayload.CompleteMultipartUploadResult.class);
      assertEquals("Unexpected status", HttpResponseStatus.OK, response.status());
      assertEquals("Unexpected content type", XML_CONTENT_TYPE, response.headers().get(CONTENT_TYPE));
      assertEquals("Unexpected bucket", bucket, uploadResult.getBucket());
      assertEquals("Unexpected key", key, uploadResult.getKey());
      assertTrue("Unexpected location", uri.startsWith(uploadResult.getLocation()));
      assertNotNull("ETag not found", uploadResult.geteTag());
    } {
      // 4. Get the blob and verify content
      String uri = String.format("%s/%s/%s", endpoint, bucket, key);
      HttpHeaders headers = new DefaultHttpHeaders();
      FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, uri, headers, null);
      NettyClient.ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected status", HttpResponseStatus.OK, response.status());
      assertEquals("Unexpected content type", OCTET_STREAM_CONTENT_TYPE, response.headers().get(CONTENT_TYPE));
      long expectedContentLength = partCount * partSize;
      if (expectedContentLength <= frontendConfig.chunkedGetResponseThresholdInBytes) {
        int contentLength = response.headers().getInt(CONTENT_LENGTH);
        assertEquals("Unexpected content length", expectedContentLength, contentLength);
      }
      byte[] content = getContent(responseParts.queue, expectedContentLength).array();
      ByteBuffer expectedContent = ByteBuffer.wrap(new byte[partCount * partSize]);
      Arrays.stream(contents).forEach(expectedContent::put);
      assertArrayEquals("Unexpected content", expectedContent.array(), content);
    }
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param account {@link Account} for which quota needs to be specified.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVPropsForQuota(File trustStoreFile, Account account)
      throws IOException, GeneralSecurityException {
    Properties properties = buildFrontendVProps(trustStoreFile);
    JSONObject cuResourceQuotaJson = new JSONObject();
    JSONObject quotaJson = new JSONObject();
    quotaJson.put("rcu", 10737418240L);
    quotaJson.put("wcu", 10737418240L);
    cuResourceQuotaJson.put(Integer.toString(account.getId()), quotaJson);
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, cuResourceQuotaJson.toString());
    properties.setProperty(QuotaConfig.THROTTLING_MODE, QuotaMode.TRACKING.name());
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, String.valueOf(true));
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON,
        "{\n" + "  \"rcu\": 10240,\n" + "  \"wcu\": 10240\n" + "}");
    return new VerifiableProperties(properties);
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link Properties} with the parameters for an Ambry frontend server.
   */
  private static Properties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.put("rest.server.rest.request.service.factory",
        "com.github.ambry.frontend.FrontendRestRequestServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("rest.server.account.service.factory", "com.github.ambry.account.InMemAccountServiceFactory");
    properties.put("netty.server.port", Integer.toString(PLAINTEXT_SERVER_PORT));
    properties.put("netty.server.ssl.port", Integer.toString(SSL_SERVER_PORT));
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
    properties.setProperty(FrontendConfig.ENABLE_UNDELETE, Boolean.toString(true));
    properties.setProperty(FrontendConfig.NAMED_BLOB_DB_FACTORY, "com.github.ambry.frontend.TestNamedBlobDbFactory");
    properties.setProperty(MySqlNamedBlobDbConfig.LIST_MAX_RESULTS, String.valueOf(NAMED_BLOB_LIST_RESULT_MAX));
    return properties;
  }
}
