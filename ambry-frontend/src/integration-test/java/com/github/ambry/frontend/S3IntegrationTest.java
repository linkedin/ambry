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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapSnapshotConstants;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.NettyClient.ResponseParts;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;


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
  private static final String DATA_CENTER_NAME = "localDc";
  private static final String HOST_NAME = "localhost";
  private static final String CLUSTER_NAME = "Cluster-name";
  private static boolean enableUndeleteTested = false;
  private static RestServer ambryRestServer = null;
  private static NettyClient plaintextNettyClient = null;
  private static NettyClient sslNettyClient = null;
  private final boolean addClusterPrefix = true;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVProps(trustStoreFile);
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  /**
   * Constructor for {@link S3IntegrationTest}.
   */
  public S3IntegrationTest() {
    super(FRONTEND_CONFIG, sslNettyClient);
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
  public void flinkTest() {
    System.out.println("flink test");
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    return buildFrontendVProps(trustStoreFile, true, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT);
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param enableUndelete enable undelete in frontend when it's true.
   * @param plaintextServerPort server port number to support plaintext protocol
   * @param sslServerPort server port number to support ssl protocol
   * @return a {@link Properties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile, boolean enableUndelete,
      int plaintextServerPort, int sslServerPort) throws IOException, GeneralSecurityException {
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
    properties.setProperty(FrontendConfig.ENABLE_UNDELETE, Boolean.toString(enableUndelete));
    properties.setProperty(FrontendConfig.NAMED_BLOB_DB_FACTORY, "com.github.ambry.frontend.TestNamedBlobDbFactory");
    properties.setProperty(MySqlNamedBlobDbConfig.LIST_MAX_RESULTS, String.valueOf(NAMED_BLOB_LIST_RESULT_MAX));
    return new VerifiableProperties(properties);
  }

  /**
   * Upload data chunks using chunk upload signed URL.
   * @param account the {@link Account} to upload into.
   * @param container the {@link Container} to upload into.
   * @param chunkBlobTtl
   * @param chunkSizes The sizes for each data chunk to upload.
   * @return the list of signed chunk IDs for the uploaded chunks and an array containing the concatenated content of
   *         the data chunks.
   * @throws Exception
   */
  private Pair<List<String>, byte[]> uploadDataChunksAndVerify(Account account, Container container, Long chunkBlobTtl,
      int... chunkSizes) throws Exception {
    IdSigningService idSigningService = new AmbryIdSigningService();
    HttpHeaders chunkUploadHeaders = new DefaultHttpHeaders();
    chunkUploadHeaders.add(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    chunkUploadHeaders.add(RestUtils.Headers.CHUNK_UPLOAD, "true");
    setAmbryHeadersForPut(chunkUploadHeaders, chunkBlobTtl, !container.isCacheable(), "chunkUploader",
        "application/octet-stream", "stitchedUploadTest", account.getName(), container.getName());

    // POST
    // Get signed URL
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, Operations.GET_SIGNED_URL, chunkUploadHeaders, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    verifyTrackingHeaders(response);
    String signedPostUrl = response.headers().get(RestUtils.Headers.SIGNED_URL);
    assertNotNull("Did not get a signed POST URL", signedPostUrl);
    assertNoContent(responseParts.queue, 1);

    List<String> signedChunkIds = new ArrayList<>();
    ByteArrayOutputStream fullContentStream = new ByteArrayOutputStream();
    URI uri = new URI(signedPostUrl);
    for (int chunkSize : chunkSizes) {
      byte[] contentArray = TestUtils.getRandomBytes(chunkSize);
      ByteBuffer content = ByteBuffer.wrap(contentArray);
      // Use signed URL to POST
      httpRequest = buildRequest(HttpMethod.POST, uri.getPath() + "?" + uri.getQuery(), null, content);
      responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      String signedId = verifyPostAndReturnBlobId(responseParts, chunkSize, false);
      assertTrue("Blob ID for chunk upload must be signed", idSigningService.isIdSigned(signedId.substring(1)));
      Pair<String, Map<String, String>> idAndMetadata = idSigningService.parseSignedId(signedId.substring(1));
      // Inspect metadata fields
      String chunkUploadSession = idAndMetadata.getSecond().get(RestUtils.Headers.SESSION);
      assertNotNull("x-ambry-chunk-upload-session should be present in signed ID", chunkUploadSession);
      String blobSize = idAndMetadata.getSecond().get(RestUtils.Headers.BLOB_SIZE);
      assertNotNull("x-ambry-blob-size should be present in signed ID", blobSize);
      assertEquals("wrong size value in signed id", content.capacity(), Long.parseLong(blobSize));
      HttpHeaders expectedGetHeaders = new DefaultHttpHeaders().add(chunkUploadHeaders);
      // Use signed ID and blob ID for GET request
      expectedGetHeaders.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
      // Blob TTL for chunk upload default is chunkUploadInitialChunkTtlSecs if x-ambry-ttl == null.
      if (chunkBlobTtl == null) {
        expectedGetHeaders.set(RestUtils.Headers.TTL, FRONTEND_CONFIG.chunkUploadInitialChunkTtlSecs);
      } else {
        expectedGetHeaders.set(RestUtils.Headers.TTL, chunkBlobTtl);
      }
      expectedGetHeaders.set(RestUtils.Headers.LIFE_VERSION, "0");
      for (String id : new String[]{signedId, idAndMetadata.getFirst()}) {
        getBlobAndVerify(id, null, GetOption.None, false, expectedGetHeaders, !container.isCacheable(), content,
            account.getName(), container.getName(), container);
        getBlobInfoAndVerify(id, GetOption.None, expectedGetHeaders, !container.isCacheable(), account.getName(),
            container.getName(), null, container);
      }
      signedChunkIds.add(addClusterPrefix ? "/" + CLUSTER_NAME + signedId : signedId);
      fullContentStream.write(contentArray);
    }
    return new Pair<>(signedChunkIds, fullContentStream.toByteArray());
  }

  /**
   * Test the stitched upload flow for a specified chunk size and number of chunks.
   * @param account the {@link Account} to upload into.
   * @param container the {@link Container} to upload into.
   * @param signedChunkIds the list of signed chunk IDs to stitch together.
   * @param fullContentArray the content to compare the stitched blob against.
   * @throws Exception
   */
  private void stitchBlobAndVerify(Account account, Container container, List<String> signedChunkIds,
      byte[] fullContentArray, long stitchedBlobSize) throws Exception {
    // stitchBlob
    HttpHeaders stitchHeaders = new DefaultHttpHeaders();
    setAmbryHeadersForPut(stitchHeaders, TTL_SECS, !container.isCacheable(), "stitcher", "video/mp4",
        "stitchedUploadTest", account.getName(), container.getName());
    HttpRequest httpRequest = buildRequest(HttpMethod.POST, Operations.STITCH, stitchHeaders,
        ByteBuffer.wrap(StitchRequestSerDe.toJson(signedChunkIds).toString().getBytes(StandardCharsets.UTF_8)));
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    String stitchedBlobId = verifyPostAndReturnBlobId(responseParts, stitchedBlobSize, true);
    HttpHeaders expectedGetHeaders = new DefaultHttpHeaders().add(stitchHeaders);
    // Test different request types on stitched blob ID
    // (getBlobInfo, getBlob, getBlob w/ range, head, updateBlobTtl, deleteBlob)
    expectedGetHeaders.add(RestUtils.Headers.BLOB_SIZE, fullContentArray.length);
    expectedGetHeaders.set(RestUtils.Headers.LIFE_VERSION, "0");
    getBlobInfoAndVerify(stitchedBlobId, GetOption.None, expectedGetHeaders, !container.isCacheable(),
        account.getName(), container.getName(), null, container);
    List<ByteRange> ranges = new ArrayList<>();
    ranges.add(null);
    ranges.add(ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(fullContentArray.length + 1)));
    ranges.add(ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(fullContentArray.length)));
    long random1 = ThreadLocalRandom.current().nextLong(fullContentArray.length);
    long random2 = ThreadLocalRandom.current().nextLong(fullContentArray.length);
    ranges.add(ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2)));
    for (ByteRange range : ranges) {
      getBlobAndVerify(stitchedBlobId, range, GetOption.None, false, expectedGetHeaders, !container.isCacheable(),
          ByteBuffer.wrap(fullContentArray), account.getName(), container.getName(), container);
      getHeadAndVerify(stitchedBlobId, range, GetOption.None, expectedGetHeaders, !container.isCacheable(),
          account.getName(), container.getName());
    }
    updateBlobTtlAndVerify(stitchedBlobId, expectedGetHeaders, !container.isCacheable(), account.getName(),
        container.getName(), null, container);
    // Delete stitched blob.
    deleteBlobAndVerify(stitchedBlobId);
    verifyOperationsAfterDelete(stitchedBlobId, expectedGetHeaders, !container.isCacheable(), account.getName(),
        container.getName(), ByteBuffer.wrap(fullContentArray), null, false, container);
  }

  // accountApiTest() helpers

  /**
   * Call the {@code POST /accounts/updateContainers} API to update account metadata and verify that the update succeeded.
   * @param account the account in which to update containers.
   * @param containers the containers to update.
   */
  private void updateContainersAndVerify(Account account, Container... containers) throws Exception {
    byte[] containersUpdateJson = AccountCollectionSerde.serializeContainersInJson(Arrays.asList(containers));
    String accountName = account.getName();
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    FullHttpRequest request =
        buildRequest(HttpMethod.POST, Operations.ACCOUNTS_CONTAINERS, headers, ByteBuffer.wrap(containersUpdateJson));
    ResponseParts responseParts = nettyClient.sendRequest(request, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    // verify regular response header
    assertEquals("Unexpected account id in response header", account.getId(),
        Short.parseShort(response.headers().get(RestUtils.Headers.TARGET_ACCOUNT_ID)));
    assertEquals("Unexpected content type in response header", RestUtils.JSON_CONTENT_TYPE,
        response.headers().get(RestUtils.Headers.CONTENT_TYPE));
    verifyTrackingHeaders(response);
    ByteBuffer content = getContent(responseParts.queue, HttpUtil.getContentLength(response));
    Collection<Container> outputContainers =
        AccountCollectionSerde.containersFromInputStreamInJson(new ByteArrayInputStream(content.array()),
            account.getId());

    for (Container container : outputContainers) {
      assertEquals("Update not reflected in AccountService", container,
          ACCOUNT_SERVICE.getContainerByName(accountName, container.getName()));
    }
  }

  /**
   * Call the {@code GET /accounts} and {@code Get /accounts/containers} API and verify the response for all accounts
   * managed by {@link #ACCOUNT_SERVICE}.
   */
  private void verifyGetAccountsAndContainer() throws Exception {
    Collection<Account> expectedAccounts = ACCOUNT_SERVICE.getAllAccounts();
    // fetch snapshot of all accounts
    assertEquals("GET /accounts returned wrong result", new HashSet<>(expectedAccounts), getAccounts(null, null));
    // fetch accounts one by one
    for (Account account : expectedAccounts) {
      assertEquals("Fetching of single account by name failed", Collections.singleton(account),
          getAccounts(account.getName(), null));
      assertEquals("Fetching of single account by id failed", Collections.singleton(account),
          getAccounts(null, account.getId()));
    }
    // fetch container one by one from specific account
    Account account = expectedAccounts.iterator().next();
    for (Container container : account.getAllContainers()) {
      assertEquals("Mismatch in container", container, getContainer(account.getName(), container.getName()));
    }
  }

  /**
   * Get a container from given account.
   * @param accountName name of account which container belongs to.
   * @param containerName name of container
   * @return the requested container.
   * @throws Exception
   */
  private Container getContainer(String accountName, String containerName) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    headers.add(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);

    FullHttpRequest request = buildRequest(HttpMethod.GET, Operations.ACCOUNTS_CONTAINERS, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(request, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    verifyTrackingHeaders(response);
    short accountId = Short.parseShort(response.headers().get(RestUtils.Headers.TARGET_ACCOUNT_ID));
    ByteBuffer content = getContent(responseParts.queue, HttpUtil.getContentLength(response));
    return AccountCollectionSerde.containersFromInputStreamInJson(new ByteArrayInputStream(content.array()), accountId)
        .iterator()
        .next();
  }

  /**
   * Call the {@code GET /accounts} API and deserialize the response.
   * @param accountName if non-null, fetch a single account by name instead of all accounts.
   * @param accountId if non-null, fetch a single account by ID instead of all accounts.
   * @return the accounts fetched.
   */
  private Set<Account> getAccounts(String accountName, Short accountId) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (accountName != null) {
      headers.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    } else if (accountId != null) {
      headers.add(RestUtils.Headers.TARGET_ACCOUNT_ID, accountId);
    }
    FullHttpRequest request = buildRequest(HttpMethod.GET, Operations.ACCOUNTS, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(request, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    verifyTrackingHeaders(response);
    ByteBuffer content = getContent(responseParts.queue, HttpUtil.getContentLength(response));
    return new HashSet<>(
        AccountCollectionSerde.accountsFromInputStreamInJson(new ByteArrayInputStream(content.array())));
  }
}
