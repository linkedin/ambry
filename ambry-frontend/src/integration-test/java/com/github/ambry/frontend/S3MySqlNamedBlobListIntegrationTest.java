/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
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
import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.Headers.*;
import static com.github.ambry.rest.RestUtils.*;
import static org.junit.Assert.*;


/**
 * Integration test that exercises the S3 empty-prefix LIST path end-to-end against a REAL MySQL backend under
 * {@code listNamedBlobsSqlOption=4}: HTTP -> Netty -> S3ListHandler -> NamedBlobPath.parseS3 (empty prefix collapses
 * to null) -> NamedBlobListHandler -> {@link com.github.ambry.named.MySqlNamedBlobDb#list} -> LIST_ALL (option 4).
 *
 * <p>This is the stitched coverage that {@code S3IntegrationTest#s3ListEmptyPrefixTest} could not provide (it uses
 * {@code InMemNamedBlobDbFactory}, so it never runs the SQL), and that the SQL-only int test
 * {@code MySqlNamedBlobDbListOperationIntegrationTest} could not provide (it never goes through the S3 handler). It is
 * the only layer that runs the exact path that returned HTTP 500 on large containers when LIST_ALL option 4 used a
 * window-function plan that materialized the whole container.
 *
 * <p>The CI {@code int-test} job provisions {@code localhost/AmbryNamedBlobs} (NamedBlobsSchema) and the {@code travis}
 * user, so this test runs there. When no such MySQL is reachable (e.g. local dev), it is skipped via {@link Assume}.
 */
public class S3MySqlNamedBlobListIntegrationTest extends FrontendIntegrationTestBase {
  // dbInfo "datacenter" MUST match clustermap.datacenter.name that S3IntegrationTest.buildFrontendVProps sets.
  private static final String DATA_CENTER_NAME = "localDc";
  private static final String NAMED_BLOB_DB_URL = "jdbc:mysql://localhost/AmbryNamedBlobs?serverTimezone=UTC";
  private static final String NAMED_BLOB_DB_USER = "travis";
  private static final String NAMED_BLOB_DB_PASSWORD = "";
  private static final String DB_INFO =
      "[{\"url\":\"" + NAMED_BLOB_DB_URL + "\",\"datacenter\":\"" + DATA_CENTER_NAME
          + "\",\"isWriteable\":\"true\",\"username\":\"" + NAMED_BLOB_DB_USER + "\",\"password\":\""
          + NAMED_BLOB_DB_PASSWORD + "\",\"sslMode\":\"NONE\"}]";

  private static final MockClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final VerifiableProperties SSL_CLIENT_VERIFIABLE_PROPS;
  private static final FrontendConfig FRONTEND_CONFIG;
  // InMemAccountServiceFactory returns a (returnOnlyUnknown, notifyConsumers)-keyed singleton, so this is the SAME
  // instance the frontend's account service resolves against -> the account created here is visible to MySqlNamedBlobDb.
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final Account ACCOUNT;
  private static RestServer ambryRestServer = null;
  private static NettyClient plaintextNettyClient = null;
  private static NettyClient sslNettyClient = null;
  private static boolean mysqlAvailable = false;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
      ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
      Properties mysqlProps = new Properties();
      mysqlProps.setProperty(MySqlNamedBlobDbConfig.DB_INFO, DB_INFO);
      // Exercise the option-4 LIST_ALL path end-to-end (the empty-prefix -> null-prefix LIST that 500'd on large containers).
      mysqlProps.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION,
          Integer.toString(MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION));
      FRONTEND_VERIFIABLE_PROPS = S3IntegrationTest.buildFrontendVPropsForQuota(trustStoreFile, ACCOUNT,
          "com.github.ambry.named.MySqlNamedBlobDbFactory", mysqlProps);
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  public S3MySqlNamedBlobListIntegrationTest() {
    super(FRONTEND_CONFIG, sslNettyClient);
  }

  @BeforeClass
  public static void setup() throws Exception {
    // Skip cleanly when no int-test MySQL is reachable (e.g. local dev without it). CI's int-test job provisions
    // localhost/AmbryNamedBlobs with the NamedBlobsSchema, so the test runs there.
    try (Connection ignored = DriverManager.getConnection(NAMED_BLOB_DB_URL, NAMED_BLOB_DB_USER,
        NAMED_BLOB_DB_PASSWORD)) {
      mysqlAvailable = true;
    } catch (SQLException e) {
      Assume.assumeNoException("MySQL (localhost/AmbryNamedBlobs) not reachable; skipping MySQL-backed S3 LIST test",
          e);
    }
    ambryRestServer = new RestServer(FRONTEND_VERIFIABLE_PROPS, CLUSTER_MAP, new LoggingNotificationSystem(),
        SSLFactory.getNewInstance(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    ambryRestServer.start();
    plaintextNettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
    sslNettyClient = new NettyClient("localhost", SSL_SERVER_PORT,
        SSLFactory.getNewInstance(new SSLConfig(SSL_CLIENT_VERIFIABLE_PROPS)));
  }

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
   * Clears any rows left from a prior run so the empty-prefix LIST assertions are deterministic across reruns.
   */
  @Before
  public void before() throws Exception {
    Assume.assumeTrue(mysqlAvailable);
    this.nettyClient = sslNettyClient;
    Container container = ACCOUNT.getAllContainers().iterator().next();
    try (Connection connection = DriverManager.getConnection(NAMED_BLOB_DB_URL, NAMED_BLOB_DB_USER,
        NAMED_BLOB_DB_PASSWORD); Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("DELETE FROM named_blobs_v2 WHERE account_id = %d AND container_id = %d",
          ACCOUNT.getId(), container.getId()));
    }
  }

  /**
   * PUT a few named blobs through the S3 stack, then issue an empty-prefix LIST (both v1 and v2 shapes) and assert
   * 200 OK end-to-end. With {@code listNamedBlobsSqlOption=4}, an empty prefix collapses to a null prefix and runs the
   * correlated-subquery LIST_ALL form against real MySQL. A regression in the S3-handler routing, the empty-prefix
   * collapse, or the option-4 LIST_ALL SQL/param binding surfaces here as a non-200 (or a 500 on the window plan).
   */
  @Test
  public void s3EmptyPrefixListAgainstMySqlOption4Test() throws Exception {
    String account = ACCOUNT.getName();
    Container container = ACCOUNT.getAllContainers().iterator().next();
    String containerName = container.getName();

    String[] keys = new String[]{"empty_prefix_mysql_a", "empty_prefix_mysql_b", "empty_prefix_mysql_c"};
    int contentSize = 64;
    for (String key : keys) {
      doPutBlob(account, containerName, key, contentSize, TestUtils.getRandomBytes(contentSize));
    }

    // V1 LIST: GET /s3/{account}/{container}?prefix=  (explicit empty value)
    String uriV1 = String.format("/s3/%s/%s?prefix=", account, containerName);
    FullHttpRequest reqV1 = buildRequest(HttpMethod.GET, uriV1, new DefaultHttpHeaders(), null);
    HttpResponse respV1 = getHttpResponse(nettyClient.sendRequest(reqV1, null, null).get());
    assertEquals("Empty-prefix LIST (v1) through the S3 stack against MySQL option-4 LIST_ALL must return 200 OK",
        HttpResponseStatus.OK, respV1.status());

    // V2 LIST: GET /s3/{account}/{container}?prefix=&list-type=2
    String uriV2 = String.format("/s3/%s/%s?prefix=&list-type=2", account, containerName);
    FullHttpRequest reqV2 = buildRequest(HttpMethod.GET, uriV2, new DefaultHttpHeaders(), null);
    HttpResponse respV2 = getHttpResponse(nettyClient.sendRequest(reqV2, null, null).get());
    assertEquals("Empty-prefix LIST (v2) through the S3 stack against MySQL option-4 LIST_ALL must return 200 OK",
        HttpResponseStatus.OK, respV2.status());
  }

  private void doPutBlob(String account, String container, String key, int contentSize, byte[] content)
      throws Exception {
    String uri = String.format("/s3/%s/%s/%s", account, container, key);
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(CONTENT_TYPE, OCTET_STREAM_CONTENT_TYPE);
    headers.add(CONTENT_LENGTH, contentSize);
    FullHttpRequest httpRequest = buildRequest(HttpMethod.PUT, uri, headers, ByteBuffer.wrap(content));
    HttpResponse response = getHttpResponse(nettyClient.sendRequest(httpRequest, null, null).get());
    assertEquals("Unexpected status putting seed blob " + key, HttpResponseStatus.OK, response.status());
  }
}
