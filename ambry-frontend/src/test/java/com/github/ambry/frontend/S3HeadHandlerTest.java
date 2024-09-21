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
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3HeadHandler;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class S3HeadHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final String CONTENT_TYPE = "text/plain";
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String KEY_NAME = "directory-name/key_name";
  private static final int BLOB_SIZE = 1024;
  private static final int BEG = 200;
  private static final int END = 299;
  private final Account account;
  private final Container container;
  private FrontendConfig frontendConfig;
  private NamedBlobPutHandler namedBlobPutHandler;
  private S3HeadHandler s3HeadHandler;

  public S3HeadHandlerTest() throws Exception {
    account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    container = new ContainerBuilder().setName("container-a")
        .setId((short) 10)
        .setParentAccountId(account.getId())
        .setStatus(Container.ContainerStatus.ACTIVE)
        .setNamedBlobMode(Container.NamedBlobMode.OPTIONAL)
        .build();
    account.updateContainerMap(Collections.singletonList(container));
    setup();
    putABlob();
  }

  @Test
  public void headBucketTest() throws Exception {
    // 1. Head the bucket with range
    String uri = String.format("/s3/%s/%s/", account.getName(), container.getName());
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.HEAD, uri, new JSONObject(), null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> futureResult = new FutureResult<>();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, "ambry-test"));
    s3HeadHandler.handle(request, restResponseChannel, futureResult::done);

    // 2. Verify results
    assertNull(futureResult.get());
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
  }

  @Test
  public void headObjectTest() throws Exception {
    // 1. Head the object with range
    String uri = String.format("/s3/%s/%s/%s", account.getName(), container.getName(), KEY_NAME);
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.HEAD, uri, new JSONObject(), null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> futureResult = new FutureResult<>();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    s3HeadHandler.handle(request, restResponseChannel, futureResult::done);

    // 2. Verify results
    assertNull(futureResult.get());
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", "text/plain",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Mismatch in content length", BLOB_SIZE,
        Integer.parseInt(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH).toString()));
    assertEquals("Mismatch in accept range", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
  }

  @Test
  public void headObjectWithRangeTest() throws Exception {
    // 1. Head the object with range
    String uri = String.format("/s3/%s/%s/%s", account.getName(), container.getName(), KEY_NAME);
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.HEAD, uri, new JSONObject(), null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> futureResult = new FutureResult<>();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    request.setArg(RestUtils.Headers.RANGE, String.format("bytes=%d-%d", BEG, END));
    s3HeadHandler.handle(request, restResponseChannel, futureResult::done);

    // 2. Verify results
    assertNull(futureResult.get());
    assertEquals("Mismatch on status", ResponseStatus.PartialContent, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", "text/plain",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Mismatch in accept range", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    assertEquals("Mismatch in content length", END - BEG + 1,
        Integer.parseInt(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH).toString()));
    assertEquals("Mismatch in content range", String.format("bytes %d-%d/%d", BEG, END, BLOB_SIZE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
  }

  private void setup() throws Exception {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    AccountAndContainerInjector injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    IdSigningService idSigningService = new AmbryIdSigningService();
    AmbrySecurityServiceFactory securityServiceFactory = new AmbrySecurityServiceFactory(verifiableProperties,
        new MockClusterMap(), ACCOUNT_SERVICE, null, idSigningService, injector,
        QuotaTestUtils.createDummyQuotaManager());
    SecurityService securityService = securityServiceFactory.getSecurityService();
    NamedBlobDbFactory namedBlobDbFactory =
        new TestNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    NamedBlobDb namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, new MockClusterMap(), ambryIdConverterFactory);
    namedBlobPutHandler = new NamedBlobPutHandler(securityService, namedBlobDb,
        ambryIdConverterFactory.getIdConverter(), idSigningService, router, injector, frontendConfig, metrics,
        CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    HeadBlobHandler headBlobHandler = new HeadBlobHandler(frontendConfig, router,
        securityService, ambryIdConverterFactory.getIdConverter(), injector,
        metrics, new MockClusterMap(), QuotaTestUtils.createDummyQuotaManager());
    s3HeadHandler = new S3HeadHandler(headBlobHandler, securityService, metrics, ACCOUNT_SERVICE);
  }

  private void putABlob() throws Exception {
    String requestPath = String.format("/named/%s/%s/%s", account.getName(), container.getName(), KEY_NAME);
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
        "test-app", CONTENT_TYPE, "tester", null, null, null);
    byte[] content = TestUtils.getRandomBytes(BLOB_SIZE);
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, requestPath, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
  }
}
