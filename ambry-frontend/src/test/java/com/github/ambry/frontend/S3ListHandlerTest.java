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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.*;
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
  public void listS3BlobsTest() throws Exception {

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
        S3_PREFIX + SLASH + account.getName() + SLASH + container.getName() + SLASH + "?prefix=" + PREFIX
            + "&delimiter=/" + "&max-keys=1" + "&encoding-type=url";
    request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.GET, s3_list_request_uri, new JSONObject(), null);
    request.setArg(InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3ListHandler.handle(request, restResponseChannel, futureResult::done);

    // 3. Verify results
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    ListBucketResult listBucketResult =
        xmlMapper.readValue(byteBuffer.array(), ListBucketResult.class);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Mismatch in content type", XML_CONTENT_TYPE,
        restResponseChannel.getHeader(Headers.CONTENT_TYPE));
    Contents contents = listBucketResult.getContents().get(0);
    assertEquals("Mismatch in key name", KEY_NAME, contents.getKey());
    assertEquals("Mismatch in key count", 1, listBucketResult.getKeyCount());
    assertEquals("Mismatch in prefix", PREFIX, listBucketResult.getPrefix());
    assertEquals("Mismatch in delimiter", "/", listBucketResult.getDelimiter());
    assertEquals("Mismatch in max key count", 1, listBucketResult.getMaxKeys());
    assertEquals("Mismatch in encoding type", "url", listBucketResult.getEncodingType());
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
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, new MockClusterMap());
    AccountAndContainerInjector injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    IdSigningService idSigningService = new AmbryIdSigningService();
    FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
    NamedBlobDbFactory namedBlobDbFactory =
        new TestNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    NamedBlobDb namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    namedBlobPutHandler = new NamedBlobPutHandler(securityServiceFactory.getSecurityService(), namedBlobDb,
        ambryIdConverterFactory.getIdConverter(), idSigningService, router, injector, frontendConfig, metrics,
        CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    NamedBlobListHandler namedBlobListHandler =
        new NamedBlobListHandler(securityServiceFactory.getSecurityService(), namedBlobDb, injector, metrics);
    s3ListHandler = new S3ListHandler(namedBlobListHandler, metrics);
  }
}
