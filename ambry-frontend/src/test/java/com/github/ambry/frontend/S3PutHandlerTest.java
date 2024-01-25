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
import com.github.ambry.frontend.s3.S3PutHandler;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.named.NamedBlobRecord;
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


public class S3PutHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String S3_PREFIX = "/s3";
  private static final String SLASH = "/";
  private final Account account;
  private FrontendConfig frontendConfig;
  private S3PutHandler s3PutHandler;
  private NamedBlobDb namedBlobDb;

  public S3PutHandlerTest() throws Exception {
    account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    Container container = new ContainerBuilder().setName(Container.DEFAULT_S3_CONTAINER_NAME)
        .setId((short) 10)
        .setParentAccountId(account.getId())
        .setStatus(Container.ContainerStatus.ACTIVE)
        .setNamedBlobMode(Container.NamedBlobMode.OPTIONAL)
        .build();
    account.updateContainerMap(Collections.singletonList(container));
    setup();
  }

  @Test
  public void putBlobsTest() throws Exception {
    // 1. Put a s3 blob
    String accountName = account.getName();
    String containerName = Container.DEFAULT_S3_CONTAINER_NAME;
    String blobName = "MyDirectory/MyKey";
    int size = 1024;
    String uri = S3_PREFIX + SLASH + accountName + SLASH + blobName;
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(RestUtils.Headers.CONTENT_LENGTH, size);
    byte[] content = TestUtils.getRandomBytes(size);
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, uri, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    s3PutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();

    // 2. Verify upload was successful
    String blobId = (String) restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    assertEquals("Mismatch on response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertNotNull("Location header must be present", blobId);

    // 3. Verify blob name exists in named blob DB
    NamedBlobRecord namedBlobRecord = namedBlobDb.get(accountName, containerName, blobName).get();
    assertEquals("Mismatch in blob name to blob id mapping", blobId, namedBlobRecord.getBlobId());
  }

  /**
   * Initiates a {@link S3PutHandler}
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
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    NamedBlobPutHandler namedBlobPutHandler =
        new NamedBlobPutHandler(securityServiceFactory.getSecurityService(), namedBlobDb,
            ambryIdConverterFactory.getIdConverter(), idSigningService, router, injector, frontendConfig, metrics,
            CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    s3PutHandler = new S3PutHandler(namedBlobPutHandler);
  }
}
