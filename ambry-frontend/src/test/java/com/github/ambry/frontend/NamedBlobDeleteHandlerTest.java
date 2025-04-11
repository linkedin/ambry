/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.InMemNamedBlobDbFactory;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class for {@link DeleteBlobHandler} for named blob part. Some functions of DeleteBlobHandler are
 * tested in {@link FrontendRestRequestServiceTest}. For now, this class is only testing deletes on
 * multiple versions of named blobs.
 */
public class NamedBlobDeleteHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final Account REF_ACCOUNT;
  private static final Container REF_CONTAINER;
  private static final Container REF_CONTAINER_WITH_TTL_REQUIRED;
  private static final Container REF_CONTAINER_NO_UPDATE;
  private static final ClusterMap CLUSTER_MAP;

  private static final int TIMEOUT_SECS = 10;
  private static final String SERVICE_ID = "test-app";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String OWNER_ID = "tester";
  private static final String CONVERTED_ID = "/abcdef";
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String NAMED_BLOB_PREFIX = "/named";
  private static final String SLASH = "/";
  private static final String BLOBNAME = "ambry_blob_name";

  private final NamedBlobDb namedBlobDb;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      Account account = ACCOUNT_SERVICE.createAndAddRandomAccount();
      Container publicContainer = account.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
      Container ttlRequiredContainer = new ContainerBuilder(publicContainer).setTtlRequired(true).build();
      Account newAccount = new AccountBuilder(account).addOrUpdateContainer(ttlRequiredContainer).build();
      ACCOUNT_SERVICE.updateAccounts(Collections.singleton(newAccount));
      REF_ACCOUNT = ACCOUNT_SERVICE.getAccountById(newAccount.getId());
      REF_CONTAINER_NO_UPDATE = new ContainerBuilder((short) 11, "noUpdate", Container.ContainerStatus.ACTIVE, "",
          REF_ACCOUNT.getId()).setNamedBlobMode(Container.NamedBlobMode.NO_UPDATE).build();
      ACCOUNT_SERVICE.updateContainers(REF_ACCOUNT.getName(), Arrays.asList(REF_CONTAINER_NO_UPDATE));
      REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
      REF_CONTAINER_WITH_TTL_REQUIRED = REF_ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final MockTime time = new MockTime();
  private final FrontendMetrics metrics;
  private final InMemoryRouter router;
  private final AmbryIdConverterFactory idConverterFactory;
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final AccountAndContainerInjector injector;
  private final IdSigningService idSigningService;
  private FrontendConfig frontendConfig;
  private NamedBlobPutHandler namedBlobPutHandler; // use put handler to upload
  private DeleteBlobHandler deleteBlobHandler;
  private final String request_path;

  public NamedBlobDeleteHandlerTest() throws Exception {
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    Properties props = new Properties();
    CommonTestUtils.populateRequiredRouterProps(props);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    idSigningService = new AmbryIdSigningService();
    NamedBlobDbFactory namedBlobDbFactory =
        new InMemNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    idConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, CLUSTER_MAP.getMetricRegistry(), idSigningService,
            namedBlobDb);
    router = new InMemoryRouter(verifiableProperties, CLUSTER_MAP, idConverterFactory);
    frontendConfig = new FrontendConfig(verifiableProperties);
    metrics = new FrontendMetrics(CLUSTER_MAP.getMetricRegistry(), frontendConfig);
    injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    request_path =
        NAMED_BLOB_PREFIX + SLASH + REF_ACCOUNT.getName() + SLASH + REF_CONTAINER.getName() + SLASH + BLOBNAME;
    SecurityService securityService = securityServiceFactory.getSecurityService();
    namedBlobPutHandler =
        new NamedBlobPutHandler(securityService, namedBlobDb, idConverterFactory.getIdConverter(), idSigningService,
            router, injector, frontendConfig, metrics, CLUSTER_NAME, QuotaTestUtils.createDummyQuotaManager(),
            ACCOUNT_SERVICE, null);
    deleteBlobHandler =
        new DeleteBlobHandler(router, securityServiceFactory.getSecurityService(), idConverterFactory.getIdConverter(),
            injector, metrics, CLUSTER_MAP, QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE);
  }

  @Test
  public void deleteMultipleVersionsTest() throws Exception {
    // Upload 5 versions of the same named blob
    final int NUM_VERSIONS = 5;
    List<String> blobIds = new ArrayList<>(NUM_VERSIONS);
    for (int i = 0; i < NUM_VERSIONS; i++) {
      blobIds.add(uploadOneVersion());
    }
    // Delete this named blob, it should delete all versions
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.SERVICE_ID, SERVICE_ID);
    RestRequest request = getDeleteRestRequest(headers, request_path);

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    deleteBlobHandler.handle(request, restResponseChannel, future::done);
    future.get(TIMEOUT_SECS, TimeUnit.SECONDS);

    for (String blobId : blobIds) {
      // All version of blob ids should be deleted
      assertTrue(router.getDeletedBlobs().contains(blobId));
    }
  }

  /**
   * Upload a new version for this named blob and return the blob id.
   * @return The blob id for the new version
   * @throws Exception
   */
  private String uploadOneVersion() throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, !REF_CONTAINER.isCacheable(),
        SERVICE_ID, CONTENT_TYPE, OWNER_ID, null, REF_CONTAINER_NO_UPDATE.getName(), null);
    headers.put(RestUtils.Headers.NAMED_UPSERT, true);
    byte[] content = TestUtils.getRandomBytes(1024);
    RestRequest request = getPutRestRequest(headers, request_path, content);

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, future::done);

    future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
    // in id converter, the callback is invoked before the lastConvertedId is set, we need to wait
    String location = (String) restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    String blobId =
        RequestPath.parse(location, Collections.emptyMap(), frontendConfig.pathPrefixesToRemove, CLUSTER_NAME)
            .getOperationOrBlobId(true);
    InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(blobId);
    assertNotNull(blob);
    assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());
    return blobId;
  }

  /**
   * Create a Put RestRequest for the given path and request body.
   * @param headers The http headers of the request.
   * @param path The URI path of the request.
   * @param requestBody The request body.
   * @return
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   * @throws RestServiceException
   */
  private RestRequest getPutRestRequest(JSONObject headers, String path, byte[] requestBody)
      throws UnsupportedEncodingException, URISyntaxException, RestServiceException {
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, path, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(requestBody), null)));
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    return request;
  }

  /**
   * Create a Delete RestRequest for the given path.
   * @param headers The http headers of the request.
   * @param path The URI path of the request.
   * @return
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   * @throws RestServiceException
   */
  private RestRequest getDeleteRestRequest(JSONObject headers, String path)
      throws UnsupportedEncodingException, URISyntaxException, RestServiceException {
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.DELETE, path, headers, null);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    return request;
  }
}
