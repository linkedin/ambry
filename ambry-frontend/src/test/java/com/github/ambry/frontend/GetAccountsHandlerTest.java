/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.MigrationConfig;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingBiConsumer;
import com.github.ambry.utils.ThrowingConsumer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link GetAccountsHandler}.
 */
public class GetAccountsHandlerTest {
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final InMemAccountService accountService;
  private final GetAccountsHandler handler;
  private final FrontendMetrics metrics;

  public GetAccountsHandlerTest() {
    metrics =
        new FrontendMetrics(new MetricRegistry(), new FrontendConfig(new VerifiableProperties(new Properties())));
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    accountService = spy(new InMemAccountService(false, true));
    handler = new GetAccountsHandler(securityServiceFactory.getSecurityService(), accountService, metrics);
  }

  /**
   * Test valid request cases.
   * @throws Exception
   */
  @Test
  public void validRequestsTest() throws Exception {
    Account account = accountService.createAndAddRandomAccount();
    ThrowingBiConsumer<RestRequest, Collection<Account>> testAction = (request, expectedAccounts) -> {
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = sendRequestGetResponse(request, restResponseChannel);
      assertNotNull("There should be a response", channel);
      Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Content-length is not as expected", channel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      RetainingAsyncWritableChannel asyncWritableChannel = new RetainingAsyncWritableChannel((int) channel.getSize());
      channel.readInto(asyncWritableChannel, null).get();
      assertEquals("Accounts do not match", new HashSet<>(expectedAccounts), new HashSet<>(
          AccountCollectionSerde.accountsFromInputStreamInJson(asyncWritableChannel.consumeContentAsInputStream())));
    };
    testAction.accept(createRestRequest(null, null, null, Operations.ACCOUNTS), accountService.getAllAccounts());
    testAction.accept(createRestRequest(account.getName(), null, null, Operations.ACCOUNTS),
        Collections.singleton(account));
    testAction.accept(createRestRequest(null, Short.toString(account.getId()), null, Operations.ACCOUNTS),
        Collections.singleton(account));
  }

  /**
   * Test bad request cases.
   * @throws Exception
   */
  @Test
  public void badRequestsTest() throws Exception {
    Account existingAccount = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(Collections.singletonMap("DC-1", new MigrationConfig())).build();
    accountService.updateAccounts(Collections.singleton(existingAccount));
    Account nonExistentAccount = accountService.generateRandomAccount();
    ThrowingBiConsumer<RestRequest, RestServiceErrorCode> testAction = (request, expectedErrorCode) -> {
      TestUtils.assertException(RestServiceException.class,
          () -> sendRequestGetResponse(request, new MockRestResponseChannel()),
          e -> assertEquals("Unexpected error code", expectedErrorCode, e.getErrorCode()));
    };
    // cannot supply both ID and name
    testAction.accept(createRestRequest(existingAccount.getName(), Short.toString(existingAccount.getId()), null,
        Operations.ACCOUNTS), RestServiceErrorCode.BadRequest);
    // non-numerical ID
    testAction.accept(createRestRequest(null, "ABC", null, Operations.ACCOUNTS), RestServiceErrorCode.InvalidArgs);
    // account that doesn't exist
    testAction.accept(createRestRequest(nonExistentAccount.getName(), null, null, Operations.ACCOUNTS),
        RestServiceErrorCode.NotFound);
    testAction.accept(createRestRequest(null, Short.toString(nonExistentAccount.getId()), null, Operations.ACCOUNTS),
        RestServiceErrorCode.NotFound);
    assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
  }

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  @Test
  public void securityServiceDenialTest() throws Exception {
    Account account = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(Collections.singletonMap("DC-1", new MigrationConfig())).build();
    accountService.updateAccounts(Collections.singleton(account));
    IllegalStateException injectedException = new IllegalStateException("@@expected");
    TestUtils.ThrowingRunnable testAction =
        () -> sendRequestGetResponse(createRestRequest(null, null, null, Operations.ACCOUNTS),
            new MockRestResponseChannel());
    ThrowingConsumer<IllegalStateException> errorChecker = e -> assertEquals("Wrong exception", injectedException, e);
    securityServiceFactory.exceptionToReturn = injectedException;
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    TestUtils.assertException(IllegalStateException.class, testAction, errorChecker);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    TestUtils.assertException(IllegalStateException.class, testAction, errorChecker);
    securityServiceFactory.exceptionToThrow = injectedException;
    securityServiceFactory.exceptionToReturn = null;
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    TestUtils.assertException(IllegalStateException.class, testAction, errorChecker);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    TestUtils.assertException(IllegalStateException.class, testAction, errorChecker);
    assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
  }

  /**
   * Test success case of getting single container.
   * @throws Exception
   */
  @Test
  public void getSingleContainerSuccessTest() throws Exception {
    Account existingAccount = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(Collections.singletonMap("DC-1", new MigrationConfig())).build();
    accountService.updateAccounts(Collections.singleton(existingAccount));
    Container existingContainer = existingAccount.getAllContainers().iterator().next();
    ThrowingBiConsumer<RestRequest, Container> testAction = (request, expectedContainer) -> {
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = sendRequestGetResponse(request, restResponseChannel);
      assertNotNull("There should be a response", channel);
      Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Content-length is not as expected", channel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      RetainingAsyncWritableChannel asyncWritableChannel = new RetainingAsyncWritableChannel((int) channel.getSize());
      channel.readInto(asyncWritableChannel, null).get();
      assertEquals("Container does not match", Collections.singletonList(expectedContainer),
          AccountCollectionSerde.containersFromInputStreamInJson(asyncWritableChannel.consumeContentAsInputStream(),
              existingAccount.getId()));
    };
    testAction.accept(
        createRestRequest(existingAccount.getName(), null, existingContainer.getName(), Operations.ACCOUNTS_CONTAINERS),
        existingContainer);
    assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
  }

  @Test
  public void getAccountsWithoutContainersTest() throws Exception {
    Account account = accountService.createAndAddRandomAccount();
    ThrowingConsumer<RestRequest> testAction = (request) -> {
      // Set the header to ignore containers
      request.setArg(RestUtils.Headers.IGNORE_CONTAINERS, true);
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = sendRequestGetResponse(request, restResponseChannel);
      assertNotNull("There should be a response", channel);
      assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Content-length is not as expected", channel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      RetainingAsyncWritableChannel asyncWritableChannel = new RetainingAsyncWritableChannel((int) channel.getSize());
      channel.readInto(asyncWritableChannel, null).get();
      Account receivedAccount =
          AccountCollectionSerde.accountsFromInputStreamInJson(asyncWritableChannel.consumeContentAsInputStream())
              .iterator()
              .next();
      assertTrue("Accounts do not match", account.equalsWithoutContainers(receivedAccount));
      assertTrue("Containers should not be present", receivedAccount.getAllContainers().isEmpty());
    };
    testAction.accept(createRestRequest(account.getName(), null, null, Operations.ACCOUNTS));
    testAction.accept(createRestRequest(null, Short.toString(account.getId()), null, Operations.ACCOUNTS));
  }

  /**
   * Test that an account with migrationConfigs survives a GET round-trip.
   * @throws Exception
   */
  @Test
  public void getAccountWithMigrationConfigsTest() throws Exception {
    Map<String, MigrationConfig> migrationConfigs = new HashMap<>();
    migrationConfigs.put("DC-1", new MigrationConfig());
    migrationConfigs.put("DC-2", new MigrationConfig(true, new MigrationConfig.WriteRamp(),
        new MigrationConfig.ReadRamp(), new MigrationConfig.ListRamp()));
    Account account = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(migrationConfigs).build();
    accountService.updateAccounts(Collections.singleton(account));
    assertSame(metrics.nonEmptyMigrationConfigsResponseCount, metrics.getMetricRegistry().getCounters()
        .get(MetricRegistry.name(GetAccountsHandler.class, "NonEmptyMigrationConfigsResponseCount")));
    assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel channel =
        sendRequestGetResponse(createRestRequest(null, Short.toString(account.getId()), null, Operations.ACCOUNTS),
            restResponseChannel);
    assertNotNull("There should be a response", channel);
    RetainingAsyncWritableChannel asyncWritableChannel = new RetainingAsyncWritableChannel((int) channel.getSize());
    channel.readInto(asyncWritableChannel, null).get();
    Account receivedAccount =
        AccountCollectionSerde.accountsFromInputStreamInJson(asyncWritableChannel.consumeContentAsInputStream())
            .iterator().next();
    assertEquals("Account should match", account, receivedAccount);
    assertEquals("migrationConfigs should match", migrationConfigs, receivedAccount.getMigrationConfigs());
    assertEquals(1, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
  }

  @Test
  public void migrationConfigsResponseCountTest() throws Exception {
    Account first = accountService.createAndAddRandomAccount();
    Account second = accountService.createAndAddRandomAccount();
    long expectedCount = 0;
    for (Map<String, MigrationConfig> configs : Arrays.asList(null, Collections.<String, MigrationConfig>emptyMap(),
        Collections.singletonMap("DC-1", new MigrationConfig()))) {
      accountService.updateAccounts(Arrays.asList(new AccountBuilder(first).migrationConfigs(configs).build(),
          new AccountBuilder(second).migrationConfigs(configs).build()));
      for (boolean ignoreContainers : new boolean[]{false, true}) {
        for (RestRequest request : Arrays.asList(createRestRequest(null, null, null, Operations.ACCOUNTS),
            createRestRequest(first.getName(), null, null, Operations.ACCOUNTS),
            createRestRequest(null, Short.toString(first.getId()), null, Operations.ACCOUNTS))) {
          request.setArg(RestUtils.Headers.IGNORE_CONTAINERS, ignoreContainers);
          try (ReadableStreamChannel response = sendRequestGetResponse(request, new MockRestResponseChannel())) {
            assertNotNull(response);
            if (configs != null && !configs.isEmpty()) {
              expectedCount++;
            }
            assertEquals(expectedCount, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
          }
        }
      }
    }
  }

  @Test
  public void failedSerializationDoesNotCountMigrationConfigsTest() throws Exception {
    MigrationConfig config = new MigrationConfig() {
      @Override
      public WriteRamp getWriteRamp() {
        throw new IllegalStateException("injected serialization failure");
      }
    };
    Account account = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(Collections.singletonMap("DC-1", config)).build();
    accountService.updateAccounts(Collections.singleton(account));
    TestUtils.assertException(JsonMappingException.class,
        () -> sendRequestGetResponse(createRestRequest(null, null, null, Operations.ACCOUNTS),
            new MockRestResponseChannel()), null);
    assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
  }

  @Test
  public void migrationConfigsScanFailurePreservesResponseTest() throws Exception {
    Account account = new AccountBuilder(accountService.createAndAddRandomAccount())
        .migrationConfigs(Collections.singletonMap("DC-1", new MigrationConfig())).build();
    accountService.updateAccounts(Collections.singleton(account));
    List<Account> scannedAccounts = spy(new ArrayList<>(Collections.singleton(account)));
    doThrow(new ConcurrentModificationException("injected scan failure")).when(scannedAccounts).spliterator();
    when(accountService.getAllAccounts()).thenReturn(scannedAccounts);

    RestRequest request = createRestRequest(null, null, null, Operations.ACCOUNTS);
    try (ReadableStreamChannel response = sendRequestGetResponse(request, new MockRestResponseChannel());
        RetainingAsyncWritableChannel output = new RetainingAsyncWritableChannel((int) response.getSize())) {
      response.readInto(output, null).get();
      assertEquals(Collections.singletonList(account),
          AccountCollectionSerde.accountsFromInputStreamInJson(output.consumeContentAsInputStream()));
      assertEquals(0, metrics.nonEmptyMigrationConfigsResponseCount.getCount());
    }
    verify(scannedAccounts).spliterator();
    verify(accountService).getAllAccounts();
  }

  /**
   * Test failure case of getting single container.
   * @throws Exception
   */
  @Test
  public void getSingleContainerFailureTest() throws Exception {
    ThrowingBiConsumer<RestRequest, RestServiceErrorCode> testAction = (request, expectedErrorCode) -> {
      TestUtils.assertException(RestServiceException.class,
          () -> sendRequestGetResponse(request, new MockRestResponseChannel()),
          e -> assertEquals("Unexpected error code", expectedErrorCode, e.getErrorCode()));
    };
    // 1. invalid header (i.e. missing container name)
    testAction.accept(createRestRequest("test-account", null, null, Operations.ACCOUNTS_CONTAINERS),
        RestServiceErrorCode.MissingArgs);
    // 2. account not found
    testAction.accept(createRestRequest("fake-account", null, "fake-container", Operations.ACCOUNTS_CONTAINERS),
        RestServiceErrorCode.NotFound);
  }

  // helpers
  // general

  /**
   * Creates a {@link RestRequest} for a GET /accounts or /accounts/containers request
   * @param accountName if set, add this account name as a request header.
   * @param accountId if set, add this account ID as a request header.
   * @param containerName if set, add this container name as request header.
   * @param operation the operation this request will perform.
   * @return the {@link RestRequest}
   * @throws Exception
   */
  private RestRequest createRestRequest(String accountName, String accountId, String containerName, String operation)
      throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, operation);
    JSONObject headers = new JSONObject();
    if (accountName != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    }
    if (accountId != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_ID, accountId);
    }
    if (containerName != null) {
      headers.put(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
    }
    data.put(MockRestRequest.HEADERS_KEY, headers);
    RestRequest restRequest = new MockRestRequest(data, null);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(restRequest, null, null));
    return restRequest;
  }

  /**
   * Sends the given {@link RestRequest} to the {@link GetAccountsHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @return the response body as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    handler.handle(restRequest, restResponseChannel, future::done);
    try {
      return future.get(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : new Exception(e.getCause());
    }
  }
}
