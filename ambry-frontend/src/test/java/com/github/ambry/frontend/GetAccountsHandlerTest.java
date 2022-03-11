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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link GetAccountsHandler}.
 */
public class GetAccountsHandlerTest {
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final InMemAccountService accountService;
  private final GetAccountsHandler handler;

  public GetAccountsHandlerTest() {
    FrontendMetrics metrics =
        new FrontendMetrics(new MetricRegistry(), new FrontendConfig(new VerifiableProperties(new Properties())));
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    accountService = new InMemAccountService(false, true);
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
    Account existingAccount = accountService.createAndAddRandomAccount();
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
  }

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  @Test
  public void securityServiceDenialTest() throws Exception {
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
  }

  /**
   * Test success case of getting single container.
   * @throws Exception
   */
  @Test
  public void getSingleContainerSuccessTest() throws Exception {
    Account existingAccount = accountService.createAndAddRandomAccount();
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
