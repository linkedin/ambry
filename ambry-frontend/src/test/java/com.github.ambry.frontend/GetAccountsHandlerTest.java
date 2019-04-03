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
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingBiConsumer;
import com.github.ambry.utils.ThrowingConsumer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
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
      assertEquals("Accounts do not match", new HashSet<>(expectedAccounts),
          new HashSet<>(AccountCollectionSerde.fromJson(RestTestUtils.getJsonizedResponseBody(channel))));
    };
    testAction.accept(createRestRequest(null, null), accountService.getAllAccounts());
    testAction.accept(createRestRequest(account.getName(), null), Collections.singleton(account));
    testAction.accept(createRestRequest(null, Short.toString(account.getId())), Collections.singleton(account));
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
    testAction.accept(createRestRequest(existingAccount.getName(), Short.toString(existingAccount.getId())),
        RestServiceErrorCode.BadRequest);
    // non-numerical ID
    testAction.accept(createRestRequest(null, "ABC"), RestServiceErrorCode.InvalidArgs);
    // account that doesn't exist
    testAction.accept(createRestRequest(nonExistentAccount.getName(), null), RestServiceErrorCode.NotFound);
    testAction.accept(createRestRequest(null, Short.toString(nonExistentAccount.getId())),
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
        () -> sendRequestGetResponse(createRestRequest(null, null), new MockRestResponseChannel());
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

  // helpers
  // general

  /**
   * Creates a {@link RestRequest} for a GET /accounts request
   * @param accountName if set, add this account name as a request header.
   * @param accountId if set, add this account ID as a request header.
   * @return the {@link RestRequest}
   * @throws Exception
   */
  private RestRequest createRestRequest(String accountName, String accountId) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, Operations.ACCOUNTS);
    JSONObject headers = new JSONObject();
    if (accountName != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    }
    if (accountId != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_ID, accountId);
    }
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data, null);
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
