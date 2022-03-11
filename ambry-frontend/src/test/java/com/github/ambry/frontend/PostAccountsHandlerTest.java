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
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.InMemAccountService;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class PostAccountsHandlerTest {
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final InMemAccountService accountService;
  private final PostAccountsHandler handler;

  public PostAccountsHandlerTest() {
    FrontendConfig frontendConfig = new FrontendConfig(new VerifiableProperties(new Properties()));
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    accountService = new InMemAccountService(false, true);
    handler =
        new PostAccountsHandler(securityServiceFactory.getSecurityService(), accountService, frontendConfig, metrics);
  }

  /**
   * Test valid request cases.
   * @throws Exception
   */
  @Test
  public void validRequestsTest() throws Exception {
    ThrowingConsumer<Collection<Account>> testAction = accountsToUpdate -> {
      String requestBody = new String(AccountCollectionSerde.serializeAccountsInJson(accountsToUpdate));
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      sendRequestGetResponse(requestBody, restResponseChannel);
      assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-length is not as expected", 0,
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      for (Account account : accountsToUpdate) {
        assertEquals("Account in account service not as expected", account,
            accountService.getAccountById(account.getId()));
      }
    };
    testAction.accept(Collections.emptyList());
    // add new account
    testAction.accept(Collections.singleton(accountService.generateRandomAccount()));
    // update multiple accounts
    testAction.accept(IntStream.range(0, 3).mapToObj(i -> {
      Account account = accountService.createAndAddRandomAccount();
      return new AccountBuilder(account).name(account.getName() + i).build();
    }).collect(Collectors.toList()));
  }

  /**
   * Test bad request cases.
   * @throws Exception
   */
  @Test
  public void badRequestsTest() throws Exception {
    ThrowingBiConsumer<String, RestServiceErrorCode> testAction = (requestBody, expectedErrorCode) -> {
      TestUtils.assertException(RestServiceException.class,
          () -> sendRequestGetResponse(requestBody, new MockRestResponseChannel()),
          e -> assertEquals("Unexpected error code", expectedErrorCode, e.getErrorCode()));
    };
    // non json input
    testAction.accept("ABC", RestServiceErrorCode.BadRequest);
    // invalid json
    testAction.accept(new JSONObject().append("accounts", "ABC").toString(), RestServiceErrorCode.BadRequest);
    // AccountService update failure
    accountService.setShouldUpdateSucceed(false);
    testAction.accept(new String(AccountCollectionSerde.serializeAccountsInJson(Collections.emptyList())),
        RestServiceErrorCode.InternalServerError);
  }

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  @Test
  public void securityServiceDenialTest() throws Exception {
    IllegalStateException injectedException = new IllegalStateException("@@expected");
    TestUtils.ThrowingRunnable testAction = () -> sendRequestGetResponse(
        new String(AccountCollectionSerde.serializeAccountsInJson(Collections.emptyList())),
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

  // helpers
  // general

  /**
   * Sends a request to the {@link PostAccountsHandler} and waits for the response.
   * @param requestBody body of the request in string form.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @throws Exception
   */
  private void sendRequestGetResponse(String requestBody, RestResponseChannel restResponseChannel) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.POST.name());
    data.put(MockRestRequest.URI_KEY, Operations.ACCOUNTS);
    List<ByteBuffer> body = new LinkedList<>();
    body.add(ByteBuffer.wrap(requestBody.getBytes(StandardCharsets.UTF_8)));
    body.add(null);
    RestRequest restRequest = new MockRestRequest(data, body);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(restRequest, null, null));
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    handler.handle(restRequest, restResponseChannel, future::done);
    try {
      future.get(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : new Exception(e.getCause());
    }
  }
}
