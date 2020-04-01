/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountService;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


class GetAccountsHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GetAccountsHandler.class);

  private final SecurityService securityService;
  private final AccountService accountService;
  private final FrontendMetrics frontendMetrics;

  /**
   * Constructs a handler for handling requests for getting account metadata.
   * @param securityService the {@link SecurityService} to use.
   * @param accountService the {@link AccountService} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   */
  GetAccountsHandler(SecurityService securityService, AccountService accountService, FrontendMetrics frontendMetrics) {
    this.securityService = securityService;
    this.accountService = accountService;
    this.frontendMetrics = frontendMetrics;
  }

  /**
   * Asynchronously get account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#preProcessRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          frontendMetrics.getAccountsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      // Start the callback chain by performing request security processing.
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.getAccountsSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call the final callback with the response body to
     * sen
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.getAccountsSecurityPostProcessRequestMetrics, securityCheckResult -> {
        ReadableStreamChannel channel = serializeJsonToChannel(AccountCollectionSerde.toJson(getAccounts()));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
        finalCallback.onCompletion(channel, null);
      }, uri, LOGGER, finalCallback);
    }

    /**
     * Get the accounts to return in the response. This method will check the
     * {@link RestUtils.Headers#TARGET_ACCOUNT_ID} and {@link RestUtils.Headers#TARGET_ACCOUNT_NAME} request arguments.
     * If either of those arguments is set in the request, this will return the account with the respective id or name.
     * If neither is set in the request, this will return all accounts that this frontend knows about.
     * @return a {@link Collection} of {@link Account}s.
     * @throws RestServiceException If both the account ID and name arguments are set or if the requested account was
     *                              not found.
     */
    private Collection<Account> getAccounts() throws RestServiceException {
      Short id = RestUtils.getNumericalHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_ID, false,
          Short::parseShort);
      String name = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_NAME, false);
      Collection<Account> accounts;
      if (id != null && name != null) {
        throw new RestServiceException("Cannot supply both account ID and account name in request",
            RestServiceErrorCode.BadRequest);
      } else if (id == null && name == null) {
        accounts = accountService.getAllAccounts();
      } else {
        Account account = id != null ? accountService.getAccountById(id) : accountService.getAccountByName(name);
        if (account == null) {
          throw new RestServiceException("Account not found: " + (id != null ? "id=" + id : "name=" + name),
              RestServiceErrorCode.NotFound);
        }
        accounts = Collections.singleton(account);
      }
      return accounts;
    }
  }
}
