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
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import java.util.Collection;
import java.util.GregorianCalendar;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


/**
 * Handle requests to create or update accounts using {@link AccountService}.
 */
class PostAccountsHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostAccountsHandler.class);

  private final SecurityService securityService;
  private final AccountService accountService;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;

  /**
   * Constructs a handler for handling requests updating account metadata.
   * @param securityService the {@link SecurityService} to use.
   * @param accountService the {@link AccountService} to use.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   */
  PostAccountsHandler(SecurityService securityService, AccountService accountService, FrontendConfig frontendConfig,
      FrontendMetrics frontendMetrics) {
    this.securityService = securityService;
    this.accountService = accountService;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
  }

  /**
   * Asynchronously update account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          frontendMetrics.postAccountsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
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
      return buildCallback(frontendMetrics.postAccountsSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, read the request body content.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.postAccountsSecurityPostProcessRequestMetrics, securityCheckResult -> {
        RetainingAsyncWritableChannel channel =
            new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
        restRequest.readInto(channel, fetchAccountUpdateBodyCallback(channel));
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After reading the body of the account update request, call {@link AccountService#updateAccounts}.
     * @param channel the {@link RetainingAsyncWritableChannel} to read data out of.
     * @return a {@link Callback} to be used with {@link RestRequest#readInto}.
     */
    private Callback<Long> fetchAccountUpdateBodyCallback(RetainingAsyncWritableChannel channel) {
      return buildCallback(frontendMetrics.postAccountsReadRequestMetrics, bytesRead -> {
        updateAccounts(readJsonFromChannel(channel));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        finalCallback.onCompletion(null, null);
      }, uri, LOGGER, finalCallback);
    }

    /**
     * Process the request json and call {@link AccountService#updateAccounts} to update accounts.
     * @param accountUpdateJson the request json containing the accounts to update.
     * @throws RestServiceException
     */
    private void updateAccounts(JSONObject accountUpdateJson) throws RestServiceException {
      Collection<Account> accountsToUpdate;
      try {
        accountsToUpdate = AccountCollectionSerde.fromJson(accountUpdateJson);
      } catch (JSONException e) {
        throw new RestServiceException("Bad account update request body", e, RestServiceErrorCode.BadRequest);
      }
      if (!accountService.updateAccounts(accountsToUpdate)) {
        throw new RestServiceException("Account update failed", RestServiceErrorCode.BadRequest);
      }
    }
  }
}
