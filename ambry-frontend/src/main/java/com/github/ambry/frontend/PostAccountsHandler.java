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
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.Dataset;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.Operations.*;


/**
 * Handle requests to create or update accounts using {@link AccountService}.
 */
class PostAccountsHandler {
  private static final Logger logger = LoggerFactory.getLogger(PostAccountsHandler.class);

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
          uri, logger, finalCallback);
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
      }, uri, logger, finalCallback);
    }

    /**
     * After reading the body of the account update request, call {@link AccountService#updateAccounts}.
     * @param channel the {@link RetainingAsyncWritableChannel} to read data out of.
     * @return a {@link Callback} to be used with {@link RestRequest#readInto}.
     */
    private Callback<Long> fetchAccountUpdateBodyCallback(RetainingAsyncWritableChannel channel) {
      return buildCallback(frontendMetrics.postAccountsReadRequestMetrics, bytesRead -> {
        ReadableStreamChannel outputChannel;
        if (RestUtils.getRequestPath(restRequest).matchesOperation(ACCOUNTS_CONTAINERS_DATASETS)) {
          logger.debug("Got request for {} with payload", ACCOUNTS_CONTAINERS_DATASETS);
          addDataset(channel);
          outputChannel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
        } else if (RestUtils.getRequestPath(restRequest).matchesOperation(ACCOUNTS_CONTAINERS)) {
          logger.debug("Got request for {} with payload", ACCOUNTS_CONTAINERS);
          Pair<Account, Collection<Container>> accountAndUpdatedContainers = updateContainers(channel);
          outputChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(
              AccountCollectionSerde.serializeContainersInJson(accountAndUpdatedContainers.getSecond())));
          restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_ID,
              accountAndUpdatedContainers.getFirst().getId());
        } else {
          updateAccounts(channel);
          outputChannel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
        }
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, outputChannel.getSize());
        finalCallback.onCompletion(outputChannel, null);
      }, uri, logger, finalCallback);
    }

    /**
     * Process the request json and call {@link AccountService#updateContainers} to add or update containers.
     * @param channel The {@link RetainingAsyncWritableChannel} to read the bytes out
     * @return a pair of account and its updated containers.
     * @throws RestServiceException
     */
    private Pair<Account, Collection<Container>> updateContainers(RetainingAsyncWritableChannel channel)
        throws RestServiceException {
      Short accountId = RestUtils.getNumericalHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_ID, false,
          Short::parseShort);
      String accountName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_NAME, false);
      if (accountId == null && accountName == null) {
        throw new RestServiceException("Missing required header: " + RestUtils.Headers.TARGET_ACCOUNT_NAME,
            RestServiceErrorCode.BadRequest);
      }
      Account account =
          accountName != null ? accountService.getAccountByName(accountName) : accountService.getAccountById(accountId);
      if (account == null) {
        throw new RestServiceException("Account not found: " + accountName, RestServiceErrorCode.NotFound);
      }
      accountId = account.getId();
      accountName = account.getName();

      Collection<Container> containersToUpdate;
      try {
        containersToUpdate =
            AccountCollectionSerde.containersFromInputStreamInJson(channel.consumeContentAsInputStream(),
                accountId);
      } catch (IOException e) {
        throw new RestServiceException("Bad container update request body", e, RestServiceErrorCode.BadRequest);
      }
      try {
        Collection<Container> updatedContainers = accountService.updateContainers(accountName, containersToUpdate);
        return new Pair<>(account, updatedContainers);
      } catch (AccountServiceException ex) {
        throw new RestServiceException("Container update failed for accountId " + accountId,
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }

    /**
     * Process the request json and call {@link AccountService#updateAccounts} to update accounts.
     * @param channel The {@link RetainingAsyncWritableChannel} to read the bytes out
     * @throws RestServiceException
     */
    private void updateAccounts(RetainingAsyncWritableChannel channel) throws RestServiceException {
      Collection<Account> accountsToUpdate;
      try {
        accountsToUpdate = AccountCollectionSerde.accountsFromInputStreamInJson(channel.consumeContentAsInputStream());
      } catch (IOException e) {
        throw new RestServiceException("Bad account update request body", e, RestServiceErrorCode.BadRequest);
      }
      try {
        accountService.updateAccounts(accountsToUpdate);
      } catch (AccountServiceException ase) {
        throw new RestServiceException("Account update failed: " + ase.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ase.getErrorCode()));
      }
    }

    /**
     * Process the request json and call {@link AccountService#addDataset} to add the dataset.
     * @param channel The {@link RetainingAsyncWritableChannel} to read the bytes out
     * @throws RestServiceException
     */
    private void addDataset(RetainingAsyncWritableChannel channel) throws RestServiceException {
      Dataset datasetToUpdate;
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      try {
        datasetToUpdate = AccountCollectionSerde.datasetsFromInputStreamInJson(channel.consumeContentAsInputStream());
        if (datasetToUpdate == null) {
          throw new RestServiceException("The dataset is null after deserialize from given InputStream",
              RestServiceErrorCode.BadRequest);
        }
        datasetName = datasetToUpdate.getDatasetName();
        String accountNameFromRequest =
            RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_NAME, false);
        String containerNameFromRequest =
            RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_CONTAINER_NAME, false);
        accountName = datasetToUpdate.getAccountName();
        containerName = datasetToUpdate.getContainerName();
        if (accountNameFromRequest != null && !accountNameFromRequest.equals(accountName)) {
          throw new RestServiceException(
              "There is a mismatch between account name from header: " + accountNameFromRequest
                  + " and account name from dataset: " + accountName, RestServiceErrorCode.BadRequest);
        }
        if (containerNameFromRequest != null && !containerNameFromRequest.equals(containerName)) {
          throw new RestServiceException(
              "There is a mismatch between account name from header: " + containerNameFromRequest
                  + " and account name from dataset: " + containerName, RestServiceErrorCode.BadRequest);
        }
        accountService.addDataset(datasetToUpdate);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
      } catch (IOException e) {
        throw new RestServiceException("Bad dataset update request body", e, RestServiceErrorCode.BadRequest);
      } catch (AccountServiceException ex) {
        throw new RestServiceException(
            "Dataset update failed for accountName " + accountName + " containerName " + containerName + " datasetName "
                + datasetName, RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
