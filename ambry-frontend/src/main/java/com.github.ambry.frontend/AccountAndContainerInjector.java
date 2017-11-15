/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Helper class to resolve and add {@link Account} and {@link Container} details to requests.
 */
class AccountAndContainerInjector {
  private static final Set<String> requiredAmbryHeadersForPutWithServiceId = Collections.singleton(Headers.SERVICE_ID);
  private static final Set<String> requiredAmbryHeadersForPutWithAccountAndContainerName = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(Headers.TARGET_ACCOUNT_NAME, Headers.TARGET_CONTAINER_NAME)));
  private static final Logger logger = LoggerFactory.getLogger(AccountAndContainerInjector.class);

  private final AccountService accountService;
  private final ClusterMap clusterMap;
  private final FrontendMetrics frontendMetrics;
  private final FrontendConfig frontendConfig;

  AccountAndContainerInjector(AccountService accountService, ClusterMap clusterMap, FrontendMetrics frontendMetrics,
      FrontendConfig frontendConfig) {
    this.accountService = accountService;
    this.clusterMap = clusterMap;
    this.frontendMetrics = frontendMetrics;
    this.frontendConfig = frontendConfig;
  }

  /**
   * Injects target {@link Account} and {@link Container} for PUT requests. This method also ensures required headers
   * are present for the PUT requests that use serviceId as the account name, and the PUT requests that carry both the
   * {@code x-ambry-target-account} and {@code x-ambry-target-container} headers.
   * @param restRequest The Put {@link RestRequest}.
   * @throws RestServiceException
   */
  void injectAccountAndContainerForPostRequest(RestRequest restRequest) throws RestServiceException {
    accountAndContainerSanityCheck(restRequest);
    if (getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false) != null
        || getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false) != null) {
      ensureRequiredHeadersOrThrow(restRequest, requiredAmbryHeadersForPutWithAccountAndContainerName);
      frontendMetrics.putWithAccountAndContainerHeaderRate.mark();
      injectAccountAndContainerUsingAccountAndContainerHeaders(restRequest);
    } else if (frontendConfig.frontendAllowServiceIdBasedPostRequest) {
      ensureRequiredHeadersOrThrow(restRequest, requiredAmbryHeadersForPutWithServiceId);
      frontendMetrics.putWithServiceIdForAccountNameRate.mark();
      injectAccountAndContainerUsingServiceId(restRequest);
    } else {
      throw new RestServiceException(
          "Missing either " + Headers.TARGET_ACCOUNT_NAME + " or " + Headers.TARGET_CONTAINER_NAME + " header",
          RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * Obtains the target {@link Account} and {@link Container} id from the blobId string, queries the {@link AccountService}
   * to get the corresponding {@link Account} and {@link Container}, and injects the target {@link Account} and
   * {@link Container} into the {@link RestRequest}.
   * @param blobIdStr The blobId string to get the target {@link Account} and {@link Container} id。
   * @param restRequest The rest request to insert the target {@link Account} and {@link Container}.
   * @throws RestServiceException if 1) either {@link Account} or {@link Container} could not be found; or 2)
   *                              either {@link Account} or {@link Container} were explicitly specified as
   *                              {@link Account#UNKNOWN_ACCOUNT} or {@link Container#UNKNOWN_CONTAINER}.
   */
  void injectTargetAccountAndContainerFromBlobId(String blobIdStr, RestRequest restRequest)
      throws RestServiceException {
    BlobId blobId;
    try {
      blobId = new BlobId(blobIdStr, clusterMap);
    } catch (Exception e) {
      throw new RestServiceException("Invalid blob id=" + blobIdStr, RestServiceErrorCode.BadRequest);
    }
    Account targetAccount = accountService.getAccountById(blobId.getAccountId());
    if (targetAccount == null) {
      frontendMetrics.getHeadDeleteUnrecognizedAccountCount.inc();
      // @todo The check can be removed once HelixAccountService is running with UNKNOWN_ACCOUNT created.
      if (blobId.getAccountId() != Account.UNKNOWN_ACCOUNT_ID) {
        throw new RestServiceException(
            "Account from blobId=" + blobIdStr + "with accountId=" + blobId.getAccountId() + " cannot be recognized",
            RestServiceErrorCode.InvalidAccount);
      } else {
        logger.debug(
            "Account cannot be found for blobId={} with accountId={}. Setting targetAccount to UNKNOWN_ACCOUNT",
            blobIdStr, blobId.getAccountId());
        targetAccount = Account.UNKNOWN_ACCOUNT;
      }
    }
    Container targetContainer = targetAccount.getContainerById(blobId.getContainerId());
    if (targetContainer == null) {
      frontendMetrics.getHeadDeleteUnrecognizedContainerCount.inc();
      throw new RestServiceException(
          "Container from blobId=" + blobIdStr + "with accountId=" + blobId.getAccountId() + " containerId="
              + blobId.getContainerId() + " cannot be recognized", RestServiceErrorCode.InvalidContainer);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer);
  }

  /**
   * Injects {@link Account} and {@link Container} for the put request that does not carry the target account or
   * target container header but a serviceId. The serviceId will be used as account name to query for the target
   * {@link Account}.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container} object.
   * @throws RestServiceException if either of {@link Account} or {@link Container} object could not be found.
   */
  private void injectAccountAndContainerUsingServiceId(RestRequest restRequest) throws RestServiceException {
    String accountName = getHeader(restRequest.getArgs(), Headers.SERVICE_ID, false);
    Account targetAccount = accountService.getAccountByName(accountName);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedServiceIdCount.inc();
      logger.debug(
          "Account cannot be found for accountName={} in put request with serviceId. Setting targetAccount to UNKNOWN_ACCOUNT",
          accountName);
      targetAccount = Account.UNKNOWN_ACCOUNT;
    }
    boolean isBlobPrivate = isPrivate(restRequest.getArgs());
    Container targetContainer = targetAccount.getContainerById(
        isBlobPrivate ? Container.DEFAULT_PRIVATE_CONTAINER_ID : Container.DEFAULT_PUBLIC_CONTAINER_ID);
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer);
  }

  /**
   * Injects {@link Account} and {@link Container} for the PUT requests that carry the target account and container headers.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container} object.
   * @throws RestServiceException if either of {@link Account} or {@link Container} object could not be found.
   */
  private void injectAccountAndContainerUsingAccountAndContainerHeaders(RestRequest restRequest)
      throws RestServiceException {
    String accountName = getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false);
    Account targetAccount = accountService.getAccountByName(accountName);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedAccountNameCount.inc();
      throw new RestServiceException("Account cannot be found for accountName=" + accountName
          + " in put request with account and container headers.", RestServiceErrorCode.InvalidAccount);
    }
    ensureAccountNameMatch(targetAccount, restRequest);
    String containerName = getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false);
    Container targetContainer = targetAccount.getContainerByName(containerName);
    if (targetContainer == null) {
      frontendMetrics.unrecognizedContainerNameCount.inc();
      throw new RestServiceException(
          "Container cannot be found for accountName=" + accountName + " and containerName=" + containerName
              + " in put request with account and container headers.", RestServiceErrorCode.InvalidContainer);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer);
  }

  /**
   * Sanity check for {@link RestRequest}. This check ensures that the specified service id, account and container name,
   * if they exist, should not be the same as the not-allowed values. It also makes sure certain headers must not be present.
   * @param restRequest The {@link RestRequest} to check.
   * @throws RestServiceException if the specified service id, account or container name is set as system reserved value.
   */
  private void accountAndContainerSanityCheck(RestRequest restRequest) throws RestServiceException {
    if (Account.UNKNOWN_ACCOUNT_NAME.equals(getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false))
        || Account.UNKNOWN_ACCOUNT_NAME.equals(getHeader(restRequest.getArgs(), Headers.SERVICE_ID, false))) {
      throw new RestServiceException("Invalid account for putting blob", RestServiceErrorCode.InvalidAccount);
    }
    String targetContainerName = getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false);
    if (Container.UNKNOWN_CONTAINER_NAME.equals(targetContainerName)) {
      throw new RestServiceException("Invalid container for putting blob", RestServiceErrorCode.InvalidContainer);
    }
    List<String> prohibitedHeaders = Arrays.asList(InternalKeys.TARGET_ACCOUNT_KEY, InternalKeys.TARGET_CONTAINER_KEY);
    for (String prohibitedHeader : prohibitedHeaders) {
      if (restRequest.getArgs().get(prohibitedHeader) != null) {
        throw new RestServiceException("Unexpected header " + prohibitedHeader + " in request",
            RestServiceErrorCode.BadRequest);
      }
    }
  }

  /**
   * Sets target {@link Account} and {@link Container} objects in the {@link RestRequest}.
   * @param restRequest The {@link RestRequest} to set.
   * @param targetAccount The target {@link Account} to set.
   * @param targetContainer The target {@link Container} to set.
   */
  private void setTargetAccountAndContainerInRestRequest(RestRequest restRequest, Account targetAccount,
      Container targetContainer) throws RestServiceException {
    restRequest.setArg(InternalKeys.TARGET_ACCOUNT_KEY, targetAccount);
    restRequest.setArg(InternalKeys.TARGET_CONTAINER_KEY, targetContainer);
    logger.trace("Sets targetAccount={} and targetContainer={} for restRequest={} ", targetAccount, targetContainer,
        restRequest);
  }

  /**
   * Ensures the {@link Account} matches the account name specified in the {@link RestRequest}, if it is specified.
   * @param account The {@link Account} to ensure.
   * @param restRequest The {@link RestRequest} to ensure.
   * @throws RestServiceException if the {@link Account}'s name does not match the name specified in the {@link RestRequest}.
   */
  private void ensureAccountNameMatch(Account account, RestRequest restRequest) throws RestServiceException {
    String accountNameFromHeader = getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false);
    if (accountNameFromHeader != null && !accountNameFromHeader.equals(account.getName())) {
      throw new RestServiceException(
          "Account name in request did not match account name returned by backend. " + "Account in header: '"
              + accountNameFromHeader + "'. Account returned by backend: '" + account.getName() + "'.",
          RestServiceErrorCode.InternalServerError);
    }
  }
}
