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
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.Dataset;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Helper class to resolve and add {@link Account} and {@link Container} details to requests.
 */
public class AccountAndContainerInjector {
  private static final Set<String> requiredAmbryHeadersForPutWithServiceId = Collections.singleton(Headers.SERVICE_ID);
  private static final Set<String> requiredAmbryHeadersForPutWithAccountAndContainerName = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(Headers.TARGET_ACCOUNT_NAME, Headers.TARGET_CONTAINER_NAME)));
  private static final Set<String> requiredAmbryHeadersForGetWithAccountAndContainerName = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(Headers.TARGET_ACCOUNT_NAME, Headers.TARGET_CONTAINER_NAME)));
  private static final Logger logger = LoggerFactory.getLogger(AccountAndContainerInjector.class);

  private final AccountService accountService;
  private final FrontendMetrics frontendMetrics;
  private final FrontendConfig frontendConfig;

  public AccountAndContainerInjector(AccountService accountService, FrontendMetrics frontendMetrics,
      FrontendConfig frontendConfig) {
    this.accountService = accountService;
    this.frontendMetrics = frontendMetrics;
    this.frontendConfig = frontendConfig;
  }

  /**
   * Injects target {@link Account} and {@link Container} for PUT requests. This method also ensures required headers
   * are present for the PUT requests that use serviceId as the account name, and the PUT requests that carry both the
   * {@code x-ambry-target-account} and {@code x-ambry-target-container} headers.
   * @param restRequest The Put {@link RestRequest}.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException
   */
  public void injectAccountAndContainerForPostRequest(RestRequest restRequest, RestRequestMetricsGroup metricsGroup)
      throws RestServiceException {
    accountAndContainerSanityCheck(restRequest);
    if (getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false) != null
        || getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false) != null) {
      ensureRequiredHeadersOrThrow(restRequest, requiredAmbryHeadersForPutWithAccountAndContainerName);
      frontendMetrics.putWithAccountAndContainerHeaderRate.mark();
      injectAccountAndContainerUsingAccountAndContainerHeaders(restRequest, metricsGroup);
    } else if (frontendConfig.allowServiceIdBasedPostRequest) {
      ensureRequiredHeadersOrThrow(restRequest, requiredAmbryHeadersForPutWithServiceId);
      frontendMetrics.putWithServiceIdForAccountNameRate.mark();
      String serviceId = getHeader(restRequest.getArgs(), Headers.SERVICE_ID, true);
      boolean isPrivate = isPrivate(restRequest.getArgs());
      injectAccountAndContainerUsingServiceId(restRequest, serviceId, isPrivate, metricsGroup);
    } else {
      throw new RestServiceException(
          "Missing either " + Headers.TARGET_ACCOUNT_NAME + " or " + Headers.TARGET_CONTAINER_NAME + " header",
          RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * Injects target {@link Account} and {@link Container} for GET datset requests. This method also ensures required the GET
   * requests that carry both the {@code x-ambry-target-account} and {@code x-ambry-target-container} headers.
   * @param restRequest The Get {@link RestRequest}.
   * @throws RestServiceException
   */
  public void injectAccountAndContainerForGetDatasetRequest(RestRequest restRequest) throws RestServiceException {
    accountAndContainerSanityCheck(restRequest);
    if (getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false) != null
        || getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false) != null) {
      ensureRequiredHeadersOrThrow(restRequest, requiredAmbryHeadersForGetWithAccountAndContainerName);
      frontendMetrics.getDatasetWithAccountAndContainerHeaderRate.mark();
      injectAccountAndContainerUsingAccountAndContainerHeaders(restRequest, null);
    } else {
      throw new RestServiceException(
          "Missing either " + Headers.TARGET_ACCOUNT_NAME + " or " + Headers.TARGET_CONTAINER_NAME + " header",
          RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * Injects target {@link Account}, {@link Container} for named blob requests.
   * Injects target {@link Dataset} for named blob request if it's dataset upload.
   * This will treat the request path as a named blob path that includes the account and container names.
   * @param restRequest The Put {@link RestRequest}.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException
   */
  public void injectAccountContainerAndDatasetForNamedBlob(RestRequest restRequest, RestRequestMetricsGroup metricsGroup)
      throws RestServiceException {
    accountAndContainerSanityCheck(restRequest);

    NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
    String accountName = namedBlobPath.getAccountName();
    Account targetAccount = accountService.getAccountByName(accountName);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedAccountNameCount.inc();
      throw new RestServiceException("Account cannot be found for accountName=" + accountName
          + " in put request with account and container headers.", RestServiceErrorCode.InvalidAccount);
    }
    ensureAccountNameMatch(targetAccount, restRequest);
    String containerName = namedBlobPath.getContainerName();
    Container targetContainer;
    try {
      targetContainer = accountService.getContainerByName(accountName, containerName);
    } catch (AccountServiceException e) {
      throw new RestServiceException("Failed to get container " + containerName + " from account " + accountName
          + " in put request with account and container headers.",
          RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
    }
    if (targetContainer == null) {
      frontendMetrics.unrecognizedContainerNameCount.inc();
      throw new RestServiceException(
          "Container cannot be found for accountName=" + accountName + " and containerName=" + containerName
              + " in put request with account and container headers.", RestServiceErrorCode.InvalidContainer);
    }
    if (targetContainer.getNamedBlobMode() == Container.NamedBlobMode.DISABLED) {
      throw new RestServiceException(
          "Named blob APIs disabled for this container. account=" + accountName + ", container=" + containerName,
          RestServiceErrorCode.BadRequest);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer, metricsGroup);
    setTargetDatasetAndVersionInRestRequestIfNeeded(restRequest, namedBlobPath);
  }

  /**
   * Obtains the target {@link Account} and {@link Container} id from the blobId string, queries the {@link AccountService}
   * to get the corresponding {@link Account} and {@link Container}, and injects the target {@link Account} and
   * {@link Container} into the {@link RestRequest}.
   * @param blobId The blobId to get the target {@link Account} and {@link Container} id.
   * @param restRequest The rest request to insert the target {@link Account} and {@link Container}.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException if 1) either {@link Account} or {@link Container} could not be found; or 2)
   *                              either {@link Account} or {@link Container} IDs were explicitly specified as
   *                              {@link Account#UNKNOWN_ACCOUNT_ID} or {@link Container#UNKNOWN_CONTAINER_ID}.
   */
  public void injectTargetAccountAndContainerFromBlobId(BlobId blobId, RestRequest restRequest,
      RestRequestMetricsGroup metricsGroup) throws RestServiceException {
    Account targetAccount = accountService.getAccountById(blobId.getAccountId());
    if (targetAccount == null) {
      frontendMetrics.getHeadDeleteUnrecognizedAccountCount.inc();
      // @todo The check can be removed once HelixAccountService is running with UNKNOWN_ACCOUNT created.
      if (blobId.getAccountId() != Account.UNKNOWN_ACCOUNT_ID) {
        throw new RestServiceException(
            "Account from blobId=" + blobId.getID() + "with accountId=" + blobId.getAccountId()
                + " cannot be recognized", RestServiceErrorCode.InvalidAccount);
      } else {
        logger.debug(
            "Account cannot be found for blobId={} with accountId={}. Setting targetAccount to UNKNOWN_ACCOUNT",
            blobId.getID(), blobId.getAccountId());
        targetAccount = accountService.getAccountById(Account.UNKNOWN_ACCOUNT_ID);
      }
    }
    Container targetContainer;
    try {
      targetContainer = accountService.getContainerById(blobId.getAccountId(), blobId.getContainerId());
    } catch (AccountServiceException e) {
      throw new RestServiceException(
          "Failed to get container with Id= " + blobId.getContainerId() + " from account " + targetAccount.getName()
              + "for blobId=" + blobId.getID(), RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
    }
    if (targetContainer == null) {
      frontendMetrics.getHeadDeleteUnrecognizedContainerCount.inc();
      throw new RestServiceException(
          "Container from blobId=" + blobId.getID() + "with accountId=" + blobId.getAccountId() + " containerId="
              + blobId.getContainerId() + " cannot be recognized", RestServiceErrorCode.InvalidContainer);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer, metricsGroup);
  }

  /**
   * If a non-unknown {@link Account} and {@link Container} was not previously injected, inject them into the provided
   * {@link RestRequest}, based on the given {@link BlobProperties}' service ID and blob privacy setting. This is useful
   * for V1 blob IDs that do not directly encode the account/container ID.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container}.
   * @param blobProperties The {@link BlobProperties} that contains the service id and blob privacy setting.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException if no valid account or container could be identified for re-injection.
   */
  public void ensureAccountAndContainerInjected(RestRequest restRequest, BlobProperties blobProperties,
      RestRequestMetricsGroup metricsGroup) throws RestServiceException {
    Account targetAccount = (Account) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY);
    Container targetContainer = (Container) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY);
    if (targetAccount == null || targetContainer == null) {
      throw new RestServiceException("Account and container were not injected by RestRequestService",
          RestServiceErrorCode.InternalServerError);
    } else if (targetAccount.getId() == Account.UNKNOWN_ACCOUNT_ID) {
      // This should only occur for V1 blobs, where the blob ID does not contain the actual account and container IDs.
      String serviceId = blobProperties.getServiceId();
      boolean isPrivate = blobProperties.isPrivate();
      injectAccountAndContainerUsingServiceId(restRequest, serviceId, isPrivate, metricsGroup);
    }
  }

  /**
   * Inject {@link Account} and {@link Container} into a {@link RestRequest} based on a blob's service ID and
   * privacy setting.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container} object.
   * @param serviceId The service ID associated with the blob.
   * @param isPrivate The blob's privacy setting.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException if either of {@link Account} or {@link Container} object could not be found.
   */
  private void injectAccountAndContainerUsingServiceId(RestRequest restRequest, String serviceId, boolean isPrivate,
      RestRequestMetricsGroup metricsGroup) throws RestServiceException {
    // First, try to see if a migrated account exists for the service ID.
    Account targetAccount = accountService.getAccountByName(serviceId);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedServiceIdCount.inc();
      logger.debug(
          "Account cannot be found for put request with serviceId={}. Setting targetAccount to UNKNOWN_ACCOUNT",
          serviceId);
      // If a migrated account does not exist fall back to the UNKNOWN_ACCOUNT.
      targetAccount = accountService.getAccountById(Account.UNKNOWN_ACCOUNT_ID);
    }
    // Either the UNKNOWN_ACCOUNT, or the migrated account should contain default public/private containers
    Container targetContainer;
    short containerId = isPrivate ? Container.DEFAULT_PRIVATE_CONTAINER_ID : Container.DEFAULT_PUBLIC_CONTAINER_ID;
    try {
      targetContainer = accountService.getContainerById(targetAccount.getId(), containerId);
    } catch (AccountServiceException e) {
      throw new RestServiceException(
          "Failed to get container with Id= " + containerId + " from account " + targetAccount.getName()
              + "for put request; ServiceId=" + serviceId + ", isPrivate=" + isPrivate,
          RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
    }
    if (targetContainer == null) {
      throw new RestServiceException(
          "Invalid account or container to inject; serviceId=" + serviceId + ", isPrivate=" + isPrivate,
          RestServiceErrorCode.InternalServerError);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer, metricsGroup);
  }

  /**
   * Injects {@link Account} and {@link Container} for the PUT and GET requests that carry the target account and container headers.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container} object.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   * @throws RestServiceException if either of {@link Account} or {@link Container} object could not be found.
   */
  private void injectAccountAndContainerUsingAccountAndContainerHeaders(RestRequest restRequest,
      RestRequestMetricsGroup metricsGroup) throws RestServiceException {
    String accountName = getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false);
    Account targetAccount = accountService.getAccountByName(accountName);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedAccountNameCount.inc();
      throw new RestServiceException("Account cannot be found for accountName=" + accountName
          + " in put request with account and container headers.", RestServiceErrorCode.InvalidAccount);
    }
    ensureAccountNameMatch(targetAccount, restRequest);
    String containerName = getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false);
    Container targetContainer;
    try {
      targetContainer = accountService.getContainerByName(accountName, containerName);
    } catch (AccountServiceException e) {
      throw new RestServiceException("Failed to get container " + containerName + " from account " + accountName
          + " for put request with account and container headers.",
          RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
    }
    if (targetContainer == null) {
      frontendMetrics.unrecognizedContainerNameCount.inc();
      throw new RestServiceException(
          "Container cannot be found for accountName=" + accountName + " and containerName=" + containerName
              + " in put request with account and container headers.", RestServiceErrorCode.InvalidContainer);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer, metricsGroup);
  }

  /**
   * Injects {@link Account},{@link Container} for the POST requests that carry the target account and
   * container headers.
   * @param restRequest The {@link RestRequest} to inject {@link Account} and {@link Container} object.
   * @param dataset the {@link Dataset}
   * @throws RestServiceException
   */
  public void injectAccountAndContainerUsingDatasetBody(RestRequest restRequest, Dataset dataset)
      throws RestServiceException {
    String accountName = dataset.getAccountName();
    Account targetAccount = accountService.getAccountByName(accountName);
    if (targetAccount == null) {
      frontendMetrics.unrecognizedAccountNameCount.inc();
      throw new RestServiceException("Account cannot be found for accountName=" + accountName
          + " in put request with account and container headers.", RestServiceErrorCode.InvalidAccount);
    }
    ensureAccountNameMatch(targetAccount, restRequest);
    String containerName = dataset.getContainerName();
    Container targetContainer;
    try {
      targetContainer = accountService.getContainerByName(accountName, containerName);
    } catch (AccountServiceException e) {
      throw new RestServiceException("Failed to get container " + containerName + " from account " + accountName
          + " for put request with account and container headers.",
          RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
    }
    if (targetContainer == null) {
      frontendMetrics.unrecognizedContainerNameCount.inc();
      throw new RestServiceException(
          "Container cannot be found for accountName=" + accountName + " and containerName=" + containerName
              + " in put request with account and container headers.", RestServiceErrorCode.InvalidContainer);
    }
    setTargetAccountAndContainerInRestRequest(restRequest, targetAccount, targetContainer, null);
  }

  /**
   * Sanity check for {@link RestRequest}. This check ensures that the specified service id, account and container name,
   * if they exist, should not be the same as the not-allowed values. It also makes sure certain headers must not be present.
   * @param restRequest The {@link RestRequest} to check.
   * @throws RestServiceException if the specified service id, account or container name is set as system reserved value.
   */
  private void accountAndContainerSanityCheck(RestRequest restRequest) throws RestServiceException {
    NamedBlobPath namedBlobPath = null;
    if (getRequestPath(restRequest).matchesOperation(Operations.NAMED_BLOB)) {
      namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
    }
    if (Account.UNKNOWN_ACCOUNT_NAME.equals(getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false))
        || Account.UNKNOWN_ACCOUNT_NAME.equals(getHeader(restRequest.getArgs(), Headers.SERVICE_ID, false)) || (
        namedBlobPath != null && Account.UNKNOWN_ACCOUNT_NAME.equals(namedBlobPath.getAccountName()))) {
      throw new RestServiceException("Invalid account for putting blob", RestServiceErrorCode.InvalidAccount);
    }
    String targetContainerName = getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false);
    if (Container.UNKNOWN_CONTAINER_NAME.equals(targetContainerName) || (namedBlobPath != null
        && Container.UNKNOWN_CONTAINER_NAME.equals(namedBlobPath.getContainerName()))) {
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
   * Sets target {@link Account} and {@link Container} objects in the {@link RestRequest}. Also handles instantiation
   * and injection of {@link ContainerMetrics} instances.
   * @param restRequest The {@link RestRequest} to set.
   * @param targetAccount The target {@link Account} to set.
   * @param targetContainer The target {@link Container} to set.
   * @param metricsGroup The {@link RestRequestMetricsGroup} to use to set up {@link ContainerMetrics}, or {@code null}
   *                     if {@link ContainerMetrics} instantiation is not needed.
   */
  private void setTargetAccountAndContainerInRestRequest(RestRequest restRequest, Account targetAccount,
      Container targetContainer, RestRequestMetricsGroup metricsGroup) {
    restRequest.setArg(InternalKeys.TARGET_ACCOUNT_KEY, targetAccount);
    restRequest.setArg(InternalKeys.TARGET_CONTAINER_KEY, targetContainer);
    logger.trace("Setting targetAccount={} and targetContainer={} for restRequest={} ", targetAccount, targetContainer,
        restRequest);
    if (metricsGroup != null) {
      if (!frontendConfig.containerMetricsExcludedAccounts.contains(targetAccount.getName())) {
        restRequest.getMetricsTracker()
            .injectContainerMetrics(
                metricsGroup.getContainerMetrics(targetAccount.getName(), targetContainer.getName()));
      }
    }
  }

  /**
   * Set target {@link Dataset} objects and its version in the {@link RestRequest}.
   * @param restRequest The {@link RestRequest} to set.
   * @param namedBlobPath the {@link NamedBlobPath} to get account, container and dataset name.
   * @throws RestServiceException
   */
  private void setTargetDatasetAndVersionInRestRequestIfNeeded(RestRequest restRequest, NamedBlobPath namedBlobPath)
      throws RestServiceException {
    if (RestUtils.isDatasetVersionUpload(restRequest.getArgs())) {
      String accountName = namedBlobPath.getAccountName();
      String containerName = namedBlobPath.getContainerName();
      String blobName = namedBlobPath.getBlobName();
      Objects.requireNonNull(blobName, "blobName should not be null");
      blobName = blobName.startsWith(PATH_SEPARATOR_STRING) ? blobName.substring(1) : blobName;
      String[] splitName = blobName.split(PATH_SEPARATOR_STRING, 2);
      int expectedSegments = 2;
      if (splitName.length != expectedSegments) {
        throw new RestServiceException(
            "Blob name must have format 'datasetName/version'.  Received blobName: " + blobName,
            RestServiceErrorCode.BadRequest);
      }
      String datasetName = splitName[0];
      String version = splitName[1];
      try {
        Dataset dataset = accountService.getDataset(accountName, containerName, datasetName);
        restRequest.setArg(InternalKeys.TARGET_DATASET, dataset);
        restRequest.setArg(InternalKeys.TARGET_DATASET_VERSION, version);
      } catch (AccountServiceException e) {
        frontendMetrics.unrecognizedDatasetNameCount.inc();
        logger.error(
            "Dataset get failed for accountName " + accountName + " containerName " + containerName + " datasetName "
                + datasetName);
        throw new RestServiceException(e.getMessage(), RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
      }
    }
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
