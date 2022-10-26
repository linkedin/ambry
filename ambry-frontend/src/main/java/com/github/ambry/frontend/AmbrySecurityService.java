/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Container;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.HostLevelThrottler;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.quota.ThrottlingRecommendation;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.router.GetBlobOptions.*;


/**
 * Default implementation of {@link SecurityService} for Ambry that doesn't do any validations, but just
 * sets the respective headers on response.
 */
class AmbrySecurityService implements SecurityService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmbrySecurityService.class);
  static final Set<String> OPERATIONS = Collections.unmodifiableSet(
      Utils.getStaticFieldValuesAsStrings(Operations.class)
          .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final UrlSigningService urlSigningService;
  private final HostLevelThrottler hostLevelThrottler;
  private final QuotaManager quotaManager;
  private final AmbryCostModelPolicy ambryCostModelPolicy;
  private boolean isOpen;

  AmbrySecurityService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics,
      UrlSigningService urlSigningService, HostLevelThrottler hostLevelThrottler, QuotaManager quotaManager) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.urlSigningService = urlSigningService;
    this.hostLevelThrottler = hostLevelThrottler;
    this.quotaManager = quotaManager;
    this.ambryCostModelPolicy = new SimpleAmbryCostModelPolicy();
    isOpen = true;
  }

  @Override
  public void preProcessRequest(RestRequest restRequest, Callback<Void> callback) {
    Exception exception = null;
    frontendMetrics.securityServicePreProcessRequestRate.mark();
    long startTimeMs = System.currentTimeMillis();
    if (!isOpen) {
      exception = new RestServiceException("SecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (restRequest == null) {
      throw new IllegalArgumentException("RestRequest is null");
    } else if (restRequest.getArgs().containsKey(InternalKeys.KEEP_ALIVE_ON_ERROR_HINT)) {
      exception = new RestServiceException(InternalKeys.KEEP_ALIVE_ON_ERROR_HINT + " is not allowed in the request",
          RestServiceErrorCode.BadRequest);
    }
    restRequest.setArg(InternalKeys.SEND_TRACKING_INFO, frontendConfig.attachTrackingInfo);
    if (exception == null && urlSigningService.isRequestSigned(restRequest)) {
      urlSigningService.verifySignedRequest(restRequest, (r, e) -> {
        frontendMetrics.securityServicePreProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
        callback.onCompletion(r, e);
      });
    } else {
      frontendMetrics.securityServicePreProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      callback.onCompletion(null, exception);
    }
  }

  @Override
  public void processRequest(RestRequest restRequest, Callback<Void> callback) {
    Exception exception = null;
    frontendMetrics.securityServiceProcessRequestRate.mark();
    long startTimeMs = System.currentTimeMillis();
    if (!isOpen) {
      exception = new RestServiceException("SecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (restRequest == null) {
      throw new IllegalArgumentException("RestRequest is null");
    }
    frontendMetrics.securityServiceProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
    callback.onCompletion(null, exception);
  }

  @Override
  public void postProcessRequest(RestRequest restRequest, Callback<Void> callback) {
    if (restRequest == null || callback == null) {
      throw new IllegalArgumentException("RestRequest or Callback is null");
    }
    Exception exception = null;
    frontendMetrics.securityServicePostProcessRequestRate.mark();
    long startTimeMs = System.currentTimeMillis();
    try {
      if (!isOpen) {
        exception = new RestServiceException("SecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
      } else if (hostLevelThrottler.shouldThrottle(restRequest)) {
        exception = new RestServiceException("Too many requests", RestServiceErrorCode.TooManyRequests);
      } else {
        if (QuotaUtils.isRequestResourceQuotaManaged(restRequest) && quotaManager != null) {
          ThrottlingRecommendation throttlingRecommendation = quotaManager.recommend(restRequest);
          if (throttlingRecommendation != null && throttlingRecommendation.shouldThrottle()
              && quotaManager.getQuotaMode() == QuotaMode.THROTTLING) {
            Map<String, String> quotaHeaderMap = RestUtils.buildUserQuotaHeadersMap(throttlingRecommendation);
            throw new RestServiceException("User Quota Exceeded", RestServiceErrorCode.TooManyRequests, true, true,
                quotaHeaderMap);
          }
        }
        if (restRequest.getRestMethod() == RestMethod.DELETE || restRequest.getRestMethod() == RestMethod.PUT) {
          accountAndContainerNamePreconditionCheck(restRequest);
        } else if (restRequest.getRestMethod() == RestMethod.GET) {
          RequestPath requestPath = getRequestPath(restRequest);
          String operationOrBlobId = requestPath.getOperationOrBlobId(true);
          // ensure that secure path validation is only performed when getting blobs rather than other operations.
          if (!operationOrBlobId.isEmpty() && !OPERATIONS.contains(operationOrBlobId)) {
            validateSecurePathIfRequired(restRequest, requestPath.getPrefix(), frontendConfig.securePathPrefix);
          }
        }
      }
    } catch (Exception e) {
      exception = e;
    } finally {
      frontendMetrics.securityServicePostProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      callback.onCompletion(null, exception);
    }
  }

  @Override
  public void processRequestCharges(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo) {
    Map<String, Double> requestCost = ambryCostModelPolicy.calculateRequestCost(restRequest, responseChannel, blobInfo);
    setRequestCostHeader(requestCost, responseChannel);
    if (QuotaUtils.isRequestResourceQuotaManaged(restRequest) && quotaManager != null) {
      ThrottlingRecommendation throttlingRecommendation = null;
      try {
        throttlingRecommendation = quotaManager.recommend(restRequest);
      } catch (QuotaException quotaException) {
        LOGGER.warn("Exception {} in while processing request charges.", quotaException);
      }
      if (throttlingRecommendation != null) {
        RestUtils.buildUserQuotaHeadersMap(throttlingRecommendation)
            .entrySet()
            .stream()
            .forEach(headerEntry -> responseChannel.setHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }
  }

  @Override
  public void processResponse(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo,
      Callback<Void> callback) {
    Exception exception = null;
    frontendMetrics.securityServiceProcessResponseRate.mark();
    long startTimeMs = System.currentTimeMillis();
    if (!isOpen) {
      exception = new RestServiceException("SecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else {
      if (restRequest == null || responseChannel == null) {
        throw new IllegalArgumentException("One of the required params is null");
      }
      RequestPath requestPath = RestUtils.getRequestPath(restRequest);
      RestMethod restMethod = restRequest.getRestMethod();
      if (blobInfo == null && !restMethod.equals(RestMethod.OPTIONS) && !restMethod.equals(RestMethod.PUT)
          && !restMethod.equals(RestMethod.DELETE)) {
        if (!requestPath.matchesOperation(Operations.GET_SIGNED_URL)) {
          throw new IllegalArgumentException("BlobInfo is null");
        }
      }
      try {
        GetBlobOptions options;
        responseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        switch (restMethod) {
          case HEAD:
            options = RestUtils.buildGetBlobOptions(restRequest.getArgs(), null, GetOption.None, restRequest,
                NO_BLOB_SEGMENT_IDX_SPECIFIED);
            responseChannel.setStatus(options.getRange() == null ? ResponseStatus.Ok : ResponseStatus.PartialContent);
            responseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
                new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
            setHeadResponseHeaders(blobInfo, options, restRequest, responseChannel);
            break;
          case GET:
            if (!requestPath.matchesOperation(Operations.GET_SIGNED_URL)) {
              options = RestUtils.buildGetBlobOptions(restRequest.getArgs(), null, GetOption.None, restRequest,
                  NO_BLOB_SEGMENT_IDX_SPECIFIED);
              responseChannel.setStatus(ResponseStatus.Ok);
              RestUtils.SubResource subResource = RestUtils.getRequestPath(restRequest).getSubResource();
              responseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
                  new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
              Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
              if (subResource == null) {
                Long ifModifiedSinceMs = getIfModifiedSinceMs(restRequest);
                if (ifModifiedSinceMs != null
                    && RestUtils.toSecondsPrecisionInMs(blobInfo.getBlobProperties().getCreationTimeInMs())
                    <= ifModifiedSinceMs) {
                  responseChannel.setStatus(ResponseStatus.NotModified);
                } else {
                  if (options.getRange() != null) {
                    responseChannel.setStatus(ResponseStatus.PartialContent);
                  }
                  setGetBlobResponseHeaders(blobInfo, options, responseChannel);
                  setBlobPropertiesHeaders(blobInfo.getBlobProperties(), responseChannel);
                  setAccountAndContainerHeaders(restRequest, responseChannel);
                  setUserMetadataHeaders(blobInfo.getUserMetadata(), responseChannel, container.getUserMetadataKeysToNotPrefixInResponse());
                  responseChannel.setHeader(RestUtils.Headers.LIFE_VERSION, blobInfo.getLifeVersion());
                }
                setCacheHeaders(restRequest, responseChannel);
              } else {
                if (subResource.equals(RestUtils.SubResource.BlobInfo)) {
                  setBlobInfoHeaders(blobInfo.getBlobProperties(), responseChannel);
                  setBlobPropertiesHeaders(blobInfo.getBlobProperties(), responseChannel);
                  setAccountAndContainerHeaders(restRequest, responseChannel);
                  responseChannel.setHeader(RestUtils.Headers.LIFE_VERSION, blobInfo.getLifeVersion());
                } else if (subResource.equals(SubResource.Segment)) {
                  setGetBlobResponseHeaders(blobInfo, options, responseChannel);
                  setBlobPropertiesHeaders(blobInfo.getBlobProperties(), responseChannel);
                  setAccountAndContainerHeaders(restRequest, responseChannel);
                }
                if (!setUserMetadataHeaders(blobInfo.getUserMetadata(), responseChannel, container.getUserMetadataKeysToNotPrefixInResponse())) {
                  restRequest.setArg(InternalKeys.SEND_USER_METADATA_AS_RESPONSE_BODY, true);
                }
              }
            }
            break;
          case POST:
            responseChannel.setStatus(ResponseStatus.Created);
            responseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
            responseChannel.setHeader(RestUtils.Headers.CREATION_TIME,
                new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
            break;
          case OPTIONS:
          case PUT:
            if (requestPath.matchesOperation(Operations.NAMED_BLOB)) {
              responseChannel.setStatus(ResponseStatus.Created);
              responseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
              responseChannel.setHeader(RestUtils.Headers.CREATION_TIME,
                  new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
            }
            break;
          case DELETE:
            // There is nothing to do with delete
            break;
          default:
            exception = new RestServiceException("Cannot process response for request with method " + restMethod,
                RestServiceErrorCode.InternalServerError);
        }
      } catch (RestServiceException e) {
        exception = e;
      }
    }
    processRequestCharges(restRequest, responseChannel, blobInfo);
    frontendMetrics.securityServiceProcessResponseTimeInMs.update(System.currentTimeMillis() - startTimeMs);
    callback.onCompletion(null, exception);
  }

  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Fetches the {@link RestUtils.Headers#IF_MODIFIED_SINCE} value in epoch time if present
   * @param restRequest the {@link RestRequest} that needs to be parsed
   * @return the {@link RestUtils.Headers#IF_MODIFIED_SINCE} value in epoch time if present
   */
  private Long getIfModifiedSinceMs(RestRequest restRequest) {
    if (restRequest.getArgs().get(RestUtils.Headers.IF_MODIFIED_SINCE) != null) {
      return RestUtils.getTimeFromDateString((String) restRequest.getArgs().get(RestUtils.Headers.IF_MODIFIED_SINCE));
    }
    return null;
  }

  /**
   * Sets the required headers in the HEAD response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @param options the {@link GetBlobOptions} associated with the request.
   * @param restRequest the {@link RestRequest} that was received.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setHeadResponseHeaders(BlobInfo blobInfo, GetBlobOptions options, RestRequest restRequest,
      RestResponseChannel restResponseChannel) throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
    }
    if (blobProperties.getContentEncoding() != null) {
      restResponseChannel.setHeader(Headers.CONTENT_ENCODING, blobProperties.getContentEncoding());
    }
    restResponseChannel.setHeader(RestUtils.Headers.ACCEPT_RANGES, RestUtils.BYTE_RANGE_UNITS);
    long contentLength = blobProperties.getBlobSize();
    if (options.getRange() != null) {
      Pair<String, Long> rangeAndLength =
          RestUtils.buildContentRangeAndLength(options.getRange(), contentLength, options.resolveRangeOnEmptyBlob());
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_RANGE, rangeAndLength.getFirst());
      contentLength = rangeAndLength.getSecond();
    }
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, contentLength);
    setBlobPropertiesHeaders(blobProperties, restResponseChannel);
    setAccountAndContainerHeaders(restRequest, restResponseChannel);
    restResponseChannel.setHeader(RestUtils.Headers.LIFE_VERSION, blobInfo.getLifeVersion());
  }

  /**
   * Sets the required headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @param options the {@link GetBlobOptions} associated with the request.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setGetBlobResponseHeaders(BlobInfo blobInfo, GetBlobOptions options,
      RestResponseChannel restResponseChannel) throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, blobProperties.getBlobSize());
    restResponseChannel.setHeader(RestUtils.Headers.ACCEPT_RANGES, RestUtils.BYTE_RANGE_UNITS);
    long contentLength = blobProperties.getBlobSize();
    if (options.getRange() != null) {
      Pair<String, Long> rangeAndLength =
          RestUtils.buildContentRangeAndLength(options.getRange(), contentLength, options.resolveRangeOnEmptyBlob());
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_RANGE, rangeAndLength.getFirst());
      contentLength = rangeAndLength.getSecond();
    }
    if (contentLength < frontendConfig.chunkedGetResponseThresholdInBytes) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, contentLength);
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
      // Ensure browsers do not execute html with embedded exploits.
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
    if (blobProperties.getContentEncoding() != null) {
      restResponseChannel.setHeader(Headers.CONTENT_ENCODING, blobProperties.getContentEncoding());
    }
  }

  /**
   * Sets headers that provide directions to proxies and caches.
   * @param restRequest the {@link RestRequest} that was received.
   * @param restResponseChannel the channel that the response will be sent over
   * @throws RestServiceException if there is any problem setting the headers
   */
  private void setCacheHeaders(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
    if (container.isCacheable()) {
      long cacheTtl = container.getCacheTtlInSecond() != null ? container.getCacheTtlInSecond().longValue()
          : frontendConfig.cacheValiditySeconds;
      restResponseChannel.setHeader(RestUtils.Headers.EXPIRES,
          new Date(System.currentTimeMillis() + cacheTtl * Time.MsPerSec));
      restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL, "max-age=" + cacheTtl);
    } else {
      restResponseChannel.setHeader(RestUtils.Headers.EXPIRES, restResponseChannel.getHeader(RestUtils.Headers.DATE));
      restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL, "private, no-cache, no-store, proxy-revalidate");
      restResponseChannel.setHeader(RestUtils.Headers.PRAGMA, "no-cache");
    }
  }

  /**
   * Sets the blob properties in the headers of the response.
   * @param blobProperties the {@link BlobProperties} that need to be set in the headers.
   * @param restResponseChannel the {@link RestResponseChannel} that is used for sending the response.
   * @throws RestServiceException if there are any problems setting the header.
   */
  private void setBlobPropertiesHeaders(BlobProperties blobProperties, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, blobProperties.getBlobSize());
    restResponseChannel.setHeader(RestUtils.Headers.SERVICE_ID, blobProperties.getServiceId());
    restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.ENCRYPTED_IN_STORAGE, blobProperties.isEncrypted());
    if (blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      restResponseChannel.setHeader(RestUtils.Headers.TTL, Long.toString(blobProperties.getTimeToLiveInSeconds()));
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE, blobProperties.getContentType());
    }
    if (blobProperties.getOwnerId() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.OWNER_ID, blobProperties.getOwnerId());
    }
  }

  /**
   * Set the specific required headers for get blob info in the response.
   * @param blobProperties the {@link BlobProperties} that need to be set in the headers.
   * @param restResponseChannel the {@link RestResponseChannel} that is used for sending the response.
   */
  private void setBlobInfoHeaders(BlobProperties blobProperties, RestResponseChannel restResponseChannel) {
    if (blobProperties.getContentEncoding() != null) {
      restResponseChannel.setHeader(Headers.AMBRY_CONTENT_ENCODING, blobProperties.getContentEncoding());
    }
  }

  /**
   * Adds the account and container details to the response headers if the {@code restRequest} contains the target
   * account and container and they are not generic unknowns.
   * @param restRequest the {@link RestRequest} that contains the {@link Account} and {@link Container} details.
   * @param restResponseChannel the {@link RestResponseChannel} where headers need to be set.
   * @throws RestServiceException if headers cannot be set.
   */
  private void setAccountAndContainerHeaders(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
    Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
    if (account.getId() != Account.UNKNOWN_ACCOUNT_ID) {
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, account.getName());
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, container.getName());
    }
    restResponseChannel.setHeader(RestUtils.Headers.PRIVATE, !container.isCacheable());
  }

  /**
   * Validate the secure path in the URI if required for specific {@link Container}.
   * @param restRequest the {@link RestRequest} that contains the {@link Account} and {@link Container} details.
   * @param prefixToCheck the prefix from URI which may contain secure path.
   * @param expectSecurePath the expected secure path specified in {@link com.github.ambry.config.FrontendConfig}.
   * @throws RestServiceException
   */
  private void validateSecurePathIfRequired(RestRequest restRequest, String prefixToCheck, String expectSecurePath)
      throws RestServiceException {
    Container targetContainer = getContainerFromArgs(restRequest.getArgs());
    if (targetContainer.isSecurePathRequired()) {
      String securePath = prefixToCheck.startsWith("/") ? prefixToCheck.substring(1) : prefixToCheck;
      if (!securePath.equals(expectSecurePath)) {
        frontendMetrics.securePathValidationFailedCount.inc();
        throw new RestServiceException("Secure path in restRequest doesn't match the expected one",
            RestServiceErrorCode.AccessDenied);
      }
    }
  }
}
