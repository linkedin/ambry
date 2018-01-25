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
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Date;
import java.util.GregorianCalendar;


/**
 * Default implementation of {@link SecurityService} for Ambry that doesn't do any validations, but just
 * sets the respective headers on response.
 */
class AmbrySecurityService implements SecurityService {

  private boolean isOpen;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final UrlSigningService urlSigningService;

  AmbrySecurityService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics,
      UrlSigningService urlSigningService) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.urlSigningService = urlSigningService;
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
    } else if (urlSigningService.isRequestSigned(restRequest)) {
      try {
        urlSigningService.verifySignedRequest(restRequest);
      } catch (RestServiceException e) {
        exception = e;
      }
    }
    frontendMetrics.securityServicePreProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
    callback.onCompletion(null, exception);
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
    Exception exception = null;
    frontendMetrics.securityServicePostProcessRequestRate.mark();
    long startTimeMs = System.currentTimeMillis();
    if (!isOpen) {
      exception = new RestServiceException("SecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (restRequest == null || callback == null) {
      throw new IllegalArgumentException("RestRequest or Callback is null");
    }
    frontendMetrics.securityServicePostProcessRequestTimeInMs.update(System.currentTimeMillis() - startTimeMs);
    callback.onCompletion(null, exception);
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
      String operationOrBlobId =
          RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
              frontendConfig.frontendPathPrefixesToRemove);
      if (operationOrBlobId.startsWith("/")) {
        operationOrBlobId = operationOrBlobId.substring(1);
      }
      if (blobInfo == null && !restRequest.getRestMethod().equals(RestMethod.OPTIONS)) {
        if (!operationOrBlobId.equals(Operations.GET_SIGNED_URL)) {
          throw new IllegalArgumentException("BlobInfo is null");
        }
      }
      try {
        GetBlobOptions options;
        responseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        RestMethod restMethod = restRequest.getRestMethod();
        switch (restMethod) {
          case HEAD:
            options = RestUtils.buildGetBlobOptions(restRequest.getArgs(), null, GetOption.None);
            responseChannel.setStatus(options.getRange() == null ? ResponseStatus.Ok : ResponseStatus.PartialContent);
            responseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
                new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
            setHeadResponseHeaders(blobInfo, options, restRequest, responseChannel);
            break;
          case GET:
            if (!operationOrBlobId.equals(Operations.GET_SIGNED_URL)) {
              responseChannel.setStatus(ResponseStatus.Ok);
              RestUtils.SubResource subResource = RestUtils.getBlobSubResource(restRequest);
              responseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
                  new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
              if (subResource == null) {
                Long ifModifiedSinceMs = getIfModifiedSinceMs(restRequest);
                if (ifModifiedSinceMs != null
                    && RestUtils.toSecondsPrecisionInMs(blobInfo.getBlobProperties().getCreationTimeInMs())
                    <= ifModifiedSinceMs) {
                  responseChannel.setStatus(ResponseStatus.NotModified);
                } else {
                  options = RestUtils.buildGetBlobOptions(restRequest.getArgs(), null, GetOption.None);
                  if (options.getRange() != null) {
                    responseChannel.setStatus(ResponseStatus.PartialContent);
                  }
                  setGetBlobResponseHeaders(blobInfo, options, responseChannel);
                }
                setCacheHeaders(restRequest, responseChannel);
              } else {
                if (subResource.equals(RestUtils.SubResource.BlobInfo)) {
                  setBlobPropertiesHeaders(blobInfo.getBlobProperties(), responseChannel);
                  setAccountAndContainerHeaders(restRequest, responseChannel);
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
            break;
          default:
            exception = new RestServiceException("Cannot process response for request with method " + restMethod,
                RestServiceErrorCode.InternalServerError);
        }
      } catch (RestServiceException e) {
        exception = e;
      }
    }
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
    restResponseChannel.setHeader(RestUtils.Headers.ACCEPT_RANGES, RestUtils.BYTE_RANGE_UNITS);
    long contentLength = blobProperties.getBlobSize();
    if (options.getRange() != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(options.getRange(), contentLength);
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_RANGE, rangeAndLength.getFirst());
      contentLength = rangeAndLength.getSecond();
    }
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, contentLength);
    setBlobPropertiesHeaders(blobProperties, restResponseChannel);
    setAccountAndContainerHeaders(restRequest, restResponseChannel);
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
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(options.getRange(), contentLength);
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_RANGE, rangeAndLength.getFirst());
      contentLength = rangeAndLength.getSecond();
    }
    if (contentLength < frontendConfig.frontendChunkedGetResponseThresholdInBytes) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, contentLength);
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
      // Ensure browsers do not execute html with embedded exploits.
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
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
      restResponseChannel.setHeader(RestUtils.Headers.EXPIRES,
          new Date(System.currentTimeMillis() + frontendConfig.frontendCacheValiditySeconds * Time.MsPerSec));
      restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL,
          "max-age=" + frontendConfig.frontendCacheValiditySeconds);
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
}
