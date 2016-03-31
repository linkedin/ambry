package com.github.ambry.frontend;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.Future;


/**
 * Default implementation of {@link SecurityService} for Ambry that doesn't do any validations, but just
 * sets the respective headers on response.
 */
class AmbrySecurityService implements SecurityService {

  private boolean isOpen;
  private final long cacheValidityInSecs;
  private final FrontendMetrics frontendMetrics;

  public AmbrySecurityService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics) {
    cacheValidityInSecs = frontendConfig.frontendCacheValiditySeconds;
    this.frontendMetrics = frontendMetrics;
    isOpen = true;
  }

  @Override
  public Future<Void> processRequest(RestRequest restRequest, Callback<Void> callback) {
    Exception exception = null;
    if (!isOpen) {
      exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
    } else {
      if (restRequest == null) {
        throw new IllegalArgumentException("RestRequest is null");
      }
    }
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (callback != null) {
      callback.onCompletion(null, exception);
    }
    futureResult.done(null, exception);
    return futureResult;
  }

  @Override
  public Future<Void> processResponse(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo,
      Callback<Void> callback) {
    Exception exception = null;
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (!isOpen) {
      exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
    } else {
      if (restRequest == null || responseChannel == null || blobInfo == null) {
        throw new IllegalArgumentException("One of the required params is null");
      }
      try {
        responseChannel.setStatus(ResponseStatus.Ok);
        responseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        responseChannel
            .setHeader(RestUtils.Headers.LAST_MODIFIED, new Date(blobInfo.getBlobProperties().getCreationTimeInMs()));
        if (restRequest.getRestMethod() == RestMethod.HEAD) {
          setHeadResponseHeaders(blobInfo, responseChannel);
        } else if (restRequest.getRestMethod() == RestMethod.GET) {
          RestUtils.SubResource subResource = RestUtils.getBlobSubResource(restRequest);
          if (subResource == null) {
            setGetBlobResponseHeaders(responseChannel, blobInfo);
          } else if (subResource == RestUtils.SubResource.BlobInfo) {
            setBlobPropertiesHeaders(blobInfo.getBlobProperties(), responseChannel);
          }
        }
      } catch (RestServiceException e) {
        exception = e;
      }
    }
    futureResult.done(null, exception);
    if (callback != null) {
      callback.onCompletion(null, exception);
    }
    return futureResult;
  }

  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Sets the required headers in the HEAD response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setHeadResponseHeaders(BlobInfo blobInfo, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, blobProperties.getBlobSize());
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
    }
    setBlobPropertiesHeaders(blobProperties, restResponseChannel);
  }

  /**
   * Sets the required headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setGetBlobResponseHeaders(RestResponseChannel restResponseChannel, BlobInfo blobInfo)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, blobProperties.getBlobSize());
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
      // Ensure browsers do not execute html with embedded exploits.
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
    if (blobProperties.isPrivate()) {
      restResponseChannel.setHeader(RestUtils.Headers.EXPIRES, new Date(0));
      restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL, "private, no-cache, no-store, proxy-revalidate");
      restResponseChannel.setHeader(RestUtils.Headers.PRAGMA, "no-cache");
    } else {
      restResponseChannel.setHeader(RestUtils.Headers.EXPIRES,
          new Date(System.currentTimeMillis() + cacheValidityInSecs * Time.MsPerSec));
      restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL, "max-age=" + cacheValidityInSecs);
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
    restResponseChannel.setHeader(RestUtils.Headers.PRIVATE, blobProperties.isPrivate());
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
}
