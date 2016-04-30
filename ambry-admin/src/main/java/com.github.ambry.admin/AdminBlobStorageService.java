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
package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.AdminConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Admin specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Admin are supported here.
 */
class AdminBlobStorageService implements BlobStorageService {
  protected final static String ECHO = "echo";
  protected final static String GET_REPLICAS_FOR_BLOB_ID = "getReplicasForBlobId";

  private final static Set<String> adminOperations = new HashSet<String>();

  static {
    adminOperations.add(ECHO);
    adminOperations.add(GET_REPLICAS_FOR_BLOB_ID);
  }

  protected final AdminMetrics adminMetrics;

  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final long cacheValidityInSecs;
  private final Logger logger = LoggerFactory.getLogger(AdminBlobStorageService.class);

  /**
   * Create a new instance of AdminBlobStorageService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param adminConfig the configuration to use in the form of {@link AdminConfig}.
   * @param adminMetrics the metrics instance to use in the form of {@link AdminMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} instance to use to perform blob operations.
   */
  public AdminBlobStorageService(AdminConfig adminConfig, AdminMetrics adminMetrics, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router) {
    this.adminMetrics = adminMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.router = router;
    cacheValidityInSecs = adminConfig.adminCacheValiditySeconds;
    logger.trace("Instantiated AdminBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("AdminBlobStorageService has started");
  }

  @Override
  public void shutdown() {
    logger.info("AdminBlobStorageService shutdown complete");
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
      logger.trace("Handling GET request - {}", restRequest.getUri());
      String operationOrBlobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("GET operation/blob ID requested - {}", operationOrBlobId);
      ReadableStreamChannel response;
      if (adminOperations.contains(operationOrBlobId)) {
        if (operationOrBlobId.equals(ECHO)) {
          adminMetrics.echoRate.mark();
          restRequest.getMetricsTracker().injectMetrics(adminMetrics.echoMetrics);
          preProcessingTime = System.currentTimeMillis() - processingStartTime;
          response = EchoHandler.handleGetRequest(restRequest, restResponseChannel, adminMetrics);
          submitResponse(restRequest, restResponseChannel, response, null);
        } else if (operationOrBlobId.equals(GET_REPLICAS_FOR_BLOB_ID)) {
          adminMetrics.getReplicasForBlobIdRate.mark();
          restRequest.getMetricsTracker().injectMetrics(adminMetrics.getReplicasForBlobIdMetrics);
          preProcessingTime = System.currentTimeMillis() - processingStartTime;
          response =
              GetReplicasForBlobIdHandler.handleGetRequest(restRequest, restResponseChannel, clusterMap, adminMetrics);
          submitResponse(restRequest, restResponseChannel, response, null);
        } else {
          adminMetrics.missingOperationHandlerError.inc();
          RestServiceException exception =
              new RestServiceException("No handler available for valid admin operation - " + operationOrBlobId,
                  RestServiceErrorCode.ServiceUnavailable);
          submitResponse(restRequest, restResponseChannel, null, exception);
        }
      } else {
        adminMetrics.getBlobRate.mark();
        restRequest.getMetricsTracker().injectMetrics(adminMetrics.getBlobMetrics);
        logger.trace("Forwarding GET of {} to the router", operationOrBlobId);
        preProcessingTime = System.currentTimeMillis() - processingStartTime;
        HeadForGetCallback callback =
            new HeadForGetCallback(this, restRequest, restResponseChannel, router, cacheValidityInSecs);
        router.getBlobInfo(operationOrBlobId, callback);
      }
    } catch (Exception e) {
      adminMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      adminMetrics.getPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    adminMetrics.postBlobRate.mark();
    restRequest.getMetricsTracker().injectMetrics(adminMetrics.postBlobMetrics);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      adminMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      logger.trace("Forwarding POST to the router");
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      PostCallback callback = new PostCallback(this, restRequest, restResponseChannel, blobProperties);
      router.putBlob(blobProperties, usermetadata, restRequest, callback);
    } catch (Exception e) {
      adminMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      adminMetrics.postPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    adminMetrics.deleteBlobRate.mark();
    restRequest.getMetricsTracker().injectMetrics(adminMetrics.deleteBlobMetrics);
    try {
      logger.trace("Handling DELETE request - {}", restRequest.getUri());
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Forwarding DELETE of {} to the router", blobId);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      DeleteCallback callback = new DeleteCallback(this, restRequest, restResponseChannel);
      router.deleteBlob(blobId, callback);
    } catch (Exception e) {
      adminMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      adminMetrics.deletePreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(adminMetrics.headBlobMetrics);
    try {
      logger.trace("Handling HEAD request - {}", restRequest.getUri());
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Forwarding HEAD of {} to the router", blobId);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      HeadCallback callback = new HeadCallback(this, restRequest, restResponseChannel);
      router.getBlobInfo(blobId, callback);
    } catch (Exception e) {
      adminMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      adminMetrics.headPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  /**
   * Submits the  {@code response} (and any {@code exception})for the {@code restRequest} to the
   * {@code responseHandler}.
   * @param restRequest the {@link RestRequest} for which a a {@code response} is ready.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response can be sent.
   * @param response the response in the form of a {@link ReadableStreamChannel}.
   * @param exception any {@link Exception} that occurred during the handling of {@code restRequest}.
   */
  protected void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    } catch (RestServiceException e) {
      adminMetrics.responseSubmissionError.inc();
      if (exception != null) {
        logger.error("Error submitting response to response handler", e);
      } else {
        exception = e;
      }
      logger.error("Handling of request {} failed", restRequest.getUri(), exception);
      restResponseChannel.onResponseComplete(exception);

      if (response != null) {
        try {
          response.close();
        } catch (IOException ioe) {
          adminMetrics.resourceReleaseError.inc();
          logger.error("Error closing ReadableStreamChannel", e);
        }
      }
    }
  }

  /**
   * Checks for bad arguments or states.
   * @param restRequest the {@link RestRequest} to use. Cannot be null.
   * @param restResponseChannel the {@link RestResponseChannel} to use. Cannot be null.
   */
  private void handlePrechecks(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (restRequest == null || restResponseChannel == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received -");
      if (restRequest == null) {
        errorMessage.append(" [RestRequest] ");
      }
      if (restResponseChannel == null) {
        errorMessage.append(" [RestResponseChannel] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
  }

  /**
   * Looks at the URI to determine the type of operation required or the blob ID that an operation needs to be
   * performed on.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @return extracted operation type or blob ID from the uri.
   */
  protected static String getOperationOrBlobIdFromUri(RestRequest restRequest) {
    String path = restRequest.getPath();
    return (path.startsWith("/") ? path.substring(1, path.length()) : path);
  }
}

/**
 * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
 */
class HeadForGetCallback implements Callback<BlobInfo> {
  private final AdminBlobStorageService adminBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final Router router;
  private final long cacheValidityInSecs;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a HEAD before GET callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @param router the {@link Router} instance to use to make the GET call.
   * @param cacheValidityInSecs the period of validity of cache that needs to be sent to the client (in case of non
   *                            private blobs).
   */
  public HeadForGetCallback(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, Router router, long cacheValidityInSecs) {
    this.adminBlobStorageService = adminBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.router = router;
    this.cacheValidityInSecs = cacheValidityInSecs;
  }

  /**
   * Sets headers and makes a GET call if the result was not null. Otherwise bails out.
   * @param result The result of the request i.e a {@link BlobInfo} object with the properties of the blob that is going
   *               to be scheduled for GET. This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      adminBlobStorageService.adminMetrics.headForGetTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD before GET of {}", blobId);
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Setting response headers for {}", blobId);
        setResponseHeaders(result);
        logger.trace("Forwarding GET after HEAD for {} to the router", blobId);
        router.getBlob(blobId, new GetCallback(adminBlobStorageService, restRequest, restResponseChannel));
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for HeadForGetCallback");
      }
    } catch (Exception e) {
      adminBlobStorageService.adminMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      adminBlobStorageService.adminMetrics.headForGetCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        adminBlobStorageService.adminMetrics.operationError.inc();
        adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
      }
    }
  }

  /**
   * Sets the required headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(BlobInfo blobInfo)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED, new Date(blobProperties.getCreationTimeInMs()));
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
}

/**
 * Callback for GET operations. Submits the response received to an instance of {@link RestResponseHandler}.
 */
class GetCallback implements Callback<ReadableStreamChannel> {
  private final AdminBlobStorageService adminBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a GET callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public GetCallback(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.adminBlobStorageService = adminBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * Submits the GET response to {@link RestResponseHandler} so that it can be sent (or the exception handled).
   * @param result The result of the request. This is the actual blob data as a {@link ReadableStreamChannel}.
   *               This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(ReadableStreamChannel result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      adminBlobStorageService.adminMetrics.getTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for GET of {}", blobId);
      if (exception == null && result != null) {
        logger.trace("Successful GET of {}", blobId);
        restResponseChannel.setStatus(ResponseStatus.Ok);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for GetCallback");
      }
    } catch (Exception e) {
      adminBlobStorageService.adminMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      adminBlobStorageService.adminMetrics.getCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        adminBlobStorageService.adminMetrics.operationError.inc();
      }
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, result, exception);
    }
  }
}

/**
 * Callback for POST operations. Sends the response received to the client. Submits response either to handle exceptions
 * or to clean up after a response.
 */
class PostCallback implements Callback<String> {
  private final AdminBlobStorageService adminBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final BlobProperties blobProperties;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a POST callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
   */
  public PostCallback(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobProperties createdBlobProperties) {
    this.adminBlobStorageService = adminBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.blobProperties = createdBlobProperties;
  }

  /**
   * If there was no exception, updates the header with the location of the object. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This is the blob ID of the blob. This is non null if the request executed
   *               successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(String result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      adminBlobStorageService.adminMetrics.postTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      logger.trace("Callback received for POST");
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Successful POST of {}", result);
        setResponseHeaders(result);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for PostCallback");
      }
    } catch (Exception e) {
      adminBlobStorageService.adminMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      adminBlobStorageService.adminMetrics.postCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        adminBlobStorageService.adminMetrics.operationError.inc();
      }
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }

  /**
   * Sets the required headers in the response.
   * @param location the location of the created resource.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(String location)
      throws RestServiceException {
    restResponseChannel.setStatus(ResponseStatus.Created);
    restResponseChannel.setHeader(RestUtils.Headers.LOCATION, location);
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
    restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
  }
}

/**
 * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits response
 * either to handle exceptions or to clean up after a response.
 */
class DeleteCallback implements Callback<Void> {
  private final AdminBlobStorageService adminBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a DELETE callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public DeleteCallback(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.adminBlobStorageService = adminBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * If there was no exception, updates the header with the acceptance of the request. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This is always null.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(Void result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      adminBlobStorageService.adminMetrics.deleteTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for DELETE of {}", blobId);
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null) {
        logger.trace("Successful DELETE of {}", blobId);
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
      }
    } catch (Exception e) {
      adminBlobStorageService.adminMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      adminBlobStorageService.adminMetrics.deleteCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        adminBlobStorageService.adminMetrics.operationError.inc();
      }
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }
}

/**
 * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either to
 * handle exceptions or to clean up after a response.
 */
class HeadCallback implements Callback<BlobInfo> {
  private final AdminBlobStorageService adminBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a HEAD callback.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public HeadCallback(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.adminBlobStorageService = adminBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * If there was no exception, updates the header with the properties. Exceptions, if any, will be handled upon
   * submission.
   * @param result The result of the request i.e a {@link BlobInfo} object with the properties of the blob. This is
   *               non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      adminBlobStorageService.adminMetrics.headTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD of {}", blobId);
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Successful HEAD of {}", blobId);
        setBlobPropertiesResponseHeaders(result);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for HeadCallback");
      }
    } catch (Exception e) {
      adminBlobStorageService.adminMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      adminBlobStorageService.adminMetrics.headCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        adminBlobStorageService.adminMetrics.operationError.inc();
      }
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }

  /**
   * Sets the required blob properties headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setBlobPropertiesResponseHeaders(BlobInfo blobInfo)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setStatus(ResponseStatus.Ok);
    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, blobProperties.getBlobSize());

    // Blob props
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, blobProperties.getBlobSize());
    restResponseChannel.setHeader(RestUtils.Headers.SERVICE_ID, blobProperties.getServiceId());
    restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.PRIVATE, blobProperties.isPrivate());
    if (blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      restResponseChannel.setHeader(RestUtils.Headers.TTL, Long.toString(blobProperties.getTimeToLiveInSeconds()));
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE, blobProperties.getContentType());
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, blobProperties.getContentType());
    }
    if (blobProperties.getOwnerId() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.OWNER_ID, blobProperties.getOwnerId());
    }
  }
}
