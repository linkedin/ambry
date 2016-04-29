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
package com.github.ambry.rest;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.ByteBufferRSC;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.CountDownLatch;


/**
 * Implementation of the {@link BlobStorageService} that can be used in tests.
 */
public class MockBlobStorageService implements BlobStorageService {

  public final static String ECHO_REST_METHOD = "mbssEchoRestMethod";
  public final static String THROW_RUNTIME_EXCEPTION = "mbssRuntimeException";
  public final static String SEND_RESPONSE_RUNTIME_EXCEPTION = "mbssResponseRuntimeException";
  public final static String SEND_RESPONSE_REST_SERVICE_EXCEPTION = "mbssRestServiceException";
  public final static String REST_ERROR_CODE = "mock.blob.storage.service.rest.error.code";

  private final RestResponseHandler responseHandler;
  private final Router router;

  private VerifiableProperties verifiableProperties;
  private volatile boolean serviceRunning = false;
  private volatile boolean blocking = false;
  private volatile CountDownLatch blockLatch = new CountDownLatch(0);

  /**
   * Changes the {@link VerifiableProperties} instance with this instance so that the behaviour can be changed on the
   * fly.
   * @param verifiableProperties the{@link VerifiableProperties} that will dictate behaviour.
   */
  public void setVerifiableProperties(VerifiableProperties verifiableProperties) {
    this.verifiableProperties = verifiableProperties;
  }

  /**
   * Creates an instance of {@link MockBlobStorageService} with {@code router} as the backing {@link Router} and
   * {@code verifiableProperties} defining the behavior of this instance.
   * @param verifiableProperties the {@link VerifiableProperties} that defines the behavior of this instance.
   * @param responseHandler the {@link RestResponseHandler} instance to use.
   * @param router the {@link Router} that will back this instance.
   */
  public MockBlobStorageService(VerifiableProperties verifiableProperties, RestResponseHandler responseHandler,
      Router router) {
    setVerifiableProperties(verifiableProperties);
    this.responseHandler = responseHandler;
    this.router = router;
  }

  @Override
  public void start()
      throws InstantiationException {
    serviceRunning = true;
  }

  @Override
  public void shutdown() {
    serviceRunning = false;
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (shouldProceed(restRequest, restResponseChannel)) {
      String blobId = getBlobId(restRequest);
      MockHeadForGetCallback callback = new MockHeadForGetCallback(this, restRequest, restResponseChannel, router);
      router.getBlobInfo(blobId, callback);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (shouldProceed(restRequest, restResponseChannel)) {
      try {
        BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
        byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
        router.putBlob(blobProperties, usermetadata, restRequest,
            new MockPostCallback(this, restRequest, restResponseChannel, blobProperties));
      } catch (RestServiceException e) {
        handleResponse(restRequest, restResponseChannel, null, e);
      }
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (shouldProceed(restRequest, restResponseChannel)) {
      String blobId = getBlobId(restRequest);
      router.deleteBlob(blobId, new MockDeleteCallback(this, restRequest, restResponseChannel));
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (shouldProceed(restRequest, restResponseChannel)) {
      String blobId = getBlobId(restRequest);
      router.getBlobInfo(blobId, new MockHeadCallback(this, restRequest, restResponseChannel));
    }
  }

  /**
   * All operations block until {@link #releaseAllOperations()} is called.
   * @throws IllegalStateException each call to this function must (eventually) be followed by a call to
   *                               {@link #releaseAllOperations()}. If this function is invoked more than once before an
   *                               accompanying {@link #releaseAllOperations()} is called, it is illegal state.
   */
  public void blockAllOperations() {
    if (blocking) {
      throw new IllegalStateException("Already in blocking state");
    } else {
      blocking = true;
      blockLatch = new CountDownLatch(1);
    }
  }

  /**
   * Releases all blocked operations.
   */
  public void releaseAllOperations() {
    blockLatch.countDown();
    blocking = false;
  }

  /**
   * Handles argument pre-checks and examines the URL to see if any custom operations need to be performed (which might
   * involve throwing exceptions).
   * <p/>
   * Also blocks if required.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} that can be used to set headers.
   * @return {@code true} if the pre-checks decided it is OK to continue. Otherwise {@code false}.
   */
  private boolean shouldProceed(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (blocking) {
      try {
        blockLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    boolean shouldProceed = canHonorRequest(restRequest, restResponseChannel);
    if (shouldProceed) {
      String uri = restRequest.getUri();
      ReadableStreamChannel response = null;
      Exception exception = null;
      if (uri.startsWith(ECHO_REST_METHOD)) {
        String responseStr = restRequest.getRestMethod().toString() + uri.substring(ECHO_REST_METHOD.length());
        ByteBuffer buffer = ByteBuffer.wrap(responseStr.getBytes());
        response = new ByteBufferRSC(buffer);
        shouldProceed = false;
      } else if (THROW_RUNTIME_EXCEPTION.equals(uri)) {
        throw new RuntimeException(THROW_RUNTIME_EXCEPTION);
      } else if (SEND_RESPONSE_RUNTIME_EXCEPTION.equals(uri)) {
        shouldProceed = false;
        exception = new RuntimeException(SEND_RESPONSE_RUNTIME_EXCEPTION);
      } else if (SEND_RESPONSE_REST_SERVICE_EXCEPTION.equals(uri)) {
        shouldProceed = false;
        RestServiceErrorCode errorCode = RestServiceErrorCode.InternalServerError;
        try {
          errorCode = RestServiceErrorCode.valueOf(verifiableProperties.getString(REST_ERROR_CODE));
        } catch (IllegalArgumentException e) {
          // it's alright.
        }
        exception = new RestServiceException(SEND_RESPONSE_REST_SERVICE_EXCEPTION, errorCode);
      }
      if (!shouldProceed) {
        try {
          if (exception == null) {
            restResponseChannel.setStatus(ResponseStatus.Ok);
          }
        } catch (RestServiceException e) {
          exception = e;
        } finally {
          handleResponse(restRequest, restResponseChannel, response, exception);
        }
      }
    }
    return shouldProceed;
  }

  /**
   * Performs null pre checks and checks that the service is running.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} that can be used to set headers.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel}is null.
   * @return {@code true} if the the service has the right arguments and is in a state to honor the request. Otherwise
   *          {@code false}.
   */
  private boolean canHonorRequest(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (restRequest == null || restResponseChannel == null) {
      throw new IllegalArgumentException("One of the arguments was null");
    } else if (!serviceRunning) {
      handleResponse(restRequest, restResponseChannel, null,
          new RestServiceException("BlobStorageService not running", RestServiceErrorCode.ServiceUnavailable));
    }
    return serviceRunning;
  }

  /**
   * Determines the blob ID desired by the request.
   * @param restRequest a {@link RestRequest} that represents the request.
   * @return the blob ID desired by the request.
   */
  protected static String getBlobId(RestRequest restRequest) {
    String path = restRequest.getPath();
    return path.startsWith("/") ? path.substring(1, path.length()) : path;
  }

  /**
   * Sends out responses immediately. Returns control only after {@code response} has been consumed and sent over the
   * {@code restResponseChannel}.
   * <p/>
   * If the response building was unsuccessful for any reason, the details are included in the {@code exception}.
   * <p/>
   * The bytes consumed from the {@code response} are streamed out (unmodified) through the {@code restResponseChannel}.
   * <p/>
   * Assumed that at least one of {@code response} or {@code exception} is null.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   */
  protected void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    } catch (RestServiceException e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      if (response != null) {
        try {
          response.close();
        } catch (IOException ioe) {
          throw new IllegalStateException(ioe);
        }
      }
    }
  }
}

/**
 * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
 */
class MockHeadForGetCallback implements Callback<BlobInfo> {
  private final MockBlobStorageService mockBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final Router router;

  /**
   * Create a HEAD before GET callback.
   * @param mockBlobStorageService the {@link MockBlobStorageService} to use to submit responses.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @param router the {@link Router} instance to use to make the GET call.
   */
  public MockHeadForGetCallback(MockBlobStorageService mockBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, Router router) {
    this.mockBlobStorageService = mockBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.router = router;
  }

  /**
   * Sets headers and makes a GET call if the result was not null. Otherwise bails out.
   * @param result The result of the request - a {@link BlobInfo} object with the blob properties and other headers of
   *               the blob that is going to be scheduled for GET. This is non null if the request executed
   *               successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    try {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setResponseHeaders(result);
        String blobId = MockBlobStorageService.getBlobId(restRequest);
        router.getBlob(blobId, new MockGetCallback(mockBlobStorageService, restRequest, restResponseChannel));
      } else {
        if (exception != null && exception instanceof RouterException) {
          exception = new RestServiceException(exception,
              RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
        }
        mockBlobStorageService.handleResponse(restRequest, restResponseChannel, null, exception);
      }
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      mockBlobStorageService.handleResponse(restRequest, restResponseChannel, null, exception);
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
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
  }
}

/**
 * Callback for GET operations.
 */
class MockGetCallback implements Callback<ReadableStreamChannel> {
  private final MockBlobStorageService mockBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;

  /**
   * Create a GET callback.
   * @param mockBlobStorageService the {@link MockBlobStorageService} to use to submit responses.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public MockGetCallback(MockBlobStorageService mockBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.mockBlobStorageService = mockBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * Sends the GET response to the client (or sends an appropriate error).
   * @param result The result of the request. This is the actual blob data as a {@link ReadableStreamChannel}.
   *               This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(ReadableStreamChannel result, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
    } catch (Exception e) {
      exception = e;
    } finally {
      mockBlobStorageService.handleResponse(restRequest, restResponseChannel, result, exception);
    }
  }
}

/**
 * Callback for POST operations. Sends the blob ID of the created blob to the client.
 */
class MockPostCallback implements Callback<String> {
  private final MockBlobStorageService mockBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final BlobProperties blobProperties;

  /**
   * Create a POST callback.
   * @param mockBlobStorageService the {@link MockBlobStorageService} to use to submit responses.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
   */
  public MockPostCallback(MockBlobStorageService mockBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobProperties createdBlobProperties) {
    this.mockBlobStorageService = mockBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.blobProperties = createdBlobProperties;
  }

  /**
   * If there was no exception, updates the header with the location of the object.
   * @param result The result of the request. This is the blob ID of the blob. This is non null if the request executed
   *               successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(String result, Exception exception) {
    try {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setResponseHeaders(result);
      } else if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
    } catch (Exception e) {
      exception = exception == null ? e : exception;
    } finally {
      mockBlobStorageService.handleResponse(restRequest, restResponseChannel, null, exception);
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
 * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful.
 */
class MockDeleteCallback implements Callback<Void> {
  private final MockBlobStorageService mockBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;

  /**
   * Create a DELETE callback.
   * @param mockBlobStorageService the {@link MockBlobStorageService} to use to submit responses.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public MockDeleteCallback(MockBlobStorageService mockBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.mockBlobStorageService = mockBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * If there was no exception, updates the header with the acceptance of the request.
   * @param result The result of the request. This is always null.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(Void result, Exception exception) {
    try {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null) {
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
      } else if (exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
    } catch (Exception e) {
      exception = exception == null ? e : exception;
    } finally {
      mockBlobStorageService.handleResponse(restRequest, restResponseChannel, null, exception);
    }
  }
}

/**
 * Callback for HEAD operations. Sends the headers to the client if operation is successful.
 */
class MockHeadCallback implements Callback<BlobInfo> {
  private final MockBlobStorageService mockBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;

  /**
   * Create a HEAD callback.
   * @param mockBlobStorageService the {@link MockBlobStorageService} to use to submit responses.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public MockHeadCallback(MockBlobStorageService mockBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.mockBlobStorageService = mockBlobStorageService;
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
    try {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setBlobPropertiesResponseHeaders(result);
      } else if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
    } catch (Exception e) {
      exception = exception == null ? e : exception;
    } finally {
      mockBlobStorageService.handleResponse(restRequest, restResponseChannel, null, exception);
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
