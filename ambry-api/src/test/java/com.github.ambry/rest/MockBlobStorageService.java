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


/**
 * Implementation of the {@link BlobStorageService} that can be used in tests.
 */
public class MockBlobStorageService implements BlobStorageService {

  public final static String ECHO_REST_METHOD = "echoRestMethod";
  public final static String THROW_RUNTIME_EXCEPTION = "MBSSRuntimeException";
  public final static String SEND_RESPONSE_RUNTIME_EXCEPTION = "MBSSResponseRuntimeException";
  public final static String SEND_RESPONSE_REST_SERVICE_EXCEPTION = "MBSSRestServiceException";
  public final static String REST_ERROR_CODE = "mock.blob.storage.service.rest.error.code";

  private final Router router;

  private VerifiableProperties verifiableProperties;
  private volatile boolean serviceRunning = false;

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
   * @param router the {@link Router} that will back this instance.
   */
  public MockBlobStorageService(VerifiableProperties verifiableProperties, Router router) {
    setVerifiableProperties(verifiableProperties);
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
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (shouldProceed(restRequest, restResponseChannel, restResponseHandler)) {
      String blobId = getBlobId(restRequest);
      MockHeadForGetCallback callback =
          new MockHeadForGetCallback(restRequest, restResponseChannel, restResponseHandler, router);
      router.getBlobInfo(blobId, callback);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (shouldProceed(restRequest, restResponseChannel, restResponseHandler)) {
      try {
        BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
        byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
        router.putBlob(blobProperties, usermetadata, restRequest,
            new MockPostCallback(restRequest, restResponseChannel, restResponseHandler, blobProperties));
      } catch (RestServiceException e) {
        try {
          restResponseHandler.handleResponse(restRequest, restResponseChannel, null, e);
        } catch (RestServiceException ie) {
          releaseResources(restRequest, null);
          throw new IllegalStateException(ie);
        }
      }
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (shouldProceed(restRequest, restResponseChannel, restResponseHandler)) {
      String blobId = getBlobId(restRequest);
      router.deleteBlob(blobId, new MockDeleteCallback(restRequest, restResponseChannel, restResponseHandler));
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (shouldProceed(restRequest, restResponseChannel, restResponseHandler)) {
      String blobId = getBlobId(restRequest);
      router.getBlobInfo(blobId, new MockHeadCallback(restRequest, restResponseChannel, restResponseHandler));
    }
  }

  /**
   * Handles argument pre-checks and examines the URL to see if any custom operations need to be performed (which might
   * involve throwing exceptions).
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} that can be used to set headers.
   * @param restResponseHandler the {@link RestResponseHandler} that can be used to submit a response.
   * @return {@code true} if the pre-checks decided it is OK to continue. Otherwise {@code false}.
   */
  private boolean shouldProceed(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    boolean shouldProceed = canHonorRequest(restRequest, restResponseChannel, restResponseHandler);
    if (shouldProceed) {
      String uri = restRequest.getUri();
      // most testing Uri result in shouldProceed = false.
      shouldProceed = false;
      ReadableStreamChannel response = null;
      Exception exception = null;
      if (ECHO_REST_METHOD.equals(uri)) {
        ByteBuffer buffer = ByteBuffer.wrap(restRequest.getRestMethod().toString().getBytes());
        response = new ByteBufferRSC(buffer);
      } else if (THROW_RUNTIME_EXCEPTION.equals(uri)) {
        throw new RuntimeException(THROW_RUNTIME_EXCEPTION);
      } else if (SEND_RESPONSE_RUNTIME_EXCEPTION.equals(uri)) {
        exception = new RuntimeException(SEND_RESPONSE_RUNTIME_EXCEPTION);
      } else if (SEND_RESPONSE_REST_SERVICE_EXCEPTION.equals(uri)) {
        RestServiceErrorCode errorCode = RestServiceErrorCode.InternalServerError;
        try {
          errorCode = RestServiceErrorCode.valueOf(verifiableProperties.getString(REST_ERROR_CODE));
        } catch (IllegalArgumentException e) {
          // it's alright.
        }
        exception = new RestServiceException(SEND_RESPONSE_REST_SERVICE_EXCEPTION, errorCode);
      }
      try {
        if (exception == null) {
          restResponseChannel.setStatus(ResponseStatus.Ok);
        }
        restResponseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
      } catch (RestServiceException e) {
        releaseResources(restRequest, null);
      }
    }
    return shouldProceed;
  }

  /**
   * Performs null pre checks and checks that the service is running.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} that can be used to set headers.
   * @param restResponseHandler the {@link RestResponseHandler} that can be used to submit a response.
   * @throws IllegalArgumentException if any of {@code restRequest}, {@code restResponseChannel} or
   *                                    {@code restResponseHandler} is null.
   * @return {@code true} if the the service has the right arguments and is in a state to honor the request. Otherwise
   *          {@code false}.
   */
  private boolean canHonorRequest(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (restRequest == null || restResponseChannel == null || restResponseHandler == null) {
      throw new IllegalArgumentException("One of the arguments was null");
    } else if (!serviceRunning) {
      try {
        restResponseHandler.handleResponse(restRequest, restResponseChannel, null,
            new RestServiceException("BlobStorageService not running", RestServiceErrorCode.ServiceUnavailable));
      } catch (RestServiceException e) {
        releaseResources(restRequest, null);
      }
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
   * Cleans up resources.
   * @param restRequest the {@link RestRequest} that needs to be cleaned up.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that needs to be cleaned up. Can be null.
   */
  protected static void releaseResources(RestRequest restRequest, ReadableStreamChannel readableStreamChannel) {
    try {
      restRequest.close();
      if (readableStreamChannel != null) {
        readableStreamChannel.close();
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}

/**
 * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
 */
class MockHeadForGetCallback implements Callback<BlobInfo> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;
  private final Router router;

  /**
   * Create a HEAD before GET callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @param restResponseHandler the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param router the {@link Router} instance to use to make the GET call.
   *                      {@link BlobStorageService#handleGet(RestRequest, RestResponseChannel, RestResponseHandler)}.
   */
  public MockHeadForGetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler, Router router) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.restResponseHandler = restResponseHandler;
    this.router = router;
  }

  /**
   * Sets headers and makes a GET call if the result was not null. Otherwise bails out.
   * @param result The result of the request. This would be non null when the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    try {
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setResponseHeaders(result);
        String blobId = MockBlobStorageService.getBlobId(restRequest);
        router.getBlob(blobId, new MockGetCallback(restRequest, restResponseChannel, restResponseHandler));
      } else {
        if (exception != null && exception instanceof RouterException) {
          exception = new RestServiceException(exception,
              RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
        }
        restResponseHandler.handleResponse(restRequest, restResponseChannel, null, exception);
      }
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      MockBlobStorageService.releaseResources(restRequest, null);
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
    restResponseChannel.setLastModified(new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestConstants.Headers.Blob_Size, blobProperties.getBlobSize());
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setContentType(blobProperties.getContentType());
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
  }
}

/**
 * Callback for GET operations. Submits the response received to an instance of {@link RestResponseHandler}.
 */
class MockGetCallback implements Callback<ReadableStreamChannel> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a GET callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public MockGetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.restResponseHandler = restResponseHandler;
  }

  /**
   * Submits the GET response to a {@link RestResponseHandler} so that it can be sent (or the exception handled).
   * @param result The result of the request. This would be non null when the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(ReadableStreamChannel result, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      restResponseHandler.handleResponse(restRequest, restResponseChannel, result, exception);
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      MockBlobStorageService.releaseResources(restRequest, result);
    }
  }
}

/**
 * Callback for POST operations. Sends the response received to the client. Submits response either to handle exceptions
 * or to clean up after a response.
 */
class MockPostCallback implements Callback<String> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;
  private final BlobProperties blobProperties;

  /**
   * Create a POST callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
   */
  public MockPostCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler, BlobProperties createdBlobProperties) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.restResponseHandler = restResponseHandler;
    this.blobProperties = createdBlobProperties;
  }

  /**
   * If there was no exception, updates the header with the location of the object. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This would be non null when the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(String result, Exception exception) {
    try {
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setResponseHeaders(result);
      } else if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      restResponseHandler.handleResponse(restRequest, restResponseChannel, null, exception);
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      MockBlobStorageService.releaseResources(restRequest, null);
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
    restResponseChannel.setLocation(location);
    restResponseChannel.setContentLength(0);
    restResponseChannel.setHeader(RestConstants.Headers.Creation_Time, new Date(blobProperties.getCreationTimeInMs()));
  }
}

/**
 * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits response
 * either to handle exceptions or to clean up after a response.
 */
class MockDeleteCallback implements Callback<Void> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a DELETE callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public MockDeleteCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.restResponseHandler = restResponseHandler;
  }

  /**
   * If there was no exception, updates the header with the acceptance of the request. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This would be non null when the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(Void result, Exception exception) {
    try {
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null) {
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setContentLength(0);
      } else if (exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      restResponseHandler.handleResponse(restRequest, restResponseChannel, null, exception);
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      MockBlobStorageService.releaseResources(restRequest, null);
    }
  }
}

/**
 * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either to
 * handle exceptions or to clean up after a response.
 */
class MockHeadCallback implements Callback<BlobInfo> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a HEAD callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public MockHeadCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.restResponseHandler = restResponseHandler;
  }

  /**
   * If there was no exception, updates the header with the properties. Exceptions, if any, will be handled upon
   * submission.
   * @param result The result of the request. This would be non null when the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    try {
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        setResponseHeaders(result);
      } else if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      restResponseHandler.handleResponse(restRequest, restResponseChannel, null, exception);
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      MockBlobStorageService.releaseResources(restRequest, null);
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
    restResponseChannel.setLastModified(new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setContentLength(blobProperties.getBlobSize());

    // Blob props
    restResponseChannel.setHeader(RestConstants.Headers.Blob_Size, blobProperties.getBlobSize());
    restResponseChannel.setHeader(RestConstants.Headers.Service_Id, blobProperties.getServiceId());
    restResponseChannel.setHeader(RestConstants.Headers.Creation_Time, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestConstants.Headers.Private, blobProperties.isPrivate());
    if (blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      restResponseChannel.setHeader(RestConstants.Headers.TTL, Long.toString(blobProperties.getTimeToLiveInSeconds()));
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestConstants.Headers.Content_Type, blobProperties.getContentType());
      restResponseChannel.setContentType(blobProperties.getContentType());
    }
    if (blobProperties.getOwnerId() != null) {
      restResponseChannel.setHeader(RestConstants.Headers.Owner_Id, blobProperties.getOwnerId());
    }
    // TODO: send user metadata also as header after discussion with team.
  }
}
