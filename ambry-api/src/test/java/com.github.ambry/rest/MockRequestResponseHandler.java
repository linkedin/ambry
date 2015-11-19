package com.github.ambry.rest;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;


/**
 * Implementation of {@link RestRequestHandler} and {@link RestResponseHandler} that can be used in tests.
 * <p/>
 * This implementation simply calls the appropriate method (based on the {@link RestMethod} in the request) in the
 * underlying {@link BlobStorageService} on being asked to handle a request. If the implementation of
 * {@link BlobStorageService} is blocking, then this will be blocking too.
 * <p/>
 * Submitted responses also will be sent out immediately so response handling will block until the response has been
 * sent out completely.
 * <p/>
 * Be advised that this may not work if your test code *needs* the {@link RestRequestHandler} and
 * {@link RestResponseHandler} to be non-blocking. Test code with such assumptions may run into infinite loops.
 */
public class MockRequestResponseHandler implements RestRequestHandler, RestResponseHandler {
  public static String RUNTIME_EXCEPTION_ON_HANDLE = "runtime.exception.on.handle";
  public static String REST_EXCEPTION_ON_HANDLE = "rest.exception.on.handle";
  public static String CLOSE_REQUEST_ON_HANDLE = "close.request.on.handle";

  private boolean isRunning = false;
  private VerifiableProperties failureProperties = null;

  private BlobStorageService blobStorageService = null;

  @Override
  public void start()
      throws InstantiationException {
    if (blobStorageService == null) {
      throw new InstantiationException("BlobStorageService not set");
    }
    isRunning = true;
  }

  @Override
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Calls the appropriate method in the {@link BlobStorageService}. Non-blocking nature depends on the implementation
   * of the underlying {@link BlobStorageService}.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the request.
   */
  @Override
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (shouldProceed(restRequest, restResponseChannel)) {
      RestMethod restMethod = restRequest.getRestMethod();
      switch (restMethod) {
        case GET:
          blobStorageService.handleGet(restRequest, restResponseChannel);
          break;
        case POST:
          blobStorageService.handlePost(restRequest, restResponseChannel);
          break;
        case DELETE:
          blobStorageService.handleDelete(restRequest, restResponseChannel);
          break;
        case HEAD:
          blobStorageService.handleHead(restRequest, restResponseChannel);
          break;
        default:
          throw new RestServiceException("Unknown rest method - " + restMethod,
              RestServiceErrorCode.UnsupportedRestMethod);
      }
    }
  }

  /**
   * Sends out responses immediately. Returns control only after {@code readableStreamChannel} has been consumed and
   * sent over the {@code restResponseChannel}.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the
   *                                {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the request.
   */
  @Override
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (shouldProceed(restRequest, restResponseChannel)) {
      try {
        if (exception == null && response != null) {
          // BEWARE: test code with assumptions of non blocking behaviour in this class may run into an infinite loop.
          while (response.read(restResponseChannel) != -1) {
            ;
          }
        }
        restResponseChannel.onResponseComplete(exception);
        restRequest.close();
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        throw new RestServiceException(e, RestServiceErrorCode.InternalServerError);
      }
    }
  }

  /**
   * Makes the MockRequestResponseHandler faulty.
   * @param props failure properties. Defines the faulty behaviour. If null, there is no breakdown.
   */
  public void breakdown(VerifiableProperties props) {
    failureProperties = props;
  }

  /**
   * Fixes the MockRequestResponseHandler (not faulty anymore).
   */
  public void fix() {
    failureProperties = null;
  }

  /**
   * Sets the {@link BlobStorageService} that will be used.
   * @param blobStorageService the {@link BlobStorageService} instance to be used to process requests.
   */
  protected void setBlobStorageService(BlobStorageService blobStorageService) {
    this.blobStorageService = blobStorageService;
  }

  /**
   * If the MockRequestResponseHandler is supposed to be breakdown, throws the right exception.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @return {@code true} if the the caller can proceed. {@code false} otherwise.
   * @throws RestServiceException
   */
  private boolean shouldProceed(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (restRequest == null) {
      throw new IllegalArgumentException("RestRequest is null");
    } else if (restResponseChannel == null) {
      throw new IllegalArgumentException("RestResponseChannel is null");
    } else if (!isRunning) {
      throw new RestServiceException("MockRequestResponseHandler is not running",
          RestServiceErrorCode.ServiceUnavailable);
    } else if (failureProperties != null) {
      if (failureProperties.containsKey(RUNTIME_EXCEPTION_ON_HANDLE) && failureProperties
          .getBoolean(RUNTIME_EXCEPTION_ON_HANDLE)) {
        throw new RuntimeException(RUNTIME_EXCEPTION_ON_HANDLE);
      } else if (failureProperties.containsKey(REST_EXCEPTION_ON_HANDLE)) {
        RestServiceErrorCode errorCode = RestServiceErrorCode.InternalServerError;
        try {
          errorCode = RestServiceErrorCode.valueOf(failureProperties.getString(REST_EXCEPTION_ON_HANDLE));
        } catch (IllegalArgumentException e) {
          // it's alright.
        }
        throw new RestServiceException(REST_EXCEPTION_ON_HANDLE, errorCode);
      } else if (failureProperties.containsKey(CLOSE_REQUEST_ON_HANDLE) && failureProperties
          .getBoolean(CLOSE_REQUEST_ON_HANDLE)) {
        try {
          restRequest.close();
        } catch (IOException e) {
          // too bad.
        }
      }
    }
    return failureProperties == null;
  }
}



