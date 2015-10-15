package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestConstants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Admin specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Admin are supported here.
 */
class AdminBlobStorageService implements BlobStorageService {
  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private final Router router;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a new instance of AdminBlobStorageService by supplying it with config, metrics and a cluster map.
   * @param adminConfig the configuration to use in the form of {@link AdminConfig}.
   * @param adminMetrics the metrics instance to use in the form of {@link AdminMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   * @param router the {@link Router} instance to use to perform blob operations.
   */
  public AdminBlobStorageService(AdminConfig adminConfig, AdminMetrics adminMetrics, ClusterMap clusterMap,
      Router router) {
    this.adminConfig = adminConfig;
    this.adminMetrics = adminMetrics;
    this.clusterMap = clusterMap;
    this.router = router;
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
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    adminMetrics.getOperationRate.mark();
    handlePrechecks(restRequest, restResponseChannel, restResponseHandler);
    try {
      logger.trace("Handling GET request - {}", restRequest.getUri());
      String operationOrBlobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("GET operation requested - {}", operationOrBlobId);
      AdminOperationType operationType = AdminOperationType.getAdminOperationType(operationOrBlobId);
      ReadableStreamChannel response;
      switch (operationType) {
        case echo:
          response = EchoHandler.handleGetRequest(restRequest, restResponseChannel, adminMetrics);
          submitResponse(restRequest, restResponseChannel, restResponseHandler, response, null);
          break;
        case getReplicasForBlobId:
          response =
              GetReplicasForBlobIdHandler.handleGetRequest(restRequest, restResponseChannel, clusterMap, adminMetrics);
          submitResponse(restRequest, restResponseChannel, restResponseHandler, response, null);
          break;
        default:
          HeadForGetCallback callback =
              new HeadForGetCallback(restRequest, restResponseChannel, restResponseHandler, router);
          router.getBlobInfo(operationOrBlobId, callback);
      }
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, restResponseHandler, null, e);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    adminMetrics.postOperationRate.mark();
    handlePrechecks(restRequest, restResponseChannel, restResponseHandler);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      PostCallback callback = new PostCallback(restRequest, restResponseChannel, restResponseHandler, blobProperties);
      router.putBlob(blobProperties, usermetadata, restRequest, callback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, restResponseHandler, null, e);
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    adminMetrics.deleteOperationRate.mark();
    handlePrechecks(restRequest, restResponseChannel, restResponseHandler);
    try {
      logger.trace("Handling DELETE request - {}", restRequest.getUri());
      DeleteCallback callback = new DeleteCallback(restRequest, restResponseChannel, restResponseHandler);
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      router.deleteBlob(blobId, callback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, restResponseHandler, null, e);
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    adminMetrics.headOperationRate.mark();
    handlePrechecks(restRequest, restResponseChannel, restResponseHandler);
    try {
      logger.trace("Handling HEAD request - {}", restRequest.getUri());
      HeadCallback callback = new HeadCallback(restRequest, restResponseChannel, restResponseHandler);
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      router.getBlobInfo(blobId, callback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, restResponseHandler, null, e);
    }
  }

  /**
   * Checks for bad arguments or states.
   * @param restRequest the {@link RestRequest} to use. Cannot be null.
   * @param restResponseChannel the {@link RestResponseChannel} to use. Cannot be null.
   * @param restResponseHandler the {@link RestResponseHandler} to use}. Cannot be null.
   */
  private void handlePrechecks(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler) {
    if (restRequest == null || restResponseChannel == null || restResponseHandler == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received -");
      if (restRequest == null) {
        errorMessage.append(" [RestRequest] ");
      }
      if (restResponseChannel == null) {
        errorMessage.append(" [RestResponseChannel] ");
      }
      if (restResponseHandler == null) {
        errorMessage.append(" [RestResponseHandler] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
  }

  /**
   * Submits the  {@code response} (and any {@code exception})for the {@code restRequest} to the
   * {@code restResponseHandler}.
   * @param restRequest the {@link RestRequest} for which a a {@code response} is ready.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to use to send the response.
   * @param response the response in the form of a {@link ReadableStreamChannel}.
   * @param exception any {@link Exception} that occurred during the handling of {@code restRequest}.
   */
  private void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      RestResponseHandler restResponseHandler, ReadableStreamChannel response, Exception exception) {
    try {
      restResponseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    } catch (RestServiceException e) {
      restResponseChannel.onResponseComplete(exception);
      releaseResources(restRequest, response);
    }
  }

  /**
   * Cleans up resources.
   * @param restRequest the {@link RestRequest} that needs to be cleaned up.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that needs to be cleaned up. Can be null.
   */
  protected static void releaseResources(RestRequest restRequest, ReadableStreamChannel readableStreamChannel) {
    try {
      restRequest.close();
    } catch (IOException e) {
      // TODO: log and metrics
    }

    if (readableStreamChannel != null) {
      try {
        readableStreamChannel.close();
      } catch (IOException e) {
        // TODO: log and metrics
      }
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
  // TODO: this should be a config
  private static final long CACHE_VALIDITY_IN_SECONDS = 3600;

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
  public HeadForGetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
        String blobId = AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
        router.getBlob(blobId, new GetCallback(restRequest, restResponseChannel, restResponseHandler));
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
      AdminBlobStorageService.releaseResources(restRequest, null);
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
      // Ensure browsers do not execute html with embedded exploits.
      // TODO: do we need a MIME types class?
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
    if (blobProperties.isPrivate()) {
      restResponseChannel.setExpires(new Date(0));
      restResponseChannel.setCacheControl("private, no-cache, no-store, proxy-revalidate");
      restResponseChannel.setPragma("no-cache");
    } else {
      restResponseChannel.setExpires(new Date(System.currentTimeMillis() + CACHE_VALIDITY_IN_SECONDS * Time.MsPerSec));
      restResponseChannel.setCacheControl("max-age=" + CACHE_VALIDITY_IN_SECONDS);
    }
  }
}

/**
 * Callback for GET operations. Submits the response received to an instance of {@link RestResponseHandler}.
 */
class GetCallback implements Callback<ReadableStreamChannel> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a GET callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public GetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
      if (exception == null) {
        restResponseChannel.setStatus(ResponseStatus.Ok);
      } else if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      restResponseHandler.handleResponse(restRequest, restResponseChannel, result, exception);
    } catch (Exception e) {
      exception = exception == null ? e : exception;
      restResponseChannel.onResponseComplete(exception);
      AdminBlobStorageService.releaseResources(restRequest, result);
    }
  }
}

/**
 * Callback for POST operations. Sends the response received to the client. Submits response either to handle exceptions
 * or to clean up after a response.
 */
class PostCallback implements Callback<String> {
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
  public PostCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
      AdminBlobStorageService.releaseResources(restRequest, null);
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
class DeleteCallback implements Callback<Void> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a DELETE callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public DeleteCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
      AdminBlobStorageService.releaseResources(restRequest, null);
    }
  }
}

/**
 * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either to
 * handle exceptions or to clean up after a response.
 */
class HeadCallback implements Callback<BlobInfo> {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final RestResponseHandler restResponseHandler;

  /**
   * Create a HEAD callback.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param restResponseHandler the {@link RestResponseHandler} instance to submit the response to.
   */
  public HeadCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
      AdminBlobStorageService.releaseResources(restRequest, null);
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
    restResponseChannel.setStatus(ResponseStatus.Ok);
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
