package com.github.ambry.rest;

import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Asynchronously handles requests and responses that are submitted.
 * <p/>
 * Requests are submitted by a {@link NioServer} and asynchronously routed to a {@link BlobStorageService}. Responses
 * are usually submitted from beyond the {@link BlobStorageService} layer and asynchronously sent to the client. In both
 * pathways, this class enables a non-blocking paradigm.
 * <p/>
 * Maintains multiple "workers" internally that run continuously to handle submitted requests.
 * <p/>
 * Requests are queued on submission and handed off to the {@link BlobStorageService} when they are dequeued. Responses
 * are sent to the client via the appropriate {@link RestResponseChannel} and callbacks/errors are handled.
 * <p/>
 * These are the scaling units of the {@link RestServer} and can be scaled up and down independently of any other
 * component of the {@link RestServer}.
 */
public class AsyncRequestResponseHandler implements RestRequestHandler, RestResponseHandler {
  private final RestServerMetrics restServerMetrics;

  private final List<AsyncRequestWorker> asyncRequestWorkers = new ArrayList<AsyncRequestWorker>();
  private final AtomicInteger currIndex = new AtomicInteger(0);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private AsyncResponseHandler asyncResponseHandler = null;
  private BlobStorageService blobStorageService = null;
  private int requestWorkersCount = 0;
  private volatile boolean isRunning = false;

  /**
   * Builds a AsyncRequestResponseHandler.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use to track metrics.
   */
  protected AsyncRequestResponseHandler(RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    restServerMetrics.trackAsyncRequestResponseHandler(this);
    logger.trace("Instantiated AsyncRequestResponseHandler");
  }

  /**
   * Does startup tasks for the AsyncRequestResponseHandler. When the function returns, startup is FULLY complete.
   */
  @Override
  public void start() {
    long startupBeginTime = System.currentTimeMillis();
    try {
      if (!isRunning()) {
        logger.info("Starting AsyncRequestResponseHandler with {} request workers", requestWorkersCount);
        for (int i = 0; i < requestWorkersCount; i++) {
          long workerStartupBeginTime = System.currentTimeMillis();
          AsyncRequestWorker asyncRequestWorker = new AsyncRequestWorker(restServerMetrics, blobStorageService);
          asyncRequestWorkers.add(asyncRequestWorker);
          Utils.newThread("RequestWorker-" + i, asyncRequestWorker, false).start();
          long workerStartupTime = System.currentTimeMillis() - workerStartupBeginTime;
          restServerMetrics.requestWorkerStartTimeInMs.update(workerStartupTime);
          logger.info("AsyncRequestWorker startup took {} ms", workerStartupTime);
        }
        asyncResponseHandler = new AsyncResponseHandler(restServerMetrics);
        isRunning = true;
      }
    } finally {
      long startupTime = System.currentTimeMillis() - startupBeginTime;
      restServerMetrics.requestResponseHandlerStartTimeInMs.update(startupTime);
      logger.info("AsyncRequestResponseHandler start took {} ms", startupTime);
    }
  }

  /**
   * Does shutdown tasks for the AsyncRequestResponseHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any requests/responses in flight during shutdown might be dropped.
   */
  @Override
  public void shutdown() {
    long shutdownBeginTime = System.currentTimeMillis();
    try {
      if (isRunning()) {
        isRunning = false;
        logger.info("Shutting down AsyncRequestResponseHandler");
        for (AsyncRequestWorker asyncRequestWorker : asyncRequestWorkers) {
          try {
            long workerShutdownBeginTime = System.currentTimeMillis();
            if (!asyncRequestWorker.shutdown(30, TimeUnit.SECONDS)) {
              logger.error("Shutdown of AsyncRequestWorker failed. This should not happen");
              restServerMetrics.requestResponseHandlerShutdownError.inc();
            }
            long workerShutdownTime = System.currentTimeMillis() - workerShutdownBeginTime;
            restServerMetrics.requestWorkerShutdownTimeInMs.update(workerShutdownTime);
            logger.info("AsyncRequestWorker shutdown took {} ms", workerShutdownTime);
          } catch (InterruptedException e) {
            logger.error("Await shutdown of AsyncRequestWorker was interrupted. It might not have shutdown", e);
            restServerMetrics.requestResponseHandlerShutdownError.inc();
          }
        }
        asyncResponseHandler.close();
      }
    } finally {
      long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
      logger.info("AsyncRequestResponseHandler shutdown took {} ms", shutdownTime);
      restServerMetrics.requestResponseHandlerShutdownTimeInMs.update(shutdownTime);
    }
  }

  /**
   * Queues the {@code restRequest} to be handled async. When this function returns, it may not be handled yet. When
   * the response is ready, {@link RestResponseChannel} will be used to send the response.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is a problem queuing the request.
   */
  @Override
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (isRunning() && requestWorkersCount > 0) {
      getWorker().submitRequest(restRequest, restResponseChannel);
    } else {
      restServerMetrics.requestResponseHandlerUnavailableError.inc();
      throw new RestServiceException(
          "Requests cannot be handled because the AsyncRequestResponseHandler is not available",
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Submit a response for a request along with a channel over which the response can be sent. If the response building
   * was unsuccessful for any reason, the details should be included in the {@code exception}.
   * <p/>
   * The bytes consumed from the {@code response} are streamed out (unmodified) through the {@code restResponseChannel}
   * asynchronously.
   * <p/>
   * Assumed that at least one of {@code response} or {@code exception} is null.
   * <p/>
   * When this function returns, the response may not be sent yet.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the
   *                                {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while queuing the response.
   */
  @Override
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (isRunning()) {
      asyncResponseHandler.submitResponse(restRequest, restResponseChannel, response, exception);
    } else {
      restServerMetrics.requestResponseHandlerUnavailableError.inc();
      throw new RestServiceException(
          "Requests cannot be handled because the AsyncRequestResponseHandler is not available",
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Sets the number of request handling units and the {@link BlobStorageService} that will be used in
   * {@link AsyncRequestWorker} instances..
   * @param workerCount the required number of request handling units.
   * @param blobStorageService the {@link BlobStorageService} instance to be used to process requests.
   * @throws IllegalArgumentException if {@code workerCount} < 0 or if {@code workerCount} > 0 but
   *                                  {@code blobStorageService} is null.
   * @throws IllegalStateException if {@link #start()} has already been called before a call to this function.
   */
  protected void setupRequestHandling(int workerCount, BlobStorageService blobStorageService) {
    if (isRunning()) {
      throw new IllegalStateException("Cannot modify scaling unit count after the service has started");
    } else if (workerCount < 0) {
      throw new IllegalArgumentException("Request worker workerCount has to be >= 0");
    } else if (workerCount > 0 && blobStorageService == null) {
      throw new IllegalArgumentException("BlobStorageService cannot be null");
    }
    requestWorkersCount = workerCount;
    this.blobStorageService = blobStorageService;
    logger.trace("Request handling units count set to {}", requestWorkersCount);
  }

  /**
   * Used to query whether the AsyncRequestResponseHandler is currently in a state to handle submitted
   * requests/responses.
   * @return {@code true} if in a state to handle submitted requests/responses. {@code false} otherwise.
   */
  protected boolean isRunning() {
    return isRunning;
  }

  /**
   * Gets total number of requests waiting to be processed in all workers.
   * @return total size of request queue across all workers.
   */
  protected int getRequestQueueSize() {
    int requestQueueSize = 0;
    for (AsyncRequestWorker asyncRequestWorker : asyncRequestWorkers) {
      requestQueueSize += asyncRequestWorker.getRequestQueueSize();
    }
    return requestQueueSize;
  }

  /**
   * Gets total number of responses being (or waiting to be) sent.
   * @return total size of response map/set.
   */
  protected int getResponseSetSize() {
    int responseSetSize = 0;
    if (asyncResponseHandler != null) {
      responseSetSize = asyncResponseHandler.getResponseSetSize();
    }
    return responseSetSize;
  }

  /**
   * Returns how many {@link AsyncRequestWorker}s are alive and well.
   * @return number of {@link AsyncRequestWorker}s alive and well.
   */
  protected int getWorkersAlive() {
    int count = 0;
    for (int i = 0; i < asyncRequestWorkers.size(); i++) {
      if (asyncRequestWorkers.get(i).isRunning()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Returns a {@link AsyncRequestWorker} that can be used to handle requests.
   * @return a {@link AsyncRequestResponseHandler} that can be used to handle requests.
   */
  private AsyncRequestWorker getWorker() {
    long startTime = System.currentTimeMillis();
    int absIndex = currIndex.getAndIncrement();
    int realIndex = absIndex % requestWorkersCount;
    logger.trace("Monotonically increasing value {} was used to pick worker at index {}", absIndex, realIndex);
    AsyncRequestWorker worker = asyncRequestWorkers.get(realIndex);
    restServerMetrics.requestWorkerSelectionTimeInMs.update(System.currentTimeMillis() - startTime);
    return worker;
  }
}

/**
 * Thread that handles the queuing and processing of requests.
 */
class AsyncRequestWorker implements Runnable {
  private final RestServerMetrics restServerMetrics;
  private final BlobStorageService blobStorageService;
  private final LinkedBlockingQueue<AsyncRequestInfo> requests = new LinkedBlockingQueue<AsyncRequestInfo>();
  private final AtomicInteger queuedRequestCount = new AtomicInteger(0);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a worker that can process requests.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use to track metrics.
   */
  protected AsyncRequestWorker(RestServerMetrics restServerMetrics, BlobStorageService blobStorageService) {
    this.restServerMetrics = restServerMetrics;
    this.blobStorageService = blobStorageService;
    restServerMetrics.registerRequestWorker(this);
    logger.trace("Instantiated AsyncRequestWorker");
  }

  /**
   * Handles queued requests continuously until shutdown.
   */
  @Override
  public void run() {
    logger.trace("AsyncRequestWorker started");
    AsyncRequestInfo requestInfo = null;
    try {
      while (isRunning()) {
        try {
          requestInfo = requests.take();
          if (requestInfo.restRequest != null) {
            processRequest(requestInfo);
            logger.trace("Request {} was processed successfully", requestInfo.restRequest.getUri());
          } else {
            break;
          }
        } catch (Exception e) {
          restServerMetrics.requestProcessingError.inc();
          if (requestInfo != null) {
            onProcessingFailure(requestInfo.restRequest, requestInfo.restResponseChannel, e);
          } else {
            logger.error("Unexpected exception while processing requests", e);
          }
        }
      }
    } finally {
      running.set(false);
      discardRequests();
      logger.trace("AsyncRequestWorker stopped");
      shutdownLatch.countDown();
    }
  }

  /**
   * Marks that shutdown is required and waits for the shutdown of this instance for the specified time.
   * <p/>
   * All requests still in the queue will be discarded.
   * @param timeout the amount of time to wait for shutdown.
   * @param timeUnit time unit of {@code timeout}.
   * @return {@code true} if shutdown succeeded within the {@code timeout}. {@code false} otherwise.
   * @throws InterruptedException if the wait for shutdown is interrupted.
   */
  protected boolean shutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    logger.trace("Shutting down AsyncRequestWorker");
    running.set(false);
    requests.offer(new AsyncRequestInfo(null, null));
    return shutdownLatch.await(timeout, timeUnit);
  }

  /**
   * Queues the {@code restRequest} to be handled async. When this function returns, it may not be handled yet.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if the service is unavailable or if there is a problem queuing the request.
   */
  protected void submitRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    long processingStartTime = System.currentTimeMillis();
    if (restRequest == null || restResponseChannel == null) {
      throw new IllegalArgumentException("Received one or more null arguments");
    }
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restServerMetrics.requestArrivalRate.mark();
    try {
      logger.trace("Queuing request {}", restRequest.getUri());
      AsyncRequestInfo requestInfo = new AsyncRequestInfo(restRequest, restResponseChannel);
      boolean added = false;
      RestServiceException exception = null;
      try {
        added = requests.add(requestInfo);
      } catch (Exception e) {
        exception = new RestServiceException("Attempt to add request failed", e,
            RestServiceErrorCode.RequestResponseQueuingFailure);
      }
      if (added) {
        queuedRequestCount.incrementAndGet();
        logger.trace("Queued request {}", restRequest.getUri());
        restServerMetrics.requestQueuingRate.mark();
      } else {
        restServerMetrics.requestQueueAddError.inc();
        if (exception == null) {
          exception = new RestServiceException("Attempt to add request failed",
              RestServiceErrorCode.RequestResponseQueuingFailure);
        }
        throw exception;
      }
    } finally {
      long preProcessingTime = System.currentTimeMillis() - processingStartTime;
      restServerMetrics.requestPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().scalingMetricsTracker.addToRequestProcessingTime(preProcessingTime);
    }
  }

  /**
   * Information on whether this instance is accepting requests and responses. This will return {@code false} as soon as
   * {@link #shutdown(long, TimeUnit)} is called whether or not the instance has actually stopped working.
   * @return {@code true} if in a state to receive requests/responses. {@code false} otherwise.
   */
  protected boolean isRunning() {
    return running.get();
  }

  /**
   * Gets number of requests waiting to be processed.
   * @return size of request queue.
   */
  protected int getRequestQueueSize() {
    return queuedRequestCount.get();
  }

  /**
   * Processes the {@code asyncRequestInfo}. Discerns the type of {@link RestMethod} in the request and calls the right
   * function of the {@link BlobStorageService}.
   * @param asyncRequestInfo the currently dequeued {@link AsyncRequestInfo}.
   */
  private void processRequest(AsyncRequestInfo asyncRequestInfo) {
    long processingStartTime = System.currentTimeMillis();
    // needed to avoid double counting.
    long blobStorageProcessingTime = 0;
    RestRequest restRequest = asyncRequestInfo.restRequest;
    try {
      onRequestDequeue(asyncRequestInfo);
      RestResponseChannel restResponseChannel = asyncRequestInfo.restResponseChannel;
      RestMethod restMethod = restRequest.getRestMethod();
      logger.trace("Processing request {} with RestMethod {}", restRequest.getUri(), restMethod);
      long blobStorageProcessingStartTime = System.currentTimeMillis();
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
          restServerMetrics.unknownRestMethodError.inc();
          RestServiceException e = new RestServiceException("Unsupported REST method: " + restMethod,
              RestServiceErrorCode.UnsupportedRestMethod);
          onProcessingFailure(restRequest, restResponseChannel, e);
      }
      blobStorageProcessingTime = System.currentTimeMillis() - blobStorageProcessingStartTime;
    } finally {
      restRequest.getMetricsTracker().scalingMetricsTracker
          .addToRequestProcessingTime(System.currentTimeMillis() - processingStartTime - blobStorageProcessingTime);
    }
  }

  /**
   * Called on shutdown and empties the remaining requests and releases resources held by them.
   */
  private void discardRequests() {
    logger.trace("Discarding requests on account of shutdown");
    RestServiceException e = new RestServiceException("Service shutdown", RestServiceErrorCode.ServiceUnavailable);
    AsyncRequestInfo residualRequestInfo = requests.poll();
    int discardCount = 0;
    while (residualRequestInfo != null) {
      if (residualRequestInfo.restRequest != null) {
        discardCount++;
        onRequestDequeue(residualRequestInfo);
        onProcessingFailure(residualRequestInfo.restRequest, residualRequestInfo.restResponseChannel, e);
      }
      residualRequestInfo = requests.poll();
    }
    if (discardCount > 0) {
      restServerMetrics.residualRequestQueueSize.inc(discardCount);
      logger.info("There were {} requests in flight during shutdown", discardCount);
    }
  }

  /**
   * Triggers an error response.
   * @param restRequest the {@link RestRequest} for which the response has been completed.
   * @param restResponseChannel the {@link RestResponseChannel} over which response was sent.
   * @param exception any {@link Exception} that occurred during response construction.
   */
  private void onProcessingFailure(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Exception exception) {
    try {
      if (exception instanceof RestServiceException) {
        RestServiceErrorCode errorCode = ((RestServiceException) exception).getErrorCode();
        ResponseStatus responseStatus = ResponseStatus.getResponseStatus(errorCode);
        if (responseStatus == ResponseStatus.InternalServerError) {
          logger.error("Internal error handling request {} with method {}.", restRequest.getUri(),
              restRequest.getRestMethod(), exception);
        } else if (responseStatus == ResponseStatus.BadRequest) {
          logger.debug("Request {} with method {} is a bad request.", restRequest.getUri(), restRequest.getRestMethod(),
              exception);
        } else {
          logger.trace("Error handling request {} with method {}.", restRequest.getUri(), restRequest.getRestMethod(),
              exception);
        }
      } else {
        logger.error("Unexpected error handling request {} with method {}.", restRequest.getUri(),
            restRequest.getRestMethod(), exception);
      }
      restRequest.getMetricsTracker().scalingMetricsTracker.markRequestCompleted();
      restResponseChannel.onResponseComplete(exception);
    } catch (Exception e) {
      restServerMetrics.responseCompleteTasksError.inc();
      logger.error("Error during response complete tasks", e);
    }
  }

  /**
   * Tracks required metrics once a {@link AsyncRequestInfo} is dequeued.
   * @param requestInfo the {@link AsyncRequestInfo} that was just dequeued.
   */
  private void onRequestDequeue(AsyncRequestInfo requestInfo) {
    queuedRequestCount.decrementAndGet();
    restServerMetrics.requestDequeuingRate.mark();
    long processingDelay = requestInfo.getProcessingDelay();
    requestInfo.restRequest.getMetricsTracker().scalingMetricsTracker.addToRequestProcessingWaitTime(processingDelay);
  }

  /**
   * Represents a queued request.
   */
  protected static class AsyncRequestInfo {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private long queueStartTime = System.currentTimeMillis();

    /**
     * A queued request represented by a {@link RestRequest} that encapsulates the request and a
     * {@link RestResponseChannel} that provides a way to return a response for the request.
     * @param restRequest the {@link RestRequest} that encapsulates the request.
     * @param restResponseChannel the {@link RestResponseChannel} to use to send the response to the client.
     */
    public AsyncRequestInfo(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
    }

    /**
     * Gets the time elapsed since the construction of this object.
     * @return the time elapsed since the construction of the object.
     */
    public long getProcessingDelay() {
      return System.currentTimeMillis() - queueStartTime;
    }
  }
}

/**
 * Handles sending responses, handling callbacks and doing cleanup.
 */
class AsyncResponseHandler implements Closeable {
  private final RestServerMetrics restServerMetrics;
  private final ConcurrentHashMap<RestRequest, ResponseWriteCallback> responses =
      new ConcurrentHashMap<RestRequest, ResponseWriteCallback>();
  private final AtomicInteger inFlightResponsesCount = new AtomicInteger(0);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a AsyncResponseHandler that can handle responses.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use to track metrics.
   */
  protected AsyncResponseHandler(RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    logger.trace("Instantiated AsyncResponseHandler");
  }

  @Override
  public void close() {
    long closeStartTime = System.currentTimeMillis();
    discardResponses();
    logger.trace("Closed AsyncResponseHandler");
    restServerMetrics.responseHandlerCloseTimeInMs.update(System.currentTimeMillis() - closeStartTime);
  }

  /**
   * Handles response sending asynchronously. When this function returns, it may not be sent yet.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the response.
   */
  protected void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    long processingStartTime = System.currentTimeMillis();
    if (restRequest == null || restResponseChannel == null) {
      throw new IllegalArgumentException("Received one or more null arguments");
    }
    try {
      restServerMetrics.responseArrivalRate.mark();
      if (exception != null || response == null) {
        onResponseComplete(restRequest, restResponseChannel, response, exception);
      } else {
        ResponseWriteCallback responseWriteCallback =
            new ResponseWriteCallback(restRequest, response, restResponseChannel);
        if (responses.putIfAbsent(restRequest, responseWriteCallback) != null) {
          restServerMetrics.responseAlreadyInFlightError.inc();
          throw new RestServiceException("Request for which response is being scheduled has a response outstanding",
              RestServiceErrorCode.RequestResponseQueuingFailure);
        } else {
          response.readInto(restResponseChannel, responseWriteCallback);
          inFlightResponsesCount.incrementAndGet();
          logger.trace("Response of size {} for request {} is scheduled to be sent", response.getSize(),
              restRequest.getUri());
        }
      }
    } finally {
      long preProcessingTime = System.currentTimeMillis() - processingStartTime;
      restServerMetrics.responsePreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingTime(preProcessingTime);
    }
  }

  /**
   * Gets number of responses being (or waiting to be) sent.
   * @return size of response map/set.
   */
  protected int getResponseSetSize() {
    return inFlightResponsesCount.get();
  }

  /**
   * Called on shutdown and fails the remaining responses and releases resources held by them.
   */
  private void discardResponses() {
    logger.trace("Discarding responses on account of shutdown");
    RestServiceException e = new RestServiceException("Service shutdown", RestServiceErrorCode.ServiceUnavailable);
    if (responses.size() > 0) {
      int noOfResponses = responses.size();
      logger.info("There were {} responses in flight during was shut down", noOfResponses);
      restServerMetrics.residualResponseSetSize.inc(noOfResponses);
      List<ResponseWriteCallback> callbacks = new LinkedList<ResponseWriteCallback>();
      // Since the callbacks remove the hash map entry, we need to call them when we are *not* iterating over the map.
      // Unfortunately this creates two traversals. But this should be ok as this happens during shutdown and does
      // not affect live performance.
      for (Map.Entry<RestRequest, ResponseWriteCallback> response : responses.entrySet()) {
        callbacks.add(response.getValue());
      }
      for (ResponseWriteCallback callback : callbacks) {
        callback.onCompletion(0L, e);
      }
    }
  }

  /**
   * Completes the response.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   */
  private void onResponseComplete(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) {
    try {
      if (exception != null) {
        restServerMetrics.responseExceptionCount.inc();
        if (exception instanceof RestServiceException) {
          RestServiceErrorCode errorCode = ((RestServiceException) exception).getErrorCode();
          ResponseStatus responseStatus = ResponseStatus.getResponseStatus(errorCode);
          if (responseStatus == ResponseStatus.InternalServerError) {
            logger.error("Internal error handling request {} with method {}.", restRequest.getUri(),
                restRequest.getRestMethod(), exception);
          } else if (responseStatus == ResponseStatus.BadRequest) {
            logger
                .debug("Request {} with method {} is a bad request.", restRequest.getUri(), restRequest.getRestMethod(),
                    exception);
          } else {
            logger.trace("Error handling request {} with method {}.", restRequest.getUri(), restRequest.getRestMethod(),
                exception);
          }
        } else {
          logger.error("Unexpected error handling request {} with method {}.", restRequest.getUri(),
              restRequest.getRestMethod(), exception);
        }
      }
      restRequest.getMetricsTracker().scalingMetricsTracker.markRequestCompleted();
      restResponseChannel.onResponseComplete(exception);
      restServerMetrics.responseCompletionRate.mark();
    } catch (Exception e) {
      restServerMetrics.responseCompleteTasksError.inc();
      logger.error("Error during response complete tasks", e);
    } finally {
      logger.trace("Response complete for request {}", restRequest.getUri());
      releaseResources(restRequest, response);
    }
  }

  /**
   * Cleans up resources.
   */
  private void releaseResources(RestRequest restRequest, ReadableStreamChannel response) {
    if (response != null) {
      try {
        response.close();
      } catch (IOException e) {
        restServerMetrics.resourceReleaseError.inc();
        logger.error("Error closing response", e);
      }
    }
    responses.remove(restRequest);
  }

  /**
   * Callback for response writes.
   */
  class ResponseWriteCallback implements Callback<Long> {
    private final RestRequest restRequest;
    private final ReadableStreamChannel response;
    private final RestResponseChannel restResponseChannel;
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    private long operationStartTime = System.currentTimeMillis();

    /**
     * A queued response represented by a {@link ReadableStreamChannel} that encapsulates the response and a
     * {@link RestResponseChannel} that provides a way to return the response to the client.
     * @param restRequest the {@link RestRequest} that encapsulates the request.
     * @param response the {@link ReadableStreamChannel} that encapsulates the response.
     * @param restResponseChannel the {@link RestResponseChannel} to use to send the response to the client.
     */
    public ResponseWriteCallback(RestRequest restRequest, ReadableStreamChannel response,
        RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.response = response;
      this.restResponseChannel = restResponseChannel;
    }

    @Override
    public void onCompletion(Long result, Exception exception) {
      long callbackReceiveTime = System.currentTimeMillis();
      if (callbackInvoked.compareAndSet(false, true)) {
        try {
          long callbackWaitTime = callbackReceiveTime - operationStartTime;
          restServerMetrics.responseCallbackWaitTimeInMs.update(callbackWaitTime);
          restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingWaitTime(callbackWaitTime);
          inFlightResponsesCount.decrementAndGet();
          if (exception == null && (result == null || result != response.getSize())) {
            exception = new IllegalStateException("Response write incomplete");
          }
          onResponseComplete(restRequest, restResponseChannel, response, exception);
        } finally {
          long callbackProcessingTime = System.currentTimeMillis() - callbackReceiveTime;
          restServerMetrics.responseCallbackProcessingTimeInMs.update(callbackProcessingTime);
          restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingTime(callbackProcessingTime);
        }
      }
    }
  }
}
