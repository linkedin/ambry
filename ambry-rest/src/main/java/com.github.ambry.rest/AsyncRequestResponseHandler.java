package com.github.ambry.rest;

import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.Iterator;
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
 * Multiple instances are created by the {@link RequestResponseHandlerController} and each instance runs continuously to
 * handle submitted requests and responses.
 * <p/>
 * Requests are queued on submission and handed off to the {@link BlobStorageService} when they are dequeued. Responses
 * are entered into a set/map that is continuously iterated upon and response bytes that are ready are sent to the
 * client via the appropriate {@link RestResponseChannel}.
 * <p/>
 * A single thread handles both requests and responses. Available responses are sent before handling queued requests.
 * <p/>
 * These are the scaling units of the {@link RestServer} and can be scaled up and down independently of any other
 * component of the {@link RestServer}.
 * <p/>
 * Thread safe.
 */
public class AsyncRequestResponseHandler {
  private final Thread workerThread;
  private final AsyncHandlerWorker asyncHandlerWorker;
  private final RestServerMetrics restServerMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Builds a AsyncRequestResponseHandler by creating a worker thread for handling queued requests and responses.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use to track metrics.
   */
  protected AsyncRequestResponseHandler(RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    asyncHandlerWorker = new AsyncHandlerWorker(restServerMetrics);
    workerThread = new Thread(asyncHandlerWorker);
    restServerMetrics.registerAsyncRequestResponseHandler(this);
    logger.trace("Instantiated AsyncRequestResponseHandler");
  }

  /**
   * Queues the {@code restRequest} to be handled async. When this function returns, it may not be handled yet. When
   * the response is ready, {@link RestResponseChannel} will be used to send the response.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is a problem queuing the request.
   */
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (isRunning()) {
      asyncHandlerWorker.submitRequest(restRequest, restResponseChannel);
    } else {
      restServerMetrics.requestResponseHandlerUnavailableError.inc();
      throw new RestServiceException(
          "Requests cannot be handled because the AsyncRequestResponseHandler is not available",
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Submit a response for a request along with a channel over which the response can be sent. If the response building
   * was unsuccessful for any reason, the details are included in the {@code exception}.
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
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (isRunning()) {
      asyncHandlerWorker.submitResponse(restRequest, restResponseChannel, response, exception);
    } else {
      restServerMetrics.requestResponseHandlerUnavailableError.inc();
      throw new RestServiceException(
          "Requests cannot be handled because the AsyncRequestResponseHandler is not available",
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Does startup tasks for the AsyncRequestResponseHandler. When the function returns, startup is FULLY complete.
   * @throws IllegalStateException if a {@link BlobStorageService} has not been set before starting.
   * @throws InstantiationException if the AsyncRequestResponseHandler is unable to start.
   */
  protected void start()
      throws InstantiationException {
    if (!isRunning()) {
      if (asyncHandlerWorker.isReadyToStart()) {
        long startupBeginTime = System.currentTimeMillis();
        logger.info("Starting AsyncRequestResponseHandler");
        workerThread.start();
        long startupTime = System.currentTimeMillis() - startupBeginTime;
        restServerMetrics.requestResponseHandlerStartTimeInMs.update(startupTime);
        logger.info("AsyncRequestResponseHandler start took {} ms", startupTime);
      } else {
        throw new IllegalStateException("BlobStorageService has not been set");
      }
    }
  }

  /**
   * Does shutdown tasks for the AsyncRequestResponseHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any requests/responses queued might be dropped during shutdown.
   * <p/>
   * The {@link NioServer} is expected to have stopped queuing new requests before this function is called.
   */
  protected void shutdown() {
    if (isRunning()) {
      logger.info("Shutting down AsyncRequestResponseHandler");
      long shutdownBeginTime = System.currentTimeMillis();
      try {
        asyncHandlerWorker.shutdown();
        if (!asyncHandlerWorker.awaitShutdown(30, TimeUnit.SECONDS)) {
          logger.error("Shutdown of AsyncRequestResponseHandler failed. This should not happen");
          restServerMetrics.requestResponseHandlerShutdownError.inc();
        }
      } catch (InterruptedException e) {
        logger.error("Await shutdown of AsyncRequestResponseHandler was interrupted. It might not have shutdown", e);
        restServerMetrics.requestResponseHandlerShutdownError.inc();
      } finally {
        long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
        logger.info("AsyncRequestResponseHandler shutdown took {} ms", shutdownTime);
        restServerMetrics.requestResponseHandlerShutdownTimeInMs.update(shutdownTime);
      }
    }
  }

  /**
   * Sets the {@link BlobStorageService} that will be used in {@link AsyncHandlerWorker}.
   * @param blobStorageService the {@link BlobStorageService} instance to be used to process requests.
   */
  protected void setBlobStorageService(BlobStorageService blobStorageService) {
    asyncHandlerWorker.setBlobStorageService(blobStorageService);
  }

  /**
   * Used to query whether the AsyncRequestResponseHandler is currently in a state to handle submitted
   * requests/responses.
   * @return {@code true} if in a state to handle submitted requests. {@code false} otherwise.
   */
  protected boolean isRunning() {
    return asyncHandlerWorker.isRunning() && workerThread.isAlive();
  }

  /**
   * Gets number of requests waiting to be processed.
   * @return size of request queue.
   */
  protected int getRequestQueueSize() {
    return asyncHandlerWorker.getRequestQueueSize();
  }

  /**
   * Gets number of responses being (or waiting to be) sent.
   * @return size of response map/set.
   */
  protected int getResponseSetSize() {
    return asyncHandlerWorker.getResponseSetSize();
  }
}

/**
 * Thread that handles the queuing and processing of requests and responses.
 */
class AsyncHandlerWorker implements Runnable {
  private final static int SINGLE_ITERATION_REQUEST_PROCESS_LIMIT = 10;
  private final static long POLL_TIMEOUT_MS = 1;

  private final RestServerMetrics restServerMetrics;

  private final LinkedBlockingQueue<AsyncRequestInfo> requests = new LinkedBlockingQueue<AsyncRequestInfo>();
  private final ConcurrentHashMap<RestRequest, AsyncResponseInfo> responses =
      new ConcurrentHashMap<RestRequest, AsyncResponseInfo>();
  private final AtomicInteger queuedRequestCount = new AtomicInteger(0);
  private final AtomicInteger queuedResponseCount = new AtomicInteger(0);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private BlobStorageService blobStorageService = null;

  /**
   * Creates a worker that can process requests and responses.
   * @param restServerMetrics the {@link RestServerMetrics} instance to use to track metrics.
   */
  protected AsyncHandlerWorker(RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    logger.trace("Instantiated AsyncHandlerWorker");
  }

  /**
   * Handles queued requests and responses continuously until shutdown.
   */
  @Override
  public void run() {
    logger.trace("AsyncHandlerWorker started");
    while (isRunning()) {
      try {
        processRequests();
        processResponses();
      } catch (Exception e) {
        logger.error("Swallowing unexpected exception during processing of responses and requests", e);
        restServerMetrics.unexpectedError.inc();
      }
    }
    discardRequestsResponses();
    logger.trace("AsyncHandlerWorker stopped");
    shutdownLatch.countDown();
  }

  /**
   * Sets the {@link BlobStorageService} that will be used.
   * @param blobStorageService the {@link BlobStorageService} instance to be used to process requests.
   * @throws IllegalArgumentException if {@code blobStorageService} is null.
   */
  protected void setBlobStorageService(BlobStorageService blobStorageService) {
    if (blobStorageService == null) {
      throw new IllegalArgumentException("BlobStorageService cannot be null");
    }
    this.blobStorageService = blobStorageService;
    logger.trace("BlobStorage service set to {}", blobStorageService.getClass());
  }

  /**
   * Used to query whether the AsyncHandlerWorker is ready to start.
   * @return {@code true} if ready to start, otherwise {@code false}.
   */
  protected boolean isReadyToStart() {
    return blobStorageService != null;
  }

  /**
   * Marks that shutdown is required. When this function returns, shutdown *need not* be complete. Instead, shutdown
   * is scheduled to happen after the current processing cycle finishes.
   * <p/>
   * All requests and responses still in the queue will be discarded.
   */
  protected void shutdown() {
    logger.trace("AsyncHandlerWorker slated for shutdown");
    running.set(false);
  }

  /**
   * Wait for the shutdown of this instance for the specified time.
   * @param timeout the amount of time to wait for shutdown.
   * @param timeUnit time unit of {@code timeout}.
   * @return {@code true} if shutdown succeeded within the {@code timeout}. {@code false} otherwise.
   * @throws InterruptedException if the wait for shutdown is interrupted.
   */
  protected boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return shutdownLatch.await(timeout, timeUnit);
  }

  /**
   * Queues the {@code restRequest} to be handled async. When this function returns, it may not be handled yet.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if any of the arguments are null or if there is a problem queuing the request.
   */
  protected void submitRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    long processingStartTime = System.currentTimeMillis();
    handlePrechecks(restRequest, restResponseChannel);
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
   * Queues the response to be handled async. When this function returns, it may not be sent yet.
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
    handlePrechecks(restRequest, restResponseChannel);
    restServerMetrics.responseArrivalRate.mark();
    try {
      if (exception != null || response == null) {
        restServerMetrics.responseCompletionRate.mark();
        logger.trace("There was no queuing required for response for request {}", restRequest.getUri());
        onResponseComplete(restRequest, restResponseChannel, exception, false);
        restServerMetrics.responseCompletionRate.mark();
        if (response != null) {
          releaseResources(response);
        }
      } else {
        if (responses.putIfAbsent(restRequest, new AsyncResponseInfo(response, restResponseChannel)) != null) {
          restServerMetrics.responseAlreadyInFlightError.inc();
          throw new RestServiceException("Request for which response is being scheduled has a response outstanding",
              RestServiceErrorCode.RequestResponseQueuingFailure);
        } else {
          queuedResponseCount.incrementAndGet();
          restServerMetrics.responseQueuingRate.mark();
          logger.trace("Queuing response of size {} for request {}", response.getSize(), restRequest.getUri());
        }
      }
    } finally {
      long preProcessingTime = System.currentTimeMillis() - processingStartTime;
      restServerMetrics.responsePreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingTime(preProcessingTime);
    }
  }

  /**
   * Information on whether this instance is accepting requests and responses. This will return {@code false} as soon as
   * {@link #shutdown()} is called whether or not the instance has actually stopped working.
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
   * Gets number of responses being (or waiting to be) sent.
   * @return size of response map/set.
   */
  protected int getResponseSetSize() {
    return queuedResponseCount.get();
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
   * Dequeues requests from the request queue, processes them  and calls into the appropriate APIs of
   * {@link BlobStorageService}.
   */
  private void processRequests() {
    AsyncRequestInfo requestInfo = null;
    int processedThisTime = 0;
    while (processedThisTime < SINGLE_ITERATION_REQUEST_PROCESS_LIMIT) {
      try {
        requestInfo = requests.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (requestInfo != null) {
          processRequest(requestInfo);
          logger.trace("Request {} was processed successfully", requestInfo.getRestRequest().getUri());
        } else {
          break;
        }
      } catch (Exception e) {
        restServerMetrics.requestProcessingError.inc();
        if (requestInfo != null) {
          onResponseComplete(requestInfo.getRestRequest(), requestInfo.getRestResponseChannel(), e, false);
        } else {
          logger.error("Unexpected exception while processing requests", e);
        }
      } finally {
        processedThisTime++;
      }
    }
  }

  /**
   * Iterates through the response map and sends out any response bytes that are ready over the appropriate
   * {@link RestResponseChannel}.
   * <p/>
   * If there is no more data left to write for a certain response or if an exception is thrown, removes the response
   * information from the map and commences tasks that need to be done when response is complete.
   */
  private void processResponses() {
    Iterator<Map.Entry<RestRequest, AsyncResponseInfo>> responseIterator = responses.entrySet().iterator();
    while (responseIterator.hasNext()) {
      Map.Entry<RestRequest, AsyncResponseInfo> responseInfo = responseIterator.next();
      long processingStartTime = System.currentTimeMillis();
      // needed to avoid double counting.
      long responseProcessingTime = 0;
      RestRequest restRequest = responseInfo.getKey();
      try {
        AsyncResponseInfo asyncResponseInfo = responseInfo.getValue();
        ReadableStreamChannel response = asyncResponseInfo.getResponse();
        RestResponseChannel restResponseChannel = asyncResponseInfo.getRestResponseChannel();
        int bytesWritten = 0;
        Exception exception = null;
        try {
          long processingDelay = asyncResponseInfo.getProcessingDelay();
          restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingDelay(processingDelay);
          long responseWriteStartTime = System.currentTimeMillis();
          bytesWritten = response.read(restResponseChannel);
          responseProcessingTime = System.currentTimeMillis() - responseWriteStartTime;
          logger.trace("{} response bytes were written for request {}", bytesWritten, restRequest.getUri());
        } catch (Exception e) {
          restServerMetrics.responseProcessingError.inc();
          exception = e;
        }

        if (bytesWritten == -1 || exception != null) {
          long responseCompleteStartTime = System.currentTimeMillis();
          onResponseComplete(restRequest, restResponseChannel, exception, false);
          responseProcessingTime += (System.currentTimeMillis() - responseCompleteStartTime);
          releaseResources(response);
          responseIterator.remove();
          queuedResponseCount.decrementAndGet();
          restServerMetrics.responseCompletionRate.mark();
          logger.trace("Response complete for request {}", restRequest.getUri());
        } else {
          asyncResponseInfo.recordProcessingEndTime();
        }
      } finally {
        restRequest.getMetricsTracker().scalingMetricsTracker
            .addToResponseProcessingTime(System.currentTimeMillis() - processingStartTime - responseProcessingTime);
      }
    }
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
    RestRequest restRequest = asyncRequestInfo.getRestRequest();
    try {
      onRequestDequeue(asyncRequestInfo);
      RestResponseChannel restResponseChannel = asyncRequestInfo.getRestResponseChannel();
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
          onResponseComplete(restRequest, restResponseChannel, e, false);
      }
      blobStorageProcessingTime = System.currentTimeMillis() - blobStorageProcessingStartTime;
    } finally {
      restRequest.getMetricsTracker().scalingMetricsTracker
          .addToRequestProcessingTime(System.currentTimeMillis() - processingStartTime - blobStorageProcessingTime);
    }
  }

  /**
   * Called on shutdown and empties the remaining requests and responses and releases resources held by them.
   */
  private void discardRequestsResponses() {
    logger.trace("Discarding requests/responses on account of shutdown");
    RestServiceException e = new RestServiceException("Service shutdown", RestServiceErrorCode.ServiceUnavailable);
    if (requests.size() > 0) {
      logger.info("There were {} requests in flight during shutdown", requests.size());
      restServerMetrics.residualRequestQueueSize.inc(requests.size());
      AsyncRequestInfo residualRequestInfo = requests.poll();
      while (residualRequestInfo != null) {
        onRequestDequeue(residualRequestInfo);
        onResponseComplete(residualRequestInfo.getRestRequest(), residualRequestInfo.getRestResponseChannel(), e, true);
        residualRequestInfo = requests.poll();
      }
    }
    if (responses.size() > 0) {
      logger.info("There were {} responses in flight during was shut down", responses.size());
      restServerMetrics.residualResponseSetSize.inc(responses.size());
      Iterator<Map.Entry<RestRequest, AsyncResponseInfo>> responseIterator = responses.entrySet().iterator();
      while (responseIterator.hasNext()) {
        Map.Entry<RestRequest, AsyncResponseInfo> responseInfo = responseIterator.next();
        RestRequest restRequest = responseInfo.getKey();
        AsyncResponseInfo asyncResponseInfo = responseInfo.getValue();
        long processingDelay = asyncResponseInfo.getProcessingDelay();
        restRequest.getMetricsTracker().scalingMetricsTracker.addToResponseProcessingDelay(processingDelay);
        ReadableStreamChannel response = asyncResponseInfo.getResponse();
        RestResponseChannel restResponseChannel = asyncResponseInfo.getRestResponseChannel();
        onResponseComplete(restRequest, restResponseChannel, e, true);
        queuedResponseCount.decrementAndGet();
        restServerMetrics.responseCompletionRate.mark();
        releaseResources(response);
        responseIterator.remove();
      }
    }
  }

  /**
   * Cleans up resources.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that needs to be cleaned up.
   */
  private void releaseResources(ReadableStreamChannel readableStreamChannel) {
    try {
      readableStreamChannel.close();
    } catch (IOException e) {
      restServerMetrics.resourceReleaseError.inc();
      logger.error("Error closing response", e);
    }
  }

  /**
   * Completes the response.
   * @param restRequest the {@link RestRequest} for which the response has been completed.
   * @param restResponseChannel the {@link RestResponseChannel} over which response was sent.
   * @param exception any {@link Exception} that occurred during response construction. Can be null
   * @param forceClose whether to forcibly close the channel to the client.
   */
  private void onResponseComplete(RestRequest restRequest, RestResponseChannel restResponseChannel, Exception exception,
      boolean forceClose) {
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
      if (forceClose) {
        restResponseChannel.close();
      }
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
    requestInfo.getRestRequest().getMetricsTracker().scalingMetricsTracker.addToProcessingDelay(processingDelay);
  }
}

/**
 * Represents a queued request.
 */
class AsyncRequestInfo {
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

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }

  /**
   * Gets the time elapsed since the construction of this object.
   * @return the time elapsed since the construction of the object.
   */
  public long getProcessingDelay() {
    return System.currentTimeMillis() - queueStartTime;
  }
}

/**
 * Represents a queued response.
 */
class AsyncResponseInfo {
  private final ReadableStreamChannel response;
  private final RestResponseChannel restResponseChannel;
  private long delayStartTime = System.currentTimeMillis();

  /**
   * A queued response represented by a {@link ReadableStreamChannel} that encapsulates the response and a
   * {@link RestResponseChannel} that provides a way to return the response to the client.
   * @param response the {@link ReadableStreamChannel} that encapsulates the response.
   * @param restResponseChannel the {@link RestResponseChannel} to use to send the response to the client.
   */
  public AsyncResponseInfo(ReadableStreamChannel response, RestResponseChannel restResponseChannel) {
    this.response = response;
    this.restResponseChannel = restResponseChannel;
  }

  public ReadableStreamChannel getResponse() {
    return response;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }

  /**
   * Gets the time elapsed since the last time this object was last processed. The last time this object was processed
   * is either on construction or on a call to {@link #recordProcessingEndTime()}.
   * @return the time elapsed since the last time the object was processed.
   */
  public long getProcessingDelay() {
    return System.currentTimeMillis() - delayStartTime;
  }

  /**
   * Records the time at which the current round of processing ended.
   */
  public void recordProcessingEndTime() {
    delayStartTime = System.currentTimeMillis();
  }
}
