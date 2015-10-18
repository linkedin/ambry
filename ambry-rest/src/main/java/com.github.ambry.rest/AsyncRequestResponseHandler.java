package com.github.ambry.rest;

import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Asynchronously handles requests and responses that are submitted.
 * <p/>
 * Requests are submitted by a {@link NioServer} and asynchronously routed to a {@link BlobStorageService}. Responses
 * are usually submitted through {@link Callback}s from beyond the {@link BlobStorageService} layer and asynchronously
 * sent to the client. In both pathways, this class enables the non-blocking paradigm.
 * <p/>
 * Multiple instances are created by the {@link RequestResponseHandlerController} and each instance runs continuously to
 * handle submitted request.
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
  public AsyncRequestResponseHandler(RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    asyncHandlerWorker = new AsyncHandlerWorker(restServerMetrics);
    workerThread = new Thread(asyncHandlerWorker);
    restServerMetrics.registerAsyncRequestResponseHandler(this);
    logger.trace("Instantiated AsyncRequestResponseHandler");
  }

  /**
   * Does startup tasks for the AsyncRequestResponseHandler. When the function returns, startup is FULLY complete.
   * @throws IllegalStateException if a {@link BlobStorageService} has not been set before starting.
   * @throws InstantiationException if the AsyncRequestResponseHandler is unable to start.
   */
  public void start()
      throws InstantiationException {
    if (!isRunning()) {
      if (asyncHandlerWorker.isReadyToStart()) {
        logger.info("Starting AsyncRequestResponseHandler");
        workerThread.start();
        logger.info("AsyncRequestResponseHandler has started");
      } else {
        throw new IllegalStateException("BlobStorageService has not been set");
      }
    }
  }

  /**
   * Does shutdown tasks for the AsyncRequestResponseHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any requests/responses queued after shutdown is called might be dropped.
   * <p/>
   * The {@link NioServer} is expected to have stopped queueing new requests before this function is called.
   */
  public void shutdown() {
    if (isRunning()) {
      logger.info("Shutting down AsyncRequestResponseHandler with {} requests and {} responses still in queue",
          getRequestQueueSize(), getResponseSetSize());
      long shutdownBeginTime = System.currentTimeMillis();
      try {
        asyncHandlerWorker.shutdown();
        if (!asyncHandlerWorker.awaitShutdown(30, TimeUnit.SECONDS)) {
          logger.error("Shutdown of AsyncRequestResponseHandler failed. This should not happen");
          restServerMetrics.asyncRequestHandlerShutdownError.inc();
        }
      } catch (InterruptedException e) {
        logger.error("Await shutdown of AsyncRequestResponseHandler was interrupted. It might not have shutdown", e);
        restServerMetrics.asyncRequestHandlerShutdownError.inc();
      } finally {
        long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
        logger.info("AsyncRequestResponseHandler shutdown took {} ms", shutdownTime);
        restServerMetrics.asyncRequestHandlerShutdownTimeInMs.update(shutdownTime);
      }
    }
  }

  /**
   * Queues the {@code restRequest} to be handled async. When this function returns, it may not be handled yet. When
   * the response is ready, {@link RestResponseChannel} will be used ot send the response.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is a problem queueing the request.
   */
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (isRunning()) {
      asyncHandlerWorker.handleRequest(restRequest, restResponseChannel);
    } else {
      restServerMetrics.asyncRequestHandlerUnavailableError.inc();
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
   * @throws RestServiceException if there is any error while queueing the response.
   */
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (isRunning()) {
      asyncHandlerWorker.handleResponse(restRequest, restResponseChannel, response, exception);
    } else {
      restServerMetrics.asyncRequestHandlerUnavailableError.inc();
      throw new RestServiceException(
          "Requests cannot be handled because the AsyncRequestResponseHandler is not available",
          RestServiceErrorCode.ServiceUnavailable);
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
 * Thread that handles the queueing and processing of requests and responses.
 */
class AsyncHandlerWorker implements Runnable {
  private final static long OFFER_TIMEOUT_MS = 1;
  private final static long POLL_TIMEOUT_MS = 1;

  private final RestServerMetrics restServerMetrics;

  private final LinkedBlockingQueue<AsyncRequestInfo> requests = new LinkedBlockingQueue<AsyncRequestInfo>();
  private final ConcurrentHashMap<RestRequest, AsyncResponseInfo> responses =
      new ConcurrentHashMap<RestRequest, AsyncResponseInfo>();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final QueuingTimeTracker queuingTimeTracker = new QueuingTimeTracker();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private BlobStorageService blobStorageService = null;

  /**
   * Sets the {@link BlobStorageService} that will be used.
   * @param blobStorageService the {@link BlobStorageService} instance to be used to process requests.
   */
  protected void setBlobStorageService(BlobStorageService blobStorageService) {
    this.blobStorageService = blobStorageService;
  }

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
    while (isRunning()) {
      try {
        processResponses();
        processRequests();
      } catch (Exception e) {
        logger.error("Swallowing unexpected exception during processing of responses and requests", e);
        restServerMetrics.dequeuedRequestHandlerUnexpectedExceptionError.inc();
      }
    }
    discardRequestsResponses();
    logger.trace("AsyncHandlerWorker stopped");
    shutdownLatch.countDown();
  }

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
   * @throws RestServiceException if any of the arguments are null or if there is a problem queueing the request.
   */
  protected void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (restRequest == null) {
      restServerMetrics.asyncRequestHandlerRestRequestNullError.inc();
      throw new IllegalArgumentException("RestRequest is null");
    } else if (restResponseChannel == null) {
      restServerMetrics.asyncRequestHandlerRestResponseChannelNullError.inc();
      throw new IllegalArgumentException("RestResponseChannel is null");
    }
    restServerMetrics.asyncRequestHandlerRequestArrivalRate.mark();
    logger.trace("Queueing request {}", restRequest.getUri());
    queuingTimeTracker.startTracking(restRequest);
    boolean offerFailed = false;
    try {
      if (!requests
          .offer(new AsyncRequestInfo(restRequest, restResponseChannel), OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        offerFailed = true;
        restServerMetrics.asyncRequestHandlerQueueOfferTooLongError.inc();
        throw new RestServiceException("Attempt to queue request timed out",
            RestServiceErrorCode.RequestResponseQueueingFailure);
      } else {
        logger.trace("Queued request {}", restRequest.getUri());
        restServerMetrics.asyncRequestHandlerQueueingRate.mark();
      }
    } catch (InterruptedException e) {
      offerFailed = true;
      restServerMetrics.asyncRequestHandlerQueueOfferInterruptedError.inc();
      throw new RestServiceException("Attempt to queue request interrupted", e,
          RestServiceErrorCode.RequestResponseQueueingFailure);
    } finally {
      if (offerFailed) {
        queuingTimeTracker.stopTracking(restRequest);
      }
    }
  }

  /**
   * Queues the response to be handled async. When this function returns, it may not be sent yet.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the
   *                                {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the response.
   */
  protected void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (restRequest == null) {
      restServerMetrics.asyncRequestHandlerRestRequestNullError.inc();
      throw new IllegalArgumentException("RestRequest is null");
    } else if (restResponseChannel == null) {
      restServerMetrics.asyncRequestHandlerRestResponseChannelNullError.inc();
      throw new IllegalArgumentException("RestResponseChannel is null");
    } else if (exception != null || response == null) {
      onResponseComplete(restResponseChannel, exception, false);
      releaseResources(restRequest, response);
    } else {
      if (responses.putIfAbsent(restRequest, new AsyncResponseInfo(response, restResponseChannel)) != null) {
        // log and metrics
        throw new RestServiceException("Request for which response is being scheduled has a response outstanding",
            RestServiceErrorCode.RequestResponseQueueingFailure);
      }
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
    return requests.size();
  }

  /**
   * Gets number of responses being (or waiting to be) sent.
   * @return size of response map/set.
   */
  protected int getResponseSetSize() {
    return responses.size();
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
      ReadableStreamChannel response = responseInfo.getValue().getResponse();
      RestResponseChannel restResponseChannel = responseInfo.getValue().getRestResponseChannel();
      int bytesWritten = 0;
      Exception exception = null;
      try {
        bytesWritten = response.read(restResponseChannel);
      } catch (Exception e) {
        exception = e;
      }

      if (bytesWritten == -1 || exception != null) {
        onResponseComplete(restResponseChannel, exception, false);
        releaseResources(responseInfo.getKey(), response);
        responseIterator.remove();
      }
    }
  }

  /**
   * Dequeues requests from the request queue, processes them  and calls into the appropriate APIs of
   * {@link BlobStorageService}.
   */
  private void processRequests() {
    AsyncRequestInfo requestInfo = null;
    String uri = null;
    while (true) {
      try {
        requestInfo = requests.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (requestInfo != null) {
          uri = requestInfo.getRestRequest().getUri();
          trackMetricsOnDequeue(requestInfo);
          processRequest(requestInfo);
          logger.trace("Request {} was handled successfully", uri);
        } else {
          break;
        }
      } catch (Exception e) {
        logger.error("Handling of request {} failed", uri, e);
        restServerMetrics.dequeuedRequestHandlerRequestHandlingError.inc();
        if (requestInfo != null) {
          onResponseComplete(requestInfo.getRestResponseChannel(), e, false);
          releaseResources(requestInfo.getRestRequest(), null);
        }
      }
    }
  }

  /**
   * Processes the {@code asyncRequestInfo}. Discerns the type of {@link RestMethod} in the request and calls the right
   * function of the {@link BlobStorageService}.
   * @param asyncRequestInfo the currently dequeued {@link AsyncRequestInfo}.
   */
  private void processRequest(AsyncRequestInfo asyncRequestInfo) {
    RestRequest restRequest = asyncRequestInfo.getRestRequest();
    RestResponseChannel restResponseChannel = asyncRequestInfo.getRestResponseChannel();
    RestMethod restMethod = restRequest.getRestMethod();
    logger.trace("RestMethod is {} for request {}", restMethod, restRequest.getUri());
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
        logger.debug("Unknown REST method [{}] in request {}", restMethod, restRequest.getUri());
        restServerMetrics.dequeuedRequestHandlerUnknownRestMethodError.inc();
        onResponseComplete(restResponseChannel, new RestServiceException("Unsupported REST method - " + restMethod,
            RestServiceErrorCode.UnsupportedRestMethod), false);
        releaseResources(restRequest, null);
    }
  }

  /**
   * Empties the remaining requests and responses and releases resources held by them.
   */
  private void discardRequestsResponses() {
    RestServiceException e = new RestServiceException("Service shutdown", RestServiceErrorCode.ServiceUnavailable);
    if (requests.size() > 0) {
      logger.warn("There were {} requests in flight during shutdown", requests.size());
      restServerMetrics.asyncRequestHandlerResidualRequestQueueSize.inc(requests.size());
      AsyncRequestInfo residualRequestInfo = requests.poll();
      while (residualRequestInfo != null) {
        queuingTimeTracker.stopTracking(residualRequestInfo.getRestRequest());
        onResponseComplete(residualRequestInfo.getRestResponseChannel(), e, true);
        releaseResources(residualRequestInfo.getRestRequest(), null);
        residualRequestInfo = requests.poll();
      }
    }
    if (responses.size() > 0) {
      logger.info("There were {} responses in flight during was shut down", responses.size());
      Iterator<Map.Entry<RestRequest, AsyncResponseInfo>> responseIterator = responses.entrySet().iterator();
      while (responseIterator.hasNext()) {
        Map.Entry<RestRequest, AsyncResponseInfo> response = responseIterator.next();
        ReadableStreamChannel readableStreamChannel = response.getValue().getResponse();
        RestResponseChannel restResponseChannel = response.getValue().getRestResponseChannel();
        onResponseComplete(restResponseChannel, e, true);
        releaseResources(response.getKey(), readableStreamChannel);
        responseIterator.remove();
      }
    }
  }

  /**
   * Cleans up resources.
   * @param restRequest the {@link RestRequest} that needs to be cleaned up.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that needs to be cleaned up. Can be null.
   */
  private void releaseResources(RestRequest restRequest, ReadableStreamChannel readableStreamChannel) {
    try {
      restRequest.close();
    } catch (IOException e) {
      // log and metrics
    }

    if (readableStreamChannel != null) {
      try {
        readableStreamChannel.close();
      } catch (IOException e) {
        // log and metrics
      }
    }
  }

  /**
   * Completes the response.
   * @param restResponseChannel the {@link RestResponseChannel} over which response was sent.
   * @param exception any {@link Exception} that occurred during response construction. Can be null
   * @param forceClose whether to forcibly close the channel to the client.
   */
  private void onResponseComplete(RestResponseChannel restResponseChannel, Exception exception, boolean forceClose) {
    restServerMetrics.dequeuedRequestHandlerRequestCompletionRate.mark();
    restResponseChannel.onResponseComplete(exception);
  }

  /**
   * Tracks required metrics once a {@link AsyncRequestInfo} is dequeued.
   * @param requestInfo the {@link AsyncRequestInfo} that was just dequeued.
   */
  private void trackMetricsOnDequeue(AsyncRequestInfo requestInfo) {
    Long queueTime = queuingTimeTracker.stopTracking(requestInfo.getRestRequest());
    if (queueTime != null) {
      restServerMetrics.asyncRequestHandlerQueueTimeInMs.update(queueTime);
      logger.trace("Dequeued request spent {} ms in the queue", queueTime);
    } else {
      logger.warn("Queueing time of request was not tracked since queue start time was not recorded");
    }
    restServerMetrics.dequeuedRequestHandlerDequeuingRate.mark();
  }
}

/**
 * Used to track the time a particular {@link RestRequest} spends being queued.
 */
class QueuingTimeTracker {
  private final ConcurrentHashMap<RestRequest, Long> queueTimes = new ConcurrentHashMap<RestRequest, Long>();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Starts tracking the time the request spends being queued.
   * @param restRequest the {@link RestRequest} whose queueing time needs to be tracked.
   */
  public void startTracking(RestRequest restRequest) {
    Long queueStartTime = System.currentTimeMillis();
    Long prevStartTime = queueTimes.putIfAbsent(restRequest, queueStartTime);
    if (prevStartTime != null) {
      logger.warn("Duplicate call to track a request. Prev track start time - {}, current time - {}", restRequest,
          prevStartTime, queueStartTime);
    }
  }

  /**
   * Stops tracking the time elapsed since {@link #startTracking(RestRequest)} was called on this {@code restRequest}
   * and returns it. If {@link #startTracking(RestRequest)} was never called on this {@code restRequest}, returns null.
   * @param restRequest the {@link RestRequest} whose queueing time tracking needs to be stopped and recorded.
   * @return time elapsed since {@link #startTracking(RestRequest)} was called on the {@code restRequest}. If
   * {@link #startTracking(RestRequest)} was never called, returns null.
   */
  public Long stopTracking(RestRequest restRequest) {
    Long queueStartTime = queueTimes.remove(restRequest);
    return queueStartTime != null ? System.currentTimeMillis() - queueStartTime : null;
  }
}

/**
 * Represents a queued request.
 */
class AsyncRequestInfo {
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }

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
}

/**
 * Represents a queued response.
 */
class AsyncResponseInfo {
  private final ReadableStreamChannel response;
  private final RestResponseChannel restResponseChannel;

  public ReadableStreamChannel getResponse() {
    return response;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }

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
}
