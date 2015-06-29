package com.github.ambry.rest;

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
 * Implementation of {@link RestRequestHandler} that handles requests submitted by a
 * {@link NioServer} and routes them asynchronously to a {@link BlobStorageService}.
 * <p/>
 * Multiple instances are created by the {@link RequestHandlerController}  and each instance runs continuously to handle
 * requests submitted by the {@link NioServer} in a non-blocking way by queueing them.
 * The requests are then handled async through a {@link BlobStorageService}.
 * <p/>
 * The queue owned by a single AsyncRequestHandler might have parts from multiple requests interleaved but parts of the
 * same request will (have to) be in order. This ordering cannot be enforced by the AsyncRequestHandler but instead has
 * to be enforced by the {@link NioServer} (it is responsible for finding a way to use the
 * same AsyncRequestHandler for all parts of the same request - for an example implementation see
 * {@link NettyMessageProcessor}).
 * <p/>
 * These are the scaling units of the {@link RestServer} and can be scaled up and down independently of any other
 * component of the {@link RestServer}.
 */
class AsyncRequestHandler implements RestRequestHandler {
  // magic number
  private static long OFFER_TIMEOUT_SECONDS = 5;

  private final Thread dequeuedRequestHandlerThread;
  private final DequeuedRequestHandler dequeuedRequestHandler;
  private final ConcurrentHashMap<RestRequestMetadata, Boolean> requestsInFlight =
      new ConcurrentHashMap<RestRequestMetadata, Boolean>();
  private final LinkedBlockingQueue<RestRequestInfo> restRequestInfoQueue = new LinkedBlockingQueue<RestRequestInfo>();
  private final RestServerMetrics restServerMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AsyncRequestHandler(BlobStorageService blobStorageService, RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    dequeuedRequestHandler =
        new DequeuedRequestHandler(blobStorageService, restRequestInfoQueue, requestsInFlight, restServerMetrics);
    dequeuedRequestHandlerThread = new Thread(dequeuedRequestHandler);
  }

  @Override
  public void start()
      throws InstantiationException {
    if (!dequeuedRequestHandlerThread.isAlive()) {
      logger.info("Starting AsyncRequestHandler..");
      dequeuedRequestHandlerThread.start();
      logger.info("AsyncRequestHandler has started");
    }
  }

  /**
   * Attempts to shutdown the AsyncRequestHandler gracefully by introducing {@link PoisonInfo} into the queue. Any
   * resources held by the {@link RestRequestInfo}s queued after the poison are released.
   * <p/>
   * If the graceful shutdown fails, then a shutdown is forced. Any outstanding {@link RestRequestInfo}s are not
   * handled and resources held by them are not released.
   * @throws RestServiceException
   */
  @Override
  public void shutdown() {
    if (dequeuedRequestHandlerThread.isAlive()) {
      try {
        logger.info("Shutting down AsyncRequestHandler..");
        queue(new PoisonInfo());
        //magic number
        if (dequeuedRequestHandler.awaitShutdown(60, TimeUnit.SECONDS) || shutdownNow()) {
          logger.info("AsyncRequestHandler shutdown complete");
        } else {
          logger.error("Shutdown of AsyncRequestHandler failed. This should not happen");
        }
      } catch (InterruptedException e) {
        logger.error("Await shutdown of AsyncRequestHandler was interrupted. The AsyncRequestHandler might not have "
            + "shutdown", e);
      } catch (RestServiceException e) {
        logger.error("Shutdown of AsyncRequestHandler threw RestServiceException and was aborted", e);
      }
    }
  }

  /**
   * Queues the {@link RestRequestInfo} to be handled async. When this function returns, the {@link RestRequestInfo} is
   * not yet handled.
   * <p/>
   * To receive a callback on handling completion, a {@link RestRequestInfoEventListener}
   * needs to be added to the {@link RestRequestInfo}.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be handled.
   * @throws RestServiceException
   */
  @Override
  public void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    if (dequeuedRequestHandlerThread.isAlive()) {
      if (restRequestInfo == null) {
        logger.error("While trying to handle RestRequestInfo: RestRequestInfo is null");
        restServerMetrics.asyncRequestHandlerRestRequestInfoNull.inc();
        throw new RestServiceException("RestRequestInfo is null", RestServiceErrorCode.RestRequestInfoNull);
      } else if (restRequestInfo.getRestRequestMetadata() == null) {
        logger.error("While trying to handle RestRequestInfo: RestRequestMetadata is null");
        restServerMetrics.asyncRequestHandlerRestRequestMetadataNull.inc();
        throw new RestServiceException("RestRequestInfo missing RestRequestMetadata",
            RestServiceErrorCode.RequestMetadataNull);
      } else if (restRequestInfo.getRestResponseHandler() == null) {
        logger.error("While trying to handle RestRequestInfo: RestResponseHandler is null");
        restServerMetrics.asyncRequestHandlerRestResponseHandlerNull.inc();
        throw new RestServiceException("RestRequestInfo missing RestResponseHandler",
            RestServiceErrorCode.ReponseHandlerNull);
      }
      queue(restRequestInfo);
    } else {
      logger.error("While trying to handle RestRequestInfo: DequeuedRequestHandler thread is not alive");
      restServerMetrics.asyncRequestHandlerUnavailable.inc();
      throw new RestServiceException("Requests cannot be handled because the AsyncRequestHandler is not available",
          RestServiceErrorCode.RequestHandlerUnavailable);
    }
  }

  @Override
  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    dequeuedRequestHandler.onRequestComplete(restRequestMetadata);
  }

  /**
   * Adds the {@link RestRequestInfo} to the queue of {@link RestRequestInfo}s waiting to be handled.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be added to the queue.
   * @throws RestServiceException
   */
  private void queue(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    boolean releaseContent = false;
    if (restRequestInfo.isFirstPart()) {
      // This is the first part of a new request.
      // Since we are going to handle this async, we need to make sure the RestRequestMetadata is not recycled.
      // It is released in onRequestComplete() once we know that we are not going to use it anymore.
      // If the offer fails, the request will error out and onRequestComplete() will still be called. So
      // there is no need to release right away even on error.
      restRequestInfo.getRestRequestMetadata().retain();
      if (requestsInFlight.putIfAbsent(restRequestInfo.getRestRequestMetadata(), true) != null) {
        logger.error("A request seems to be marked as in-flight before the first RestRequestInfo was seen");
        restServerMetrics.asyncRequestHandlerRequestAlreadyInFlight.inc();
      }
    }
    if (restRequestInfo.getRestRequestContent() != null) {
      // Since we are going to handle this async, we need to make sure this RestRequestContent is not recycled.
      // It is released in DequeuedRequestHandler::handleRequest() once the handling is complete.
      // If the offer fails, we release immediately.
      restRequestInfo.getRestRequestContent().retain();
    }
    try {
      if (!restRequestInfoQueue.offer(restRequestInfo, OFFER_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        releaseContent = true;
        logger.error("While trying to queue a RestRequestInfo: Could not enqueue RestRequestInfo within {} seconds",
            OFFER_TIMEOUT_SECONDS);
        restServerMetrics.asyncRequestHandlerQueueOfferTooLong.inc();
        throw new RestServiceException("Attempt to queue RestRequestInfo timed out",
            RestServiceErrorCode.RestRequestInfoQueueingFailure);
      } else {
        restServerMetrics.asyncRequestHandlerQueueingRate.mark();
      }
    } catch (InterruptedException e) {
      releaseContent = true;
      logger.error("While trying to queue a RestRequestInfo: Interrupted", e);
      restServerMetrics.asyncRequestHandlerQueueOfferInterrupted.inc();
      throw new RestServiceException("Attempt to queue restRequestInfo interrupted", e,
          RestServiceErrorCode.RestRequestInfoQueueingFailure);
    } finally {
      restServerMetrics.asyncRequestHandlerQueueOccupancy.update(restRequestInfoQueue.size());
      if (releaseContent && restRequestInfo.getRestRequestContent() != null) {
        restRequestInfo.getRestRequestContent().release();
      }
    }
  }

  /**
   * Forces immediate shutdown of the {@link DequeuedRequestHandler}. The {@link RestRequestInfo} that is being
   * currently handled might misbehave and all {@link RestRequestInfo}s still in the queue do not get handled.
   */
  private boolean shutdownNow()
      throws InterruptedException {
    logger.error(
        "Forcing shutdown of AsyncRequestHandler with " + restRequestInfoQueue.size() + " RestRequestInfos still in " +
            "queue. They will be unhandled");
    dequeuedRequestHandler.shutdownNow();
    dequeuedRequestHandlerThread.interrupt();
    return dequeuedRequestHandler.awaitShutdown(5, TimeUnit.SECONDS);
  }
}

/**
 * Thread that handles the {@link RestRequestInfo}s in queue one by one. Can deal with handling failures.
 */
class DequeuedRequestHandler implements Runnable {
  private final BlobStorageService blobStorageService;
  private final LinkedBlockingQueue<RestRequestInfo> restRequestInfoQueue;
  private final ConcurrentHashMap<RestRequestMetadata, Boolean> requestsInFlight;
  private final RestServerMetrics restServerMetrics;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean shutdownReady = new AtomicBoolean(false);

  public DequeuedRequestHandler(BlobStorageService blobStorageService,
      LinkedBlockingQueue<RestRequestInfo> restRequestInfoQueue,
      ConcurrentHashMap<RestRequestMetadata, Boolean> requestsInFlight, RestServerMetrics restServerMetrics) {
    this.blobStorageService = blobStorageService;
    this.restRequestInfoQueue = restRequestInfoQueue;
    this.requestsInFlight = requestsInFlight;
    this.restServerMetrics = restServerMetrics;
  }

  /**
   * Handles any queued {@link RestRequestInfo}s continuously until poisoned or forced to shutdown.
   */
  @Override
  public void run() {
    while (!shutdownReady.get()) {
      try {
        RestRequestInfo restRequestInfo = null;
        try {
          restRequestInfo = restRequestInfoQueue.take();
          if (restRequestInfo instanceof PoisonInfo) {
            restRequestInfo.onHandlingComplete(null);
            emptyQueue();
            break;
          }
          handleRequest(restRequestInfo);
          onHandlingComplete(restRequestInfo, null);
        } catch (InterruptedException ie) {
          restServerMetrics.asyncRequestHandlerQueueTakeInterrupted.inc();
          logger.error("Wait for data in restRequestInfoQueue was interrupted - " + ie);
        } catch (Exception e) {
          restServerMetrics.dequeuedRequestHandlerRestRequestInfoHandlingFailure.inc();
          logger.error("Exception while trying to process element in restRequestInfoQueue", e);
          onHandlingComplete(restRequestInfo, e);
        }
      } catch (Exception e) {
        // net that needs to catch all Exceptions - DequeuedRequestHandler cannot go down.
      }
    }
    shutdownLatch.countDown();
  }

  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    if (restRequestMetadata != null && requestsInFlight.remove(restRequestMetadata) != null) {
      // NOTE: some of this code might be relevant to emptyQueue() also.
      restRequestMetadata.release();
    }
  }

  /**
   * Process a dequeued {@link RestRequestInfo}. Discerns the type of {@link RestMethod} and calls the right function
   * of the {@link BlobStorageService}.
   * @param restRequestInfo - The currently de-queued {@link RestRequestInfo}.
   * @throws RestServiceException
   */
  private void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    try {
      RestMethod restMethod = restRequestInfo.getRestRequestMetadata().getRestMethod();
      switch (restMethod) {
        case GET:
          blobStorageService.handleGet(restRequestInfo);
          break;
        case POST:
          blobStorageService.handlePost(restRequestInfo);
          break;
        case DELETE:
          blobStorageService.handleDelete(restRequestInfo);
          break;
        case HEAD:
          blobStorageService.handleHead(restRequestInfo);
          break;
        default:
          restServerMetrics.dequeuedRequestHandlerUnknownRestMethod.inc();
          throw new RestServiceException("Unknown rest method - " + restMethod,
              RestServiceErrorCode.UnsupportedRestMethod);
      }
    } finally {
      if (restRequestInfo.getRestRequestContent() != null) {
        restRequestInfo.getRestRequestContent().release();
      }
    }
  }

  /**
   * Marks that shutdown is required. When this function returns, shutdown need NOT be complete. Instead, shutdown
   * is guaranteed to happen as soon as the processing of the current {@link RestRequestInfo} is complete. If an
   * immediate shutdown is desired, the thread running the DequeuedRequestHandler should be interrupted.
   * <p/>
   * All {@link RestRequestInfo}s still in the queue will be left unhandled.
   */
  public void shutdownNow() {
    shutdownReady.set(true);
  }

  /**
   * Wait for the shutdown of this instance for the specified time.
   * @param timeout - the amount of time to wait for shutdown.
   * @param timeUnit - time unit of timeout
   * @return
   * @throws InterruptedException
   */
  public boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return shutdownLatch.await(timeout, timeUnit);
  }

  /**
   * Do tasks that are required to be done on completion of {@link RestRequestInfo} handling.
   * @param restRequestInfo - The {@link RestRequestInfo} whose handling completed.
   * @param e - If handling failed, the reason for failure. If handling succeeded, null.
   */
  private void onHandlingComplete(RestRequestInfo restRequestInfo, Exception e) {
    if (restRequestInfo != null) {
      try {
        restRequestInfo.onHandlingComplete(e);
        RestResponseHandler responseHandler = restRequestInfo.getRestResponseHandler();
        if (e != null && responseHandler != null) {
          responseHandler.onRequestComplete(e, false);
        } else if (e != null) {
          logger.error("No error response was sent because there is no response handler."
              + " The connection to the client might still be open. Exception that occurred follows", e);
        }
        if (responseHandler != null && responseHandler.isRequestComplete()) {
          onRequestComplete(restRequestInfo.getRestRequestMetadata());
        }
      } catch (Exception ee) {
        logger.error("Exception while trying to do onHandlingComplete tasks. Original exception also attached", ee, e);
      }
    }
  }

  /**
   * Empties the remaining {@link RestRequestInfo}s in the queue and releases resources held by them.
   * <p/>
   * The implementation releases {@link RestRequestContent} immediately if not null. Since multiple
   * {@link RestRequestInfo} might share the same {@link RestRequestMetadata}, it maintains a map of all seen
   * {@link RestRequestMetadata} and releases them at the end.
   */
  private void emptyQueue() {
    RestRequestInfo dequeuedPart = restRequestInfoQueue.poll();
    while (dequeuedPart != null) {
      RestRequestMetadata metadata = dequeuedPart.getRestRequestMetadata();
      RestRequestContent content = dequeuedPart.getRestRequestContent();
      if (content != null) {
        content.release();
      }
      requestsInFlight.putIfAbsent(metadata, true);
      dequeuedPart = restRequestInfoQueue.poll();
    }
    Iterator<Map.Entry<RestRequestMetadata, Boolean>> requestMetadata = requestsInFlight.entrySet().iterator();
    while (requestMetadata.hasNext()) {
      requestMetadata.next().getKey().release();
      requestMetadata.remove();
    }
  }
}

/**
 * Used to gracefully shutdown a {@link DequeuedRequestHandler} instance. When this is processed by the
 * {@link DequeuedRequestHandler}, it shuts down.
 */
class PoisonInfo extends RestRequestInfo {
  public PoisonInfo() {
    super(null, null, null);
  }
}
