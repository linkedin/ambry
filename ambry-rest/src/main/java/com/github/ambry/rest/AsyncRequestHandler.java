package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequestHandler;
import com.github.ambry.restservice.RestRequestInfo;
import com.github.ambry.restservice.RestRequestMetadata;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link RestRequestHandler} that handles requests submitted by a
 * {@link com.github.ambry.restservice.NioServer} and routes them asynchronously to a {@link BlobStorageService}.
 * <p/>
 * Multiple instances are created by the {@link RequestHandlerController}  and each instance runs continuously to handle
 * requests submitted by the {@link com.github.ambry.restservice.NioServer} in a non-blocking way by queueing them.
 * The requests are then handled async through a {@link BlobStorageService}.
 * <p/>
 * The queue owned by a single AsyncRequestHandler might have parts from multiple requests interleaved but parts of the
 * same request will (have to) be in order. This ordering cannot be enforced by the AsyncRequestHandler but instead has
 * to be enforced by the {@link com.github.ambry.restservice.NioServer} (it is responsible for finding a way to use the
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
  private final LinkedBlockingQueue<RestRequestInfo> restRequestInfoQueue = new LinkedBlockingQueue<RestRequestInfo>();
  private final RestServerMetrics restServerMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AsyncRequestHandler(BlobStorageService blobStorageService, RestServerMetrics restServerMetrics) {
    this.restServerMetrics = restServerMetrics;
    dequeuedRequestHandler = new DequeuedRequestHandler(blobStorageService, restRequestInfoQueue, restServerMetrics);
    dequeuedRequestHandlerThread = new Thread(dequeuedRequestHandler);
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("Starting AsyncRequestHandler..");
    dequeuedRequestHandlerThread.start();
    logger.info("AsyncRequestHandler has started");
  }

  /**
   * Attempts to shutdown the AsyncRequestHandler gracefully by introducing poison into the queue. All
   * {@link RestRequestInfo}s before the poison get handled before shutdown occurs. Any {@link RestRequestInfo}s
   * enqueued after the poison are not processed.
   * <p/>
   * If the graceful shutdown fails, then a shutdown is forced. Any outstanding {@link RestRequestInfo}s are not
   * handled.
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
   * To receive a callback on handling completion, a {@link com.github.ambry.restservice.RestRequestInfoEventListener}
   * needs to be added to the {@link RestRequestInfo}.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be handled.
   * @throws RestServiceException
   */
  @Override
  public void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    if (dequeuedRequestHandlerThread.isAlive()) {
      // check sanity of RestRequestInfo
      if (restRequestInfo == null) {
        restServerMetrics.handlerRestRequestInfoMissingErrorCount.inc();
        throw new RestServiceException("RestRequestInfo is null", RestServiceErrorCode.RestRequestInfoNull);
      } else if (restRequestInfo.getRestRequestMetadata() == null) {
        restServerMetrics.handlerRestRequestMetadataMissingErrorCount.inc();
        throw new RestServiceException("RestRequestInfo missing RestRequestMetadata",
            RestServiceErrorCode.RequestMetadataNull);
      } else if (restRequestInfo.getRestResponseHandler() == null) {
        restServerMetrics.handlerResponseHandlerMissingErrorCount.inc();
        throw new RestServiceException("RestRequestInfo missing RestResponseHandler",
            RestServiceErrorCode.ReponseHandlerNull);
      }
      queue(restRequestInfo);
    } else {
      throw new RestServiceException("Requests cannot be handled because the DequeuedRequestHandler is not alive",
          RestServiceErrorCode.RequestHandlerUnavailable);
    }
  }

  @Override
  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    if (restRequestMetadata != null) {
      restRequestMetadata.release();
    }
  }

  /**
   * Adds the {@link RestRequestInfo} to the queue of {@link RestRequestInfo}s waiting to be handled.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be added to the queue.
   * @throws RestServiceException
   */
  private void queue(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    try {
      if (restRequestInfoQueue.offer(restRequestInfo, OFFER_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        if (restRequestInfo.getRestRequestContent() != null) {
          // Since we are going to handle this async, we need to make sure this object is not recycled.
          // It is released in DequeuedRequestHandler::handleRequest() once the handling is complete.
          restRequestInfo.getRestRequestContent().retain();
        } else if (restRequestInfo.getRestRequestMetadata() != null) {
          // This is the first part of a new request.
          // Since we are going to handle this async, we need to make sure this object is not recycled.
          // It is released in onRequestComplete() once we know that we are not going to use it anymore.
          restRequestInfo.getRestRequestMetadata().retain();
        }
      } else {
        restServerMetrics.handlerQueueOfferTookTooLongErrorCount.inc();
        logger.error("Was not able to enqueue RestRequestInfo within " + OFFER_TIMEOUT_SECONDS + " seconds");
        throw new RestServiceException("Attempt to queue restRequestInfo timed out",
            RestServiceErrorCode.RestRequestInfoQueueingFailure);
      }
    } catch (InterruptedException e) {
      restServerMetrics.handlerQueueOfferInterruptedErrorCount.inc();
      logger.error("Enqueueing of RestRequestInfo was interrupted", e);
      throw new RestServiceException("Attempt to queue restRequestInfo interrupted", e,
          RestServiceErrorCode.RestRequestInfoQueueingFailure);
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
  private final RestServerMetrics restServerMetrics;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean forceShutdown = new AtomicBoolean(false);

  public DequeuedRequestHandler(BlobStorageService blobStorageService,
      LinkedBlockingQueue<RestRequestInfo> restRequestInfoQueue, RestServerMetrics restServerMetrics) {
    this.blobStorageService = blobStorageService;
    this.restRequestInfoQueue = restRequestInfoQueue;
    this.restServerMetrics = restServerMetrics;
  }

  /**
   * Handles any queued {@link RestRequestInfo}s continuously until poisoned or forced to shutdown.
   */
  @Override
  public void run() {
    while (!forceShutdown.get()) {
      try {
        RestRequestInfo restRequestInfo = null;
        try {
          restRequestInfo = restRequestInfoQueue.take();
          if (restRequestInfo instanceof PoisonInfo) {
            restRequestInfo.onCompleted(null);
            break;
          }
          handleRequest(restRequestInfo);
          doHandlingSuccessTasks(restRequestInfo);
        } catch (InterruptedException ie) {
          restServerMetrics.handlerQueueTakeInterruptedErrorCount.inc();
          logger.error("Wait for data in restRequestInfoQueue was interrupted - " + ie);
        } catch (Exception e) {
          restServerMetrics.handlerRequestInfoProcessingFailureErrorCount.inc();
          logger.error("Exception while trying to process element in restRequestInfoQueue", e);
          doHandlingFailureTasks(restRequestInfo, e);
        }
      } catch (Exception e) {
        // net that needs to catch all Exceptions - DequeuedRequestHandler cannot go down.
        //TODO: metric
      }
    }
    shutdownLatch.countDown();
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
          restServerMetrics.handlerUnknownRestMethodErrorCount.inc();
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
   * Do tasks that are required to be done on {@link RestRequestInfo} handling success.
   * @param restRequestInfo - The {@link RestRequestInfo} whose handling succeeded.
   */
  private void doHandlingSuccessTasks(RestRequestInfo restRequestInfo) {
    try {
      restRequestInfo.onCompleted(null);
    } catch (Exception e) {
      logger.error("Exception while trying to do processing success tasks", e);
    }
  }

  /**
   * Do tasks that are required to be done on {@link RestRequestInfo} handling failure.
   * @param restRequestInfo - The {@link RestRequestInfo} whose handling failed.
   * @param e - the reason for the failure.
   */
  private void doHandlingFailureTasks(RestRequestInfo restRequestInfo, Exception e) {
    if (restRequestInfo != null) {
      try {
        restRequestInfo.onCompleted(e);
        if (restRequestInfo.getRestResponseHandler() != null) {
          restRequestInfo.getRestResponseHandler().onError(restRequestInfo.getRestRequestMetadata(), e);
        } else {
          logger.error("No error response was sent because there is no response handler."
              + " The connection to the client might still be open");
        }
      } catch (Exception ee) {
        logger.error("Exception while trying to do processing failure tasks", ee);
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
    forceShutdown.set(true);
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
