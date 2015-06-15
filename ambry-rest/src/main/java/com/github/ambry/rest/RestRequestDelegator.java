package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageResultListener;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that starts up a fixed number of RestMessageHandler instances and hands them out as required.
 * Only one instance of this is expected to be alive and that instance lives through the lifetime of the rest server
 */
public class RestRequestDelegator implements HandleMessageResultListener {

  private final RestServerMetrics restServerMetrics;
  /**
   * Underlying BlobStorageService
   */
  private final BlobStorageService blobStorageService;
  /**
   * For tracking shutdown of message handlers
   */
  private final CountDownLatch messageHandlersUp;
  private final int handlerCount;
  /**
   * The list of RestMessageHandler instances that have been started up
   */
  private final List<RestMessageHandler> messageHandlers;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  /**
   * Executor pool for the RestMessageHandler instances.
   */
  private ExecutorService executor;
  private int currIndex = 0;

  public RestRequestDelegator(int handlerCount, RestServerMetrics restServerMetrics,
      BlobStorageService blobStorageService)
      throws InstantiationException {
    if (handlerCount > 0) {
      this.handlerCount = handlerCount;
      this.restServerMetrics = restServerMetrics;
      this.blobStorageService = blobStorageService;
      messageHandlers = new ArrayList<RestMessageHandler>(handlerCount);
      messageHandlersUp = new CountDownLatch(handlerCount);
    } else {
      throw new InstantiationException("Handlers to be created has to be > 0 - (is " + handlerCount + ")");
    }
  }

  /**
   * Does startup tasks for the delegator. Returns when startup is FULLY complete.
   *
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException {
    logger.info("Request delegator starting");
    try {
      executor = Executors.newFixedThreadPool(handlerCount);
      for (int i = 0; i < handlerCount; i++) {
        RestMessageHandler messageHandler = new RestMessageHandler(blobStorageService, restServerMetrics);
        executor.execute(messageHandler);
        messageHandlers.add(messageHandler);
      }
      logger.info("Request delegator started");
    } catch (Exception e) {
      throw new InstantiationException("Exception while trying to start RestRequestDelegator - " + e);
    }
  }

  /**
   * Returns a RestMessageHandler that can be used to handle incoming messages
   * @return
   * @throws RestServiceException
   */
  public synchronized RestMessageHandler getMessageHandler()
      throws RestServiceException {
    try {
      //Alternative: can have an implementation where we check queue sizes and then return the one with the least
      RestMessageHandler messageHandler = messageHandlers.get(currIndex);
      currIndex = (currIndex + 1) % messageHandlers.size();
      return messageHandler;
    } catch (Exception e) {
      restServerMetrics.delegatorHandlerSelectionErrorCount.inc();
      throw new RestServiceException("Error while trying to pick a handler to return",
          RestServiceErrorCode.HandlerSelectionError);
    }
  }

  /**
   * Does shutdown tasks for the delegator. Returns when shutdown is FULLY complete.
   */
  public void shutdown() {
    if (executor != null) {
      logger.info("Shutting down request delegator");
      try {
        for (int i = 0; i < messageHandlers.size(); i++) {
          messageHandlers.get(i).shutdownGracefully(this);
          // This class implements HandleMessageResultListener. So passing this instance enables us to know
          // when the shutdown has completed.
        }
        executor.shutdown();
        if (!awaitTermination(60, TimeUnit.SECONDS)) {
          logger.error("Request delegator shutdown failed after waiting for 60 seconds");
        } else {
          executor = null;
          logger.info("Request delegator shutdown complete");
        }
      } catch (InterruptedException e) {
        logger.error("Request delegator termination await was interrupted - " + e);
      } catch (RestServiceException e) {
        logger.error("Shutdown of one of the message handlers failed - " + e);
      }
    }
  }

  /**
   * Waits for the message handlers and the executor service to terminate.
   * @param timeout
   * @param timeUnit
   * @return
   * @throws InterruptedException
   */
  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return messageHandlersUp.await(3 * timeout / 4, timeUnit) && executor.awaitTermination(timeout / 4, timeUnit);
  }

  /**
   * Callback for when shutdown of a message handler is complete.
   * @param messageInfo
   */
  public void onMessageHandleSuccess(MessageInfo messageInfo) {
    messageHandlersUp.countDown();
  }

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
    // since we are registering for shutdown only and that can never fail, we should never reach here.
    throw new IllegalStateException("Should not have come here. Original exception" + e);
  }
}
