package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageEventListener;
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
 * Interface for a RestRequestDelegator
 */
public class RestRequestDelegator implements HandleMessageEventListener {

  private final RestServerMetrics restServerMetrics;
  private final BlobStorageService blobStorageService;
  private final CountDownLatch messageHandlersUp;
  private final int handlerCount;
  private final List<RestMessageHandler> messageHandlers;

  private ExecutorService executor;
  private int currIndex = 0;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RestRequestDelegator(int handlerCount, RestServerMetrics restServerMetrics,
      BlobStorageService blobStorageService) {
    this.handlerCount = handlerCount;
    this.restServerMetrics = restServerMetrics;
    this.blobStorageService = blobStorageService;
    messageHandlers = new ArrayList<RestMessageHandler>(handlerCount);
    messageHandlersUp = new CountDownLatch(handlerCount);
  }

  /**
   * Does startup tasks for the delegator
   *
   * @throws Exception
   */
  public void start()
      throws InstantiationException {
    logger.info("Request delegator starting");
    if (handlerCount > 0) {
      executor = Executors.newFixedThreadPool(handlerCount);
      for (int i = 0; i < handlerCount; i++) {
        RestMessageHandler messageHandler = new RestMessageHandler(blobStorageService, restServerMetrics);
        executor.execute(messageHandler);
        messageHandlers.add(messageHandler);
      }
      logger.info("Request delegator started");
    } else {
      throw new InstantiationException("Handlers to be created is <= 0 - (is " + handlerCount + ")");
    }
  }

  /**
   * Returns a RestMessageHandler that can be used to handle incoming messages
   *
   * @return
   * @throws com.github.ambry.restservice.RestServiceException
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
   * Does shutdown tasks for the delegator.
   *
   * @throws Exception
   */
  public void shutdown()
      throws Exception {
    if (executor != null) {
      logger.info("Shutting down request delegator");
      for (int i = 0; i < messageHandlers.size(); i++) {
        messageHandlers.get(i).shutdownGracefully(this);
      }
      executor.shutdown();
      if (!awaitTermination(60, TimeUnit.SECONDS)) {
        throw new Exception("Request delegator shutdown failed after waiting for 60 seconds");
      }
      executor = null;
      logger.info("Request delegator shutdown");
    }
  }

  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return messageHandlersUp.await(3 * timeout / 4, timeUnit) && executor.awaitTermination(timeout / 4, timeUnit);
  }

  public void onMessageHandleSuccess(MessageInfo messageInfo) {
    messageHandlersUp.countDown();
  }

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
    throw new IllegalStateException("Should not have come here. Original exception" + e);
  }
}
