package com.github.ambry.admin;

import com.github.ambry.rest.HandleMessageEventListener;
import com.github.ambry.rest.MessageInfo;
import com.github.ambry.rest.RestErrorCode;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMessageHandler;
import com.github.ambry.rest.RestRequestDelegator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin specific implementation of RestRequestDelgator.
 * <p/>
 * When instantiated, we create a configurable number of message handlers.
 * The rest server requests to use a handler and we choose one out of the pool of handlers
 * and return it.
 * Multiple requests can share the same handler i.e. just because we returned a handler does not mean
 * that it is fully occupied. Our job is to just return a handler (any one) when asked for. The handler will
 * take care of serving all the requests that it has been assigned to. Having said this, we would like to
 * balance load among all the handlers.
 */
public class AdminRequestDelegator implements RestRequestDelegator, HandleMessageEventListener {

  private final AdminMetrics adminMetrics;
  private final AdminBlobStorageService adminBlobStorageService;
  private final CountDownLatch adminMessageHandlersUp;
  private final int handlerCount;
  private final List<AdminMessageHandler> adminMessageHandlers;

  private ExecutorService executor;
  private int currIndex = 0;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminRequestDelegator(int handlerCount, AdminMetrics adminMetrics,
      AdminBlobStorageService adminBlobStorageService) {
    this.handlerCount = handlerCount;
    this.adminMetrics = adminMetrics;
    this.adminBlobStorageService = adminBlobStorageService;
    adminMessageHandlers = new ArrayList<AdminMessageHandler>(handlerCount);
    adminMessageHandlersUp = new CountDownLatch(handlerCount);
  }

  public void start()
      throws InstantiationException {
    logger.info("Admin request delegator starting");
    if (handlerCount > 0) {
      executor = Executors.newFixedThreadPool(handlerCount);
      for (int i = 0; i < handlerCount; i++) {
        AdminMessageHandler messageHandler = new AdminMessageHandler(adminMetrics, adminBlobStorageService);
        executor.execute(messageHandler);
        adminMessageHandlers.add(messageHandler);
      }
      logger.info("Admin request delegator started");
    } else {
      throw new InstantiationException("Handlers to be created is <= 0 - (is " + handlerCount + ")");
    }
  }

  public synchronized RestMessageHandler getMessageHandler()
      throws RestException {
    try {
      //Alternative: can have an implementation where we check queue sizes and then return the one with the least
      AdminMessageHandler messageHandler = adminMessageHandlers.get(currIndex);
      currIndex = (currIndex + 1) % adminMessageHandlers.size();
      return messageHandler;
    } catch (Exception e) {
      logger.error("Error while trying to pick a handler to return - " + e);
      adminMetrics.handlerSelectionErrorCount.inc();
      throw new RestException("Error while trying to pick a handler to return", RestErrorCode.HandlerSelectionError);
    }
  }

  public void shutdown()
      throws Exception {
    if(executor != null) {
      logger.info("Shutting down admin request delegator");
      for (int i = 0; i < adminMessageHandlers.size(); i++) {
        adminMessageHandlers.get(i).shutdownGracefully(this);
      }
      executor.shutdown();
      if(!awaitTermination(60, TimeUnit.SECONDS)) {
        throw new Exception("AdminRequestDelegator shutdown failed after waiting for 60 seconds");
      }
      executor = null;
    }
  }

  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return adminMessageHandlersUp.await(3 * timeout/4, timeUnit) && executor.awaitTermination(timeout/4, timeUnit);
  }

  public void onMessageHandleSuccess(MessageInfo messageInfo) {
    adminMessageHandlersUp.countDown();
  }

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
    throw new IllegalStateException("Should not have come here. Original exception" + e);
  }
}
