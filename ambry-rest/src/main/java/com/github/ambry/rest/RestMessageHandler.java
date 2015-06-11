package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageEventListener;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for a RestMessageHandler. Handles all incoming messages from the NIOServer
 * <p/>
 * One or many instances of this are created by the Admin/Frontend during startup and they continuously run
 * and process messages that have been put on their queue.
 */
public class RestMessageHandler implements Runnable {

  private final long offerTimeout = 30; //seconds
  private final BlobStorageService blobStorageService;
  private final LinkedBlockingQueue<MessageInfo> messageInfoQueue = new LinkedBlockingQueue<MessageInfo>();
  private final RestServerMetrics restServerMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public RestMessageHandler(BlobStorageService blobStorageService, RestServerMetrics restServerMetrics) {
    this.blobStorageService = blobStorageService;
    this.restServerMetrics = restServerMetrics;
  }

  public void run() {
    while (true) {
      try {
        MessageInfo messageInfo = null;
        try {
          messageInfo = messageInfoQueue.take();
          if (messageInfo instanceof PoisonInfo) {
            messageInfo.onHandleSuccess();
            break;
          }
          processMessage(messageInfo);
          doProcessingSuccessTasks(messageInfo);
        } catch (InterruptedException ie) {
          restServerMetrics.handlerQueueTakeInterruptedErrorCount.inc();
          logger.error("Wait for data in messageInfoQueue was interrupted - " + ie);
        } catch (Exception e) {
          restServerMetrics.handlerMessageProcessingFailureErrorCount.inc();
          logger.error("Exception while trying to process element in messageInfoQueue - " + e);
          doProcessingFailureTasks(messageInfo, e);
        }
      } catch (Exception e) { // net
        //TODO: metric
      }
    }
  }

  public void shutdownGracefully(HandleMessageEventListener handleMessageEventListener)// CHANGE: to hard shutdown?
      throws RestServiceException {
    PoisonInfo poison = new PoisonInfo();
    poison.addListener(handleMessageEventListener);
    queue(poison);
  }

  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException {
    verifyMessageInfo(messageInfo);
    queue(messageInfo);
  }

  private void processMessage(MessageInfo messageInfo)
      throws RestServiceException {
    blobStorageService.handleMessage(messageInfo);
  }

  protected void verifyMessageInfo(MessageInfo messageInfo)
      throws RestServiceException {
    if (messageInfo.getRestRequest() == null) {
      restServerMetrics.handlerRestRequestMissingErrorCount.inc();
      throw new RestServiceException("Message info missing rest request", RestServiceErrorCode.RestRequestMissing);
    } else if (messageInfo.getResponseHandler() == null) {
      restServerMetrics.handlerResponseHandlerMissingErrorCount.inc();
      throw new RestServiceException("Message info missing response handler",
          RestServiceErrorCode.ReponseHandlerMissing);
    } else if (messageInfo.getRestObject() == null) {
      restServerMetrics.handlerRestObjectMissingErrorCount.inc();
      throw new RestServiceException("Message info missing rest object", RestServiceErrorCode.RestObjectMissing);
    }
  }

  private void queue(MessageInfo messageInfo)
      throws RestServiceException {
    int MAX_ATTEMPTS = 3; // magic number
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      try {
        if (messageInfoQueue.offer(messageInfo, offerTimeout, TimeUnit.SECONDS)) {
          return;
        } else {
          restServerMetrics.handlerQueueOfferTookTooLongErrorCount.inc();
          logger.error("Waiting for space to clear up on queue for " + ((attempt + 1) * offerTimeout) + " seconds");
        }
      } catch (InterruptedException e) {
        restServerMetrics.handlerQueueOfferInterruptedErrorCount.inc();
        logger.error("Offer was interrupted - " + e);
      }
    }
    throw new RestServiceException("Attempt to queue message failed", RestServiceErrorCode.MessageQueueingFailure);
  }

  private void doProcessingSuccessTasks(MessageInfo messageInfo) {
    try {
      messageInfo.onHandleSuccess();
    } catch (Exception e) {
      logger.error("Exception while trying to do processing success tasks - " + e);
    }
  }

  private void doProcessingFailureTasks(MessageInfo messageInfo, Exception e) {
    try {
      RestServiceException restServiceException;
      if (e instanceof RestServiceException) {
        restServiceException = (RestServiceException) e;
      } else {
        restServiceException = new RestServiceException(e, RestServiceErrorCode.RequestProcessingFailure);
      }

      messageInfo.onHandleFailure(e);
      onError(messageInfo, restServiceException);
    } catch (Exception ee) {
      logger.error("Exception while trying to do processing failure tasks - " + ee);
    }
  }

  /**
   * Called by processMessage when it detects/catches an error
   *
   * @param messageInfo
   * @param e
   */
  protected void onError(MessageInfo messageInfo, Exception e) {
    try {
      if (messageInfo != null && messageInfo.getResponseHandler() != null) {
        messageInfo.getResponseHandler().onError(e);
      } else {
        logger.error("No error response was sent because there is no response handler."
            + " The connection to the client might still be open");
      }
    } catch (Exception ee) {
      //TODO: metric
      logger.error("Caught exception while trying to handle an error - " + ee);
    }
  }

  /**
   * Called by the NIOServer after the request is complete and the connection is inactive.
   * This is (has to be) called regardless of the request being concluded successfully or
   * unsuccessfully (i.e. connection interruption).
   *
   * @param request
   * @throws Exception
   */
  protected void onRequestComplete(RestRequest request) {
    /**
     * NOTE: This is where any cleanup code has to go. This is (has to be) called regardless of
     * the request being concluded successfully or unsuccessfully (i.e. connection interruption).
     * Any state that is maintained per request has to be destroyed here.
     */
    if (request != null) {
      request.release();
    }
  }
}
