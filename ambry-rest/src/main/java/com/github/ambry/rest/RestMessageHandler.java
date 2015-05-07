package com.github.ambry.rest;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for a RestMessageHandler. Handles all incoming messages from the RestServer
 *
 * One or many instances of this are created by the Admin/Frontend during startup and they continuously run
 * and process messages that have been put on their queue.
 */
public abstract class RestMessageHandler implements Runnable {
  private final long offerTimeout = 30;
  private final LinkedBlockingQueue<MessageInfo> messageInfoQueue = new LinkedBlockingQueue<MessageInfo>();
  private final ServerMetrics serverMetrics;

  protected Logger logger = LoggerFactory.getLogger(getClass());

  protected RestMessageHandler(ServerMetrics serverMetrics) {
    this.serverMetrics = serverMetrics;
  }

  public void handleMessage(MessageInfo messageInfo) {
    queue(messageInfo);
  }

  public void run() {
    while (true) {
      MessageInfo messageInfo = null;
      try {
        messageInfo = messageInfoQueue.take();
        if (messageInfo instanceof PoisonInfo) {
          break;
        }
        processMessage(messageInfo);
      } catch (InterruptedException ie) {
        serverMetrics.handlerQueueTakeInterruptedErrorCount.inc();
        logger.error("Wait for data in messageInfoQueue was interrupted - " + ie);
      } catch (Exception e) {
        serverMetrics.handlerMessageProcessingFailureErrorCount.inc();
        logger.error("Error while trying to process element in messageInfoQueue - " + e);
        onError(messageInfo, e);
      }
    }
  }

  public void shutdownGracefully()
      throws Exception {
    queue(new PoisonInfo());
  }

  private void processMessage(MessageInfo messageInfo)
      throws Exception {
    verifyMessageInfo(messageInfo);
    RestMethod restMethod = messageInfo.getRestRequest().getRestMethod();
    if (restMethod == RestMethod.GET) {
      handleGet(messageInfo);
    } else if (restMethod == RestMethod.POST) {
      handlePost(messageInfo);
    } else if (restMethod == RestMethod.DELETE) {
      handleDelete(messageInfo);
    } else if (restMethod == RestMethod.HEAD) {
      handleHead(messageInfo);
    } else {
      serverMetrics.handlerUnknownHttpMethodErrorCount.inc();
      throw new RestException("Unknown httpMethod - " + restMethod, RestErrorCode.UnknownHttpMethod);
    }
  }

  protected void verifyMessageInfo(MessageInfo messageInfo)
      throws RestException {
    if (messageInfo.getRestRequest() == null) {
      serverMetrics.handlerRestRequestMissingErrorCount.inc();
      throw new RestException("Message info missing rest request", RestErrorCode.RestRequestMissing);
    } else if (messageInfo.getResponseHandler() == null) {
      serverMetrics.handlerResponseHandlerMissingErrorCount.inc();
      throw new RestException("Message info missing response handler", RestErrorCode.ReponseHandlerMissing);
    } else if (messageInfo.getRestObject() == null) {
      serverMetrics.handlerRestObjectMissingErrorCount.inc();
      throw new RestException("Message info missing rest object", RestErrorCode.RestObjectMissing);
    }
  }

  private void queue(MessageInfo messageInfo) {
    int failedAttempts = 0;
    while (true) {
      try {
        if (!messageInfoQueue.offer(messageInfo, offerTimeout, TimeUnit.SECONDS)) {
          failedAttempts++;
          serverMetrics.handlerQueueOfferTookTooLongErrorCount.inc();
          logger.error("Waiting for space to clear up on queue for " + (failedAttempts * offerTimeout) + " seconds");
        } else {
          break;
        }
      } catch (InterruptedException e) {
        serverMetrics.handlerQueueOfferInterruptedErrorCount.inc();
        logger.error("Offer was interrupted - " + e);
      }
    }
  }

  protected abstract void handleGet(MessageInfo messageInfo)
      throws Exception;

  protected abstract void handlePost(MessageInfo messageInfo)
      throws Exception;

  protected abstract void handleDelete(MessageInfo messageInfo)
      throws Exception;

  protected abstract void handleHead(MessageInfo messageInfo)
      throws Exception;

  /**
   * Called by processMessage when it detects/catches an error
   * @param messageInfo
   * @param e
   */
  protected abstract void onError(MessageInfo messageInfo, Exception e);

  /**
   * Called by the RestServer after the request is complete and the connection is inactive.
   * @param request
   * @throws Exception
   */
  protected abstract void onRequestComplete(RestRequest request)
      throws Exception;
}
