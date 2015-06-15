package com.github.ambry.rest;

import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageResultListener;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scaling unit that handles all incoming messages enqueued by the NioServer.
 * <p/>
 * Multiple instances are created by the RequestDelegator during startup and each instance runs continuously and
 * processes messages (through the BlobStorageService) that have been enqueued. Messages are enqueued by the NioServer.
 * <p/>
 * Each messageInfoQueue might have messages from multiple requests interleaved but messages of the same request will
 * (have to) be in order. This ordering cannot be enforced by this class but instead has to be enforced by NioServer
 * (it is responsible for finding a way to use the same RestMessageHandler for all chunks of the same request - for
 * an example implementation see com.github.com.ambry.rest.NettyMesssageProcessor).
 * <p/>
 * These are the scaling units of the any RestServer and can be scaled up and down independently of any other component
 * of the RestServer.
 */
public class RestMessageHandler implements Runnable {

  /**
   * Timeout for messageInfoQueue.offer()
   */
  private final long offerTimeout = 30; //seconds
  /**
   * Backing BlobStorageService
   */
  private final BlobStorageService blobStorageService;
  /**
   * Queue of messages that need to be processed.
   */
  private final LinkedBlockingQueue<MessageInfo> messageInfoQueue = new LinkedBlockingQueue<MessageInfo>();
  private final RestServerMetrics restServerMetrics;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public RestMessageHandler(BlobStorageService blobStorageService, RestServerMetrics restServerMetrics) {
    this.blobStorageService = blobStorageService;
    this.restServerMetrics = restServerMetrics;
  }

  public void run() {
    // runs infinitely until poisoned.
    while (true) {
      try {
        MessageInfo messageInfo = null;
        try {
          messageInfo = messageInfoQueue.take();
          if (messageInfo instanceof PoisonInfo) {
            messageInfo.onHandleSuccess(); // acknowledge shutdown.
            break; // poisoned.
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
      } catch (Throwable e) { // net that needs to catch all errors - RestMessageHandler cannot go down for any reason.
        //TODO: metric
      }
    }
  }
  /**
   * TODO: think about whether this should return after complete shutdown (that would take a longer time).
   */
  /**
   * Gracefully shuts down the RestMessageHandler. This is done by introducing poison into the queue.
   * Any message enqueued after the poison is not processed.
   * @param handleMessageResultListener
   * @throws RestServiceException
   */
  public void shutdownGracefully(HandleMessageResultListener handleMessageResultListener)// CHANGE: to hard shutdown?
      throws RestServiceException {
    PoisonInfo poison = new PoisonInfo();
    poison.addListener(handleMessageResultListener);
    queue(poison);
  }

  /**
   * Enqueues this message to be processed later.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException {
    verifyMessageInfo(messageInfo);
    queue(messageInfo);
  }

  /**
   * Process dequeued message.
   * @param messageInfo
   * @throws RestServiceException
   */
  private void processMessage(MessageInfo messageInfo)
      throws RestServiceException {
    RestMethod restMethod = messageInfo.getRestRequest().getRestMethod();
    switch (restMethod) {
      case GET:
        blobStorageService.handleGet(messageInfo);
        break;
      case POST:
        blobStorageService.handlePost(messageInfo);
        break;
      case DELETE:
        blobStorageService.handleDelete(messageInfo);
        break;
      case HEAD:
        blobStorageService.handleHead(messageInfo);
        break;
      default:
        restServerMetrics.handlerUnknownRestMethodErrorCount.inc();
        throw new RestServiceException("Unknown rest method - " + restMethod, RestServiceErrorCode.UnknownRestMethod);
    }
  }

  /**
   * Verifies that all required components are present in the MessageInfo
   * @param messageInfo
   * @throws RestServiceException
   */
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

  /**
   * Adds message to the queue.
   * @param messageInfo
   * @throws RestServiceException
   */
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

  /**
   * Do tasks that are required to be done on message processing success.
   * @param messageInfo
   */
  private void doProcessingSuccessTasks(MessageInfo messageInfo) {
    try {
      messageInfo.onHandleSuccess();
    } catch (Exception e) {
      logger.error("Exception while trying to do processing success tasks - " + e);
    }
  }

  /**
   * Do tasks that are required to be done on message processing failure.
   * @param messageInfo
   * @param e
   */
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
   * Handle the detected exception.
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
   * Do tasks that need to be done after request is complete.
   * <p/>
   * Needs to be called by the NioServer after the request is complete and the connection is inactive.
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
     * Any state that is maintained per request can be destroyed here.
     */
    if (request != null) {
      request.release();
    }
  }
}
