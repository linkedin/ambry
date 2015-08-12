package com.github.ambry.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This object contains information about a particular piece of a request that is independently enough to determine the
 * operation that needs to be performed, the subject of the operation and a way to return a response to the original
 * caller.
 * <p/>
 * It provides information to associate the piece (represented by {@link RestRequestContent} to the larger request of
 * which it is a part through the {@link RestRequestMetadata} and a reference to a {@link RestResponseHandler} through
 * which response can be returned to the client.
 * <p/>
 * Typically this is the unit of communication between the {@link NioServer} and the {@link BlobStorageService}.
 */
public class RestRequestInfo {

  private final boolean isFirstPart;
  private final RestRequestMetadata restRequestMetadata;
  private final RestRequestContent restRequestContent;
  private final RestResponseHandler restResponseHandler;
  private final AtomicBoolean operationComplete = new AtomicBoolean(false);
  private final List<RestRequestInfoEventListener> listeners =
      Collections.synchronizedList(new ArrayList<RestRequestInfoEventListener>());
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Exception exception = null;

  /**
   * Specifies whether this RestRequestInfo is the the first part of a request.
   * @return - whether this is the first part of a request.
   */
  public boolean isFirstPart() {
    return isFirstPart;
  }

  /**
   * Metadata that is needed to process every piece (like RestMethod, URI etc).
   * @return - the {@link RestRequestMetadata} representing metadata about the request.
   */
  public RestRequestMetadata getRestRequestMetadata() {
    return restRequestMetadata;
  }

  /**
   * Piece that needs to be handled in the current RestRequestInfo.
   * <p/>
   * For the very first RestRequestInfo object of a request this will be null.
   * <p/>
   * Pieces that belong to a single request are expected to have a reference to the same {@link RestRequestMetadata}.
   * @return - the {@link RestRequestContent} containing content that might be a piece of a larger request.
   */
  public RestRequestContent getRestRequestContent() {
    return restRequestContent;
  }

  /**
   * Reference to the {@link RestResponseHandler} that can be used to return responses to the client.
   * @return - a {@link RestResponseHandler} that provides APIs to return responses to the client.
   */
  public RestResponseHandler getRestResponseHandler() {
    return restResponseHandler;
  }

  public RestRequestInfo(RestRequestMetadata restRequestMetadata, RestRequestContent restRequestContent,
      RestResponseHandler restResponseHandler) {
    this(restRequestMetadata, restRequestContent, restResponseHandler, false);
  }

  public RestRequestInfo(RestRequestMetadata restRequestMetadata, RestRequestContent restRequestContent,
      RestResponseHandler restResponseHandler, boolean isFirstPart) {
    this.restRequestMetadata = restRequestMetadata;
    this.restRequestContent = restRequestContent;
    this.restResponseHandler = restResponseHandler;
    this.isFirstPart = isFirstPart;
  }

  /**
   * Register to be notified about handling results for this RestRequestInfo.
   * @param restRequestInfoEventListener - the listener that needs to be notified of handling completion.
   */
  public RestRequestInfo addListener(RestRequestInfoEventListener restRequestInfoEventListener) {
    if (restRequestInfoEventListener != null) {
      if (operationComplete.get()) {
        logger.trace("Firing onComplete() for late listener");
        restRequestInfoEventListener.onHandlingComplete(this, exception);
      } else {
        synchronized (listeners) {
          if (operationComplete.get()) {
            logger.trace("Firing onComplete() for late listener");
            restRequestInfoEventListener.onHandlingComplete(this, exception);
          } else {
            listeners.add(restRequestInfoEventListener);
          }
        }
      }
    }
    return this;
  }

  /**
   * Notify listeners of handling completion. If there was an {@link Exception}, e will be non-null (this defines
   * failure).
   * @param e - the {@link Exception} that caused the handling to fail.
   */
  public void onComplete(Exception e) {
    if (operationComplete.compareAndSet(false, true)) {
      logger.trace("Firing onComplete() for listeners");
      exception = e;
      synchronized (listeners) {
        for (RestRequestInfoEventListener listener : listeners) {
          try {
            listener.onHandlingComplete(this, e);
          } catch (Exception ee) {
            logger.error("Swallowing onComplete listener exception", ee);
          }
        }
      }
    } else {
      logger.error("onComplete for RestRequestInfo has already been fired. Ignoring current invocation");
    }
  }

  @Override
  public String toString() {
    return "Request metadata: " + restRequestMetadata + " Request content: " + restRequestContent
        + " Response handler: " + restResponseHandler;
  }
}
