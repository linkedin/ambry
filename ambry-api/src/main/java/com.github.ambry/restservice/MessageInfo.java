package com.github.ambry.restservice;

import java.util.ArrayList;
import java.util.List;


/**
 * Stores all the objects required to handle a particular chunk of a request.
 */
public class MessageInfo {
  /**
   * Metadata that is needed to process every chunk (like RestMethod etc).
   */
  private final RestObject restObject;
  /**
   * Chunk that needs to be handled. For the very first MessageInfo object of a request this is the
   * same as RestRequest.
   */
  private final RestRequest restRequest;
  /**
   * Reference to the RestResponseHandler that can be used to return responses to the client.
   */
  private final RestResponseHandler responseHandler;

  /**
   * The listeners that need to notified about handling results.
   */
  private List<HandleMessageResultListener> listeners = new ArrayList<HandleMessageResultListener>();

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestObject getRestObject() {
    return restObject;
  }

  public RestResponseHandler getResponseHandler() {
    return responseHandler;
  }

  public MessageInfo(RestRequest restRequest, RestObject restObject, RestResponseHandler responseHandler) {
    this.restRequest = restRequest;
    this.restObject = restObject;
    this.responseHandler = responseHandler;
  }

  /**
   * Register to be notified about handling results.
   * @param handleMessageResultListener
   */
  public void addListener(HandleMessageResultListener handleMessageResultListener) {
    if (handleMessageResultListener != null) {
      listeners.add(handleMessageResultListener);
    }
  }

  /**
   * Notify listeners of handling success.
   */
  public void onHandleSuccess() {
    for (HandleMessageResultListener listener : listeners) {
      listener.onMessageHandleSuccess(this);
    }
  }

  /**
   * Notify listeners of handling failure.
   * @param e
   */
  public void onHandleFailure(Exception e) {
    for (HandleMessageResultListener listener : listeners) {
      listener.onMessageHandleFailure(this, e);
    }
  }
}
