package com.github.ambry.rest;

import java.util.ArrayList;
import java.util.List;


/**
 * Stores all the objects required to handle a particular chunk of a request
 * Stores:
 * 1. RestRequest - sort of metadata that is needed to process every chunk
 * 2. RestObject - chunk that needs to be handled. For the very first MessageInfo object of a connection this is the
 * same as RestRequest
 * 3. ResponseHandler - Reference to the RestResponseHandler that the RestMessageHandler will use
 */
public class MessageInfo {
  private final RestObject restObject;
  private final RestRequest restRequest;
  private final RestResponseHandler responseHandler;

  private List<HandleMessageEventListener> listeners = new ArrayList<HandleMessageEventListener>();

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

  public void releaseAll() {
    restRequest.release();
    releaseRestObject();
  }

  public void releaseRestObject() {
    restObject.release();
  }

  public void addListener(HandleMessageEventListener handleMessageEventListener) {
    if (handleMessageEventListener != null) {
      listeners.add(handleMessageEventListener);
    }
  }

  public void onHandleSuccess() {
    for (HandleMessageEventListener listener : listeners) {
      listener.onMessageHandleSuccess(this);
    }
  }

  public void onHandleFailure(Exception e) {
    for (HandleMessageEventListener listener : listeners) {
      listener.onMessageHandleFailure(this, e);
    }
  }
}
