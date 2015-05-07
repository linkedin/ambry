package com.github.ambry.rest;

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
}
