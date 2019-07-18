package com.github.ambry.network;

import java.util.Collection;
import java.util.Collections;


public class RequestBundle {
  private final Request requestToServe;
  private final Collection<Request> requestsToDrop;

  RequestBundle(Request requestToServe, Collection<Request> requestsToDrop) {
    this.requestToServe = requestToServe;
    this.requestsToDrop = requestsToDrop;
  }

  /**
   * @return a request that the request handler should serve. Can be null if there are no pending requests that have
   * not timed out.
   */
  public Request getRequestToServe() {
    return requestToServe;
  }

  /**
   * @return requests that have spent too long in the queue and should be dropped by the request handler.
   */
  public Collection<Request> getRequestsToDrop() {
    return requestsToDrop;
  }
}
