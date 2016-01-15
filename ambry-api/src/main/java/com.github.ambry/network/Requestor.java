package com.github.ambry.network;

import java.util.List;


/**
 * The interface that needs to be implemented by any component that needs to use the RequestResponseHandler to
 * send out requests and receive responses. The latter should be initialized with an instance of an implementing class
 * of this interface, which will be used by it to poll for requests to send out and notify about connections,
 * disconnections, completed sends and receives.
 */
public interface Requestor {
  /**
   * Poll the Requestor for a list of requests in the form of {@link NetworkSend} to send out.
   * @return list of {@link NetworkSend}
   */
  public List<NetworkSend> poll();

  /**
   * Notifies the Requestor about Network events
   * @param connected a list of connection ids for any connections established.
   * @param disconnected a list of connection ids for any disconnections.
   * @param completedSends a list of {@link NetworkSend} for requests that were successufully sent out.
   * @param completedReceives a list of {@link NetworkReceive} for responses successfully received.
   */
  public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
      List<NetworkReceive> completedReceives);

  /**
   * Notifies the Requestor of any exception encountered.
   * @param e the exception encountered.
   */
  public void onException(Exception e);
}
