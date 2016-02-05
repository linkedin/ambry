package com.github.ambry.network;

import java.util.List;


/**
 * The interface to use for sending out requests and receive responses. An implementing class will be polled for
 * requests and notified on completion of sends, on receiving responses and on connection establishments and
 * disconnections.
 */
public interface Requestor {
  /**
   * Poll the Requestor for a list of requests in the form of {@link NetworkSend} to send out.
   * @return list of {@link NetworkSend}
   */
  public List<NetworkSend> poll();

  /**
   * Notifies the Requestor about network events
   * @param connected a list of connection ids for any connections established.
   * @param disconnected a list of connection ids for any disconnections.
   * @param completedSends a list of {@link NetworkSend} for requests that were successufully sent out.
   * @param completedReceives a list of {@link NetworkReceive} for responses successfully received.
   */
  public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
      List<NetworkReceive> completedReceives);

  /**
   * Notifies the Requestor that the RequestResponseHandler is closing.
   * @param e the exception encountered, if any.
   */
  public void onRequestResponseHandlerShutDown(Exception e);
}
