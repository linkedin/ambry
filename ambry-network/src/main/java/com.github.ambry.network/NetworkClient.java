/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The NetworkClient provides a method for sending a list of requests in the form of {@link Send} to a host:port,
 * and receive responses for sent requests. Requests that come in via {@link #sendAndPoll(List, int)} call,
 * that could not be immediately sent is queued and an attempt will be made in subsequent invocations of the call (or
 * until they time out).
 * (Note: We will empirically determine whether, rather than queueing a request,
 * a request should be failed if connections could not be checked out if pool limit for its hostPort has been reached
 * and all connections to the hostPort are unavailable).
 *
 * This class is not thread safe.
 */
public class NetworkClient implements Closeable {
  private final Selector selector;
  private final ConnectionTracker connectionTracker;
  private final NetworkConfig networkConfig;
  private final NetworkMetrics networkMetrics;
  private final Time time;
  private final LinkedList<RequestMetadata> pendingRequests;
  private final HashMap<String, RequestMetadata> connectionIdToRequestInFlight;
  private final AtomicLong numPendingRequests;
  private final int checkoutTimeoutMs;
  private boolean closed = false;
  private static final Logger logger = LoggerFactory.getLogger(NetworkClient.class);

  /**
   * Instantiates a NetworkClient.
   * @param selector the {@link Selector} for this NetworkClient
   * @param maxConnectionsPerPortPlainText the maximum number of connections per node per plain text port
   * @param maxConnectionsPerPortSsl the maximum number of connections per node per ssl port
   * @param networkConfig the {@link NetworkConfig} for this NetworkClient
   * @param networkMetrics the metrics to track the network related metrics
   * @param checkoutTimeoutMs the maximum time a request should remain in this NetworkClient's pending queue waiting
   *                          for an available connection to its destination.
   * @param time The Time instance to use.
   */
  public NetworkClient(Selector selector, NetworkConfig networkConfig, NetworkMetrics networkMetrics,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl, int checkoutTimeoutMs, Time time) {
    this.selector = selector;
    this.connectionTracker = new ConnectionTracker(maxConnectionsPerPortPlainText, maxConnectionsPerPortSsl);
    this.networkConfig = networkConfig;
    this.networkMetrics = networkMetrics;
    this.checkoutTimeoutMs = checkoutTimeoutMs;
    this.time = time;
    pendingRequests = new LinkedList<RequestMetadata>();
    numPendingRequests = new AtomicLong(0);
    connectionIdToRequestInFlight = new HashMap<String, RequestMetadata>();
    networkMetrics.registerNetworkClientPendingConnections(numPendingRequests);
  }

  /**
   * Attempt to send the given requests and poll for responses from the network via the associated selector. Any
   * requests that could not be sent out will be added to a queue. Every time this method is called, it will first
   * attempt sending the requests in the queue (or time them out) and then attempt sending the newly added requests.
   * @param requestInfos the list of {@link RequestInfo} representing the requests that need to be sent out. This
   *                     could be empty.
   * @param pollTimeoutMs the poll timeout.
   * @return a list of {@link ResponseInfo} representing the responses received for any requests that were sent out
   * so far.
   * @throws IOException if the {@link Selector} associated with this NetworkClient throws
   * @throws IllegalStateException if the NetworkClient is closed.
   */
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestInfos, int pollTimeoutMs)
      throws IOException {
    long startTime = time.milliseconds();
    try {
      if (closed) {
        throw new IllegalStateException("The NetworkClient is closed.");
      }
      List<ResponseInfo> responseInfoList = new ArrayList<ResponseInfo>();
      for (RequestInfo requestInfo : requestInfos) {
        ClientNetworkRequestMetrics clientNetworkRequestMetrics =
            new ClientNetworkRequestMetrics(networkMetrics.requestQueueTime, networkMetrics.requestSendTime,
                networkMetrics.requestSendTotalTime, 0);
        pendingRequests.add(new RequestMetadata(time.milliseconds(), requestInfo, clientNetworkRequestMetrics));
      }
      List<NetworkSend> sends = prepareSends(responseInfoList);
      selector.poll(pollTimeoutMs, sends);
      handleSelectorEvents(responseInfoList);
      return responseInfoList;
    } finally {
      numPendingRequests.set(pendingRequests.size());
      networkMetrics.networkClientSendAndPollTime.update(time.milliseconds() - startTime);
      logger.trace("Completing a send and poll cycle ");
    }
  }

  /**
   * Process the requests in the pendingRequestsQueue. Create {@link ResponseInfo} for those requests that have timed
   * out while waiting in the queue. Then, attempt to prepare {@link NetworkSend}s by checking out connections for
   * the rest of the requests in the queue.
   * @param responseInfoList the list to populate with responseInfos for requests that timed out waiting for
   *                         connections.
   * @return the list of {@link NetworkSend} objects to hand over to the Selector.
   */
  private List<NetworkSend> prepareSends(List<ResponseInfo> responseInfoList) {
    List<NetworkSend> sends = new ArrayList<NetworkSend>();
    ListIterator<RequestMetadata> iter = pendingRequests.listIterator();

    /* Drop requests that have waited too long */
    while (iter.hasNext()) {
      RequestMetadata requestMetadata = iter.next();
      if (time.milliseconds() - requestMetadata.requestQueuedAtMs > checkoutTimeoutMs) {
        responseInfoList
            .add(new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.ConnectionUnavailable, null));
        logger.trace("Failing request to host {} port {} due to connection unavailability",
            requestMetadata.requestInfo.getHost(), requestMetadata.requestInfo.getPort());
        iter.remove();
        networkMetrics.connectionTimeOutError.inc();
      } else {
        // Since requests are ordered by time, once the first request that cannot be dropped is found,
        // we let that and the rest be iterated over in the next while loop. Just move the cursor backwards as this
        // element needs to be processed.
        iter.previous();
        break;
      }
    }

    while (iter.hasNext()) {
      RequestMetadata requestMetadata = iter.next();
      try {
        String host = requestMetadata.requestInfo.getHost();
        Port port = requestMetadata.requestInfo.getPort();
        String connId = connectionTracker.checkOutConnection(host, port);
        if (connId == null) {
          if (connectionTracker.mayCreateNewConnection(host, port)) {
            connId = selector.connect(new InetSocketAddress(host, port.getPort()), networkConfig.socketSendBufferBytes,
                networkConfig.socketReceiveBufferBytes, port.getPortType());
            connectionTracker.startTrackingInitiatedConnection(host, port, connId);
            logger.trace("Initiated a connection to {}:{} ", host, port);
          }
        } else {
          logger.trace("Connection checkout succeeded for {}:{} with connectionId {} ", host, port, connId);
          sends.add(new NetworkSend(connId, requestMetadata.requestInfo.getRequest(),
              requestMetadata.clientNetworkRequestMetrics, time));
          connectionIdToRequestInFlight.put(connId, requestMetadata);
          iter.remove();
          requestMetadata.onRequestDequeue();
        }
      } catch (IOException e) {
        networkMetrics.networkClientIOError.inc();
        logger.error("Received exception while checking out a connection", e);
      }
    }
    return sends;
  }

  /**
   * Handle Selector events after a poll: newly established connections, new disconnections, newly completed sends and
   * receives.
   * @param responseInfoList the list to populate with {@link ResponseInfo} objects for responses created based on
   *                         the selector events.
   */
  private void handleSelectorEvents(List<ResponseInfo> responseInfoList) {
    for (String connId : selector.connected()) {
      logger.trace("Checking in connection back to connection tracker for connectionId {} ", connId);
      connectionTracker.checkInConnection(connId);
    }

    for (String connId : selector.disconnected()) {
      logger.trace("ConnectionId {} disconnected, removing it from connection tracker", connId);
      connectionTracker.removeConnection(connId);
      RequestMetadata requestMetadata = connectionIdToRequestInFlight.remove(connId);
      if (requestMetadata != null) {
        responseInfoList.add(new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.NetworkError, null));
      }
    }

    for (NetworkReceive recv : selector.completedReceives()) {
      String connId = recv.getConnectionId();
      logger.trace("Receive completed for connectionId {} and checking in the connection back to connection tracker",
          connId);
      connectionTracker.checkInConnection(connId);
      RequestMetadata requestMetadata = connectionIdToRequestInFlight.remove(connId);
      responseInfoList.add(new ResponseInfo(requestMetadata.requestInfo, null, recv.getReceivedBytes().getPayload()));
      requestMetadata.onResponseReceive();
    }
  }

  /**
   * Close the NetworkClient and cleanup.
   */
  @Override
  public void close() {
    logger.trace("Closing the NetworkClient");
    selector.close();
    closed = true;
  }

  /**
   * Wake up the NetworkClient if it is within a {@link #sendAndPoll(List, int)} sleep. This wakes
   * up the {@link Selector}, which in turn wakes up the {@link java.nio.channels.Selector}.
   * <br>
   * @see java.nio.channels.Selector#wakeup()
   */
  public void wakeup() {
    selector.wakeup();
  }

  /**
   * A class that consists of a {@link RequestInfo} and some metadata related to the request
   */
  private class RequestMetadata {
    // to track network request related metrics
    ClientNetworkRequestMetrics clientNetworkRequestMetrics;
    // the RequestInfo associated with the request.
    RequestInfo requestInfo;
    // the time at which this request was queued.
    private long requestQueuedAtMs;
    // the time at which this request was sent(or moved from queue to in flight state)
    private long requestDequeuedAtMs;

    RequestMetadata(long requestQueuedAtMs, RequestInfo requestInfo,
        ClientNetworkRequestMetrics clientNetworkRequestMetrics) {
      this.requestInfo = requestInfo;
      this.requestQueuedAtMs = requestQueuedAtMs;
      this.clientNetworkRequestMetrics = clientNetworkRequestMetrics;
    }

    /**
     * Actions to be done on dequeue of this request and ready to be sent
     */
    void onRequestDequeue() {
      requestDequeuedAtMs = System.currentTimeMillis();
      clientNetworkRequestMetrics.updateQueueTime(requestDequeuedAtMs - requestQueuedAtMs);
    }

    /**
     * Actions to be done on receiving response for the request sent
     */
    void onResponseReceive() {
      networkMetrics.requestResponseRoundTripTime.update(System.currentTimeMillis() - requestDequeuedAtMs);
      networkMetrics.requestResponseTotalTime.update(System.currentTimeMillis() - requestQueuedAtMs);
    }
  }
}
