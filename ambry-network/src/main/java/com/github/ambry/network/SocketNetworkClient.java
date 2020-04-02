/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link NetworkClient} that provides a method for sending a list of requests in the form of {@link Send} to a host:port,
 * and receive responses for sent requests. Requests that come in via {@link #sendAndPoll(List, Set, int)} call,
 * that could not be immediately sent are queued, and an attempt will be made in subsequent invocations of the call (or
 * until they time out).
 * (Note: We will empirically determine whether, rather than queueing a request,
 * a request should be failed if connections could not be checked out if pool limit for its hostPort has been reached
 * and all connections to the hostPort are unavailable).
 *
 * This class is not thread safe.
 */
public class SocketNetworkClient implements NetworkClient {
  private final Selector selector;
  private final ConnectionTracker connectionTracker;
  private final NetworkConfig networkConfig;
  private final NetworkMetrics networkMetrics;
  private final Time time;
  private final LinkedList<RequestMetadata> pendingRequests;
  private final HashMap<String, RequestMetadata> connectionIdToRequestInFlight;
  private final HashMap<Integer, String> correlationIdInFlightToConnectionId;
  private final HashMap<String, RequestMetadata> pendingConnectionsToAssociatedRequests;
  private final AtomicLong numPendingRequests;
  private final int checkoutTimeoutMs;
  private long nextReplenishMs;
  private boolean closed = false;
  private static final Logger logger = LoggerFactory.getLogger(SocketNetworkClient.class);

  /**
   * Instantiates a SocketNetworkClient.
   * @param selector the {@link Selector} for this SocketNetworkClient
   * @param maxConnectionsPerPortPlainText the maximum number of connections per node per plain text port
   * @param maxConnectionsPerPortSsl the maximum number of connections per node per ssl port
   * @param networkConfig the {@link NetworkConfig} for this SocketNetworkClient
   * @param networkMetrics the metrics to track the network related metrics
   * @param checkoutTimeoutMs the maximum time a request should remain in this SocketNetworkClient's pending queue waiting
   *                          for an available connection to its destination.
   * @param time The Time instance to use.
   */
  public SocketNetworkClient(Selector selector, NetworkConfig networkConfig, NetworkMetrics networkMetrics,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl, int checkoutTimeoutMs, Time time) {
    this.selector = selector;
    this.connectionTracker = new ConnectionTracker(maxConnectionsPerPortPlainText, maxConnectionsPerPortSsl);
    this.networkConfig = networkConfig;
    this.networkMetrics = networkMetrics;
    this.checkoutTimeoutMs = checkoutTimeoutMs;
    this.time = time;
    pendingRequests = new LinkedList<>();
    numPendingRequests = new AtomicLong(0);
    connectionIdToRequestInFlight = new HashMap<>();
    correlationIdInFlightToConnectionId = new HashMap<>();
    pendingConnectionsToAssociatedRequests = new HashMap<>();
    nextReplenishMs = time.milliseconds() + ThreadLocalRandom.current().nextInt(Time.MsPerSec);
    networkMetrics.registerNetworkClientPendingConnections(numPendingRequests);
  }

  /**
   * Attempt to send the given requests and poll for responses from the network via the associated selector. Any
   * requests that could not be sent out will be added to a queue. Every time this method is called, it will first
   * attempt sending the requests in the queue (or time them out) and then attempt sending the newly added requests.
   * @param requestsToSend the list of {@link RequestInfo} representing the requests that need to be sent out. This
   *                     could be empty.
   * @param requestsToDrop the list of correlation IDs representing the requests that can be dropped by
   *                       closing the connection.
   * @param pollTimeoutMs the poll timeout.
   * @return a list of {@link ResponseInfo} representing the responses received for any requests that were sent out
   * so far.
   * @throws IllegalStateException if the SocketNetworkClient is closed.
   */
  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    if (closed || !selector.isOpen()) {
      throw new IllegalStateException("The SocketNetworkClient is closed.");
    }
    long startTime = time.milliseconds();
    List<ResponseInfo> responseInfoList = new ArrayList<>();
    List<NetworkSend> sends = null;
    try {
      for (RequestInfo requestInfo : requestsToSend) {
        pendingRequests.add(new RequestMetadata(requestInfo));
      }
      sends = prepareSends(requestsToDrop, responseInfoList);
      replenishConnections();
      for (Integer correlationId : requestsToDrop) {
        String connectionId = correlationIdInFlightToConnectionId.get(correlationId);
        if (connectionId != null) {
          // we do not need to mutate any of the bookkeeping data structures since that will be handled when dealing
          // with the disconnected list in handleSelectorEvents()
          selector.close(connectionId);
        }
      }
      selector.poll(pollTimeoutMs, sends);
      handleSelectorEvents(responseInfoList);
    } catch (Exception e) {
      logger.error("Received an unexpected error during sendAndPoll(): ", e);
      networkMetrics.networkClientException.inc();
      if (sends != null) {
        sends.forEach(send -> send.getPayload().release());
      }
    } finally {
      numPendingRequests.set(pendingRequests.size());
      networkMetrics.networkClientSendAndPollTime.update(time.milliseconds() - startTime, TimeUnit.MILLISECONDS);
    }
    return responseInfoList;
  }

  /**
   * Process the requests in the pendingRequestsQueue. Create {@link ResponseInfo} for those requests that have timed
   * out while waiting in the queue. Then, attempt to prepare {@link NetworkSend}s by checking out connections for
   * the rest of the requests in the queue.
   * @param requestsToDrop the list of correlation IDs representing the requests that can be dropped. If any of these
   *                       correlation IDs match pending requests, those pending requests will not be sent out.
   * @param responseInfoList the list to populate with responseInfos for requests that timed out waiting for
   *                         connections.
   * @return the list of {@link NetworkSend} objects to hand over to the Selector.
   */
  private List<NetworkSend> prepareSends(Set<Integer> requestsToDrop, List<ResponseInfo> responseInfoList) {
    List<NetworkSend> sends = new ArrayList<>();
    ListIterator<RequestMetadata> iter = pendingRequests.listIterator();

    /* Drop requests that have waited too long */
    while (iter.hasNext()) {
      RequestMetadata requestMetadata = iter.next();
      if (time.milliseconds() - requestMetadata.requestQueuedAtMs > checkoutTimeoutMs) {
        responseInfoList.add(
            new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.ConnectionUnavailable, null));
        logger.trace("Failing request to host {} port {} due to connection unavailability",
            requestMetadata.requestInfo.getHost(), requestMetadata.requestInfo.getPort());
        iter.remove();
        requestMetadata.requestInfo.getRequest().release();
        if (requestMetadata.pendingConnectionId != null) {
          pendingConnectionsToAssociatedRequests.remove(requestMetadata.pendingConnectionId);
          requestMetadata.pendingConnectionId = null;
        }
        networkMetrics.connectionCheckoutTimeoutError.inc();
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
        ReplicaId replicaId = requestMetadata.requestInfo.getReplicaId();
        if (replicaId == null) {
          throw new IllegalStateException("ReplicaId in request is null.");
        }
        if (requestsToDrop.contains(requestMetadata.requestInfo.getRequest().getCorrelationId())) {
          requestMetadata.requestInfo.getRequest().release();
          responseInfoList.add(
              new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.ConnectionUnavailable, null));
          if (requestMetadata.pendingConnectionId != null) {
            pendingConnectionsToAssociatedRequests.remove(requestMetadata.pendingConnectionId);
            requestMetadata.pendingConnectionId = null;
          }
          iter.remove();
        } else {
          String connId = connectionTracker.checkOutConnection(host, port, replicaId.getDataNodeId());
          if (connId == null) {
            networkMetrics.connectionNotAvailable.inc();
            if (requestMetadata.pendingConnectionId == null) {
              if (connectionTracker.mayCreateNewConnection(host, port, replicaId.getDataNodeId())) {
                connId = connectionTracker.connectAndTrack(this::connect, host, port, replicaId.getDataNodeId());
                requestMetadata.pendingConnectionId = connId;
                pendingConnectionsToAssociatedRequests.put(connId, requestMetadata);
                logger.trace("Initiated a connection to host {} port {} ", host, port);
              } else {
                networkMetrics.connectionReachLimit.inc();
              }
            }
          } else {
            if (requestMetadata.pendingConnectionId != null) {
              pendingConnectionsToAssociatedRequests.remove(requestMetadata.pendingConnectionId);
              requestMetadata.pendingConnectionId = null;
            }
            logger.trace("Connection checkout succeeded for {}:{} with connectionId {} ", host, port, connId);
            sends.add(new NetworkSend(connId, requestMetadata.requestInfo.getRequest(), null, time));
            connectionIdToRequestInFlight.put(connId, requestMetadata);
            correlationIdInFlightToConnectionId.put(requestMetadata.requestInfo.getRequest().getCorrelationId(),
                connId);
            iter.remove();
            requestMetadata.onRequestDequeue();
          }
        }
      } catch (IOException e) {
        requestMetadata.requestInfo.getRequest().release();
        networkMetrics.networkClientIOError.inc();
        logger.error("Received exception while checking out a connection", e);
      }
    }
    return sends;
  }

  /**
   * If enabled, initiate new connections to reach the minimum number of open connections per remote host.
   */
  private void replenishConnections() {
    long currentTimeMs = time.milliseconds();
    if (networkConfig.networkClientEnableConnectionReplenishment && currentTimeMs >= nextReplenishMs) {
      int connectionsInitiated = connectionTracker.replenishConnections(this::connect,
          networkConfig.networkClientMaxReplenishmentPerHostPerSecond);
      if (connectionsInitiated > 0) {
        networkMetrics.connectionReplenished.inc(connectionsInitiated);
        logger.debug("replenishConnections initiated {} connections", connectionsInitiated);
        nextReplenishMs = currentTimeMs + Time.MsPerSec;
      }
    }
  }

  /**
   * Warm up connections to dataNodes in a specified time window.
   * If a connection established successfully, it's a successConnection.
   * If a connection failed(for example, target dataNode offline), it's a failedConnection.
   * If a connection breaks after successfully established, it's counted as a failedConnection. This impact the
   * counting in the method, but this is tolerable as other good/bad connections can be handled in next selector.poll().
   * If a connection established after this time window of timeForWarmUp, it can be handled in next selector.poll().
   * <p>
   * This will also set the minimum number of active connections for each of the data nodes. This means that the
   * SocketNetworkClient will attempt to keep a percentage of connections ready for use at all times by initiating extra
   * connections in {@link SocketNetworkClient#sendAndPoll} when a pool drops below this number.
   * @param dataNodeIds warm up target nodes.
   * @param connectionWarmUpPercentagePerDataNode percentage of max connections would like to establish in the warmup.
   * @param timeForWarmUp max time to wait for connections' establish in milliseconds.
   * @param responseInfoList records responses from disconnected connections.
   * @return number of connections established successfully.
   */
  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    logger.info("Connection warm up start.");
    if (dataNodeIds.size() == 0) {
      return 0;
    }
    dataNodeIds.forEach(dataNodeId -> connectionTracker.setMinimumActiveConnectionsPercentage(dataNodeId,
        connectionWarmUpPercentagePerDataNode));
    int expectedConnections = connectionTracker.replenishConnections(this::connect, Integer.MAX_VALUE);

    long startTime = System.currentTimeMillis();
    int successfulConnections = 0;
    int failedConnections = 0;
    while (successfulConnections + failedConnections < expectedConnections) {
      try {
        selector.poll(1000L);
        successfulConnections += selector.connected().size();
        failedConnections += selector.disconnected().size();
        handleSelectorEvents(responseInfoList);
      } catch (IOException e) {
        logger.error("Warm up received unexpected error while polling: ", e);
      }
      if (System.currentTimeMillis() - startTime > timeForWarmUp) {
        break;
      }
    }
    logger.info("Connection warm up done. Tried: {}, Succeeded: {}, Failed: {}, Time elapsed: {} ms",
        expectedConnections, successfulConnections, failedConnections, System.currentTimeMillis() - startTime);

    return successfulConnections;
  }

  /**
   * Start creating a connection to a host using {@link Selector#connect}.
   * @param host the hostname to connect to.
   * @param port the port to connect to.
   * @return the connection ID.
   * @throws IOException if DNS resolution fails on the hostname or if the server is down
   */
  private String connect(String host, Port port) throws IOException {
    return selector.connect(new InetSocketAddress(host, port.getPort()), networkConfig.socketSendBufferBytes,
        networkConfig.socketReceiveBufferBytes, port.getPortType());
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
      pendingConnectionsToAssociatedRequests.remove(connId);
    }

    for (String connId : selector.disconnected()) {
      logger.trace("ConnectionId {} disconnected, removing it from connection tracker", connId);
      DataNodeId dataNodeId = connectionTracker.removeConnection(connId);
      // If this was a pending connection and if there is a request that initiated this connection,
      // mark the corresponding request as failed.
      RequestMetadata requestMetadata = pendingConnectionsToAssociatedRequests.remove(connId);
      if (requestMetadata != null) {
        logger.trace("Pending connectionId {} disconnected", connId);
        pendingRequests.remove(requestMetadata);
        requestMetadata.pendingConnectionId = null;
        responseInfoList.add(new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.NetworkError, null));
      } else {
        // If this was an established connection and if there is a request in flight on this connection,
        // mark the corresponding request as failed.
        requestMetadata = connectionIdToRequestInFlight.remove(connId);
        if (requestMetadata != null) {
          logger.trace("ConnectionId {} with request in flight disconnected", connId);
          correlationIdInFlightToConnectionId.remove(requestMetadata.requestInfo.getRequest().getCorrelationId());
          responseInfoList.add(
              new ResponseInfo(requestMetadata.requestInfo, NetworkClientErrorCode.NetworkError, null));
        } else {
          logger.debug(
              "ConnectionId {} has been failed previously due to long wait and now associated channel to {} timed out",
              connId, dataNodeId);
          // Explicitly set requestInfo = null in ResponseInfo, the OperationController should detect this and directly
          // notify ResponseHandler without handing it over to PutManager/GetManager/DeleteManager/TtlUpdateManager.
          responseInfoList.add(new ResponseInfo(null, NetworkClientErrorCode.NetworkError, null, dataNodeId));
          // No need to call pendingRequests.remove() because it has been removed due to connection unavailability in prepareSends()
        }
      }
      if (requestMetadata != null) {
        requestMetadata.requestInfo.getRequest().release();
      }
      networkMetrics.connectionDisconnected.inc();
    }

    for (NetworkReceive recv : selector.completedReceives()) {
      String connId = recv.getConnectionId();
      logger.trace("Receive completed for connectionId {} and checking in the connection back to connection tracker",
          connId);
      connectionTracker.checkInConnection(connId);
      RequestMetadata requestMetadata = connectionIdToRequestInFlight.remove(connId);
      correlationIdInFlightToConnectionId.remove(requestMetadata.requestInfo.getRequest().getCorrelationId());
      // This would transfer the ownership of the content from BoundedNettyByteBufReceive to ResponseInfo.
      // Don't use this BoundedNettyByteBufReceive anymore.
      responseInfoList.add(new ResponseInfo(requestMetadata.requestInfo, null, recv.getReceivedBytes().content()));
      requestMetadata.onResponseReceive();
    }
  }

  /**
   * Close the SocketNetworkClient and cleanup.
   */
  @Override
  public void close() {
    if (pendingRequests != null) {
      pendingRequests.forEach(requestMetadata -> {
        requestMetadata.requestInfo.getRequest().release();
      });
    } if (connectionIdToRequestInFlight != null) {
      connectionIdToRequestInFlight.values().forEach(requestMetadata -> {
        requestMetadata.requestInfo.getRequest().release();
      });
    } if (pendingConnectionsToAssociatedRequests != null) {
      pendingConnectionsToAssociatedRequests.values().forEach(requestMetadata -> {
        requestMetadata.requestInfo.getRequest().release();
      });
    }
    logger.trace("Closing the SocketNetworkClient");
    selector.close();
    closed = true;
  }

  /**
   * Wake up the SocketNetworkClient if it is within a {@link #sendAndPoll(List, Set, int)} sleep. This wakes
   * up the {@link Selector}, which in turn wakes up the {@link java.nio.channels.Selector}.
   * <br>
   * @see java.nio.channels.Selector#wakeup()
   */
  @Override
  public void wakeup() {
    selector.wakeup();
  }

  /**
   * A class that consists of a {@link RequestInfo} and some metadata related to the request
   */
  private class RequestMetadata {
    // the RequestInfo associated with the request.
    RequestInfo requestInfo;
    // the time at which this request was queued.
    private long requestQueuedAtMs;
    // the time at which this request was sent(or moved from queue to in flight state)
    private long requestDequeuedAtMs;
    // if non-null, this is the connection that was initiated (and not established) on behalf of this request. This
    // information is kept so that the SocketNetworkClient does not keep initiating new connections for the same request, and
    // so that in case this connection establishment fails, the request is failed immediately.
    // Note that this is not necessarily the connection on which this request is sent eventually. This is because
    // if another connection to the same destination becomes available before this pending connection is established,
    // then the request will be sent on it. Similarly, if this connection gets established before a previously
    // initiated connection for an earlier request in the queue, then in the next iteration, the earlier request could
    // check out this connection. This, however, does not affect correctness.
    private String pendingConnectionId;

    RequestMetadata(RequestInfo requestInfo) {
      this.requestInfo = requestInfo;
      this.requestQueuedAtMs = time.milliseconds();
      this.pendingConnectionId = null;
    }

    /**
     * Actions to be done on dequeue of this request and ready to be sent
     */
    void onRequestDequeue() {
      requestDequeuedAtMs = time.milliseconds();
      networkMetrics.networkClientRequestQueueTime.update(requestDequeuedAtMs - requestQueuedAtMs,
          TimeUnit.MILLISECONDS);
    }

    /**
     * Actions to be done on receiving response for the request sent
     */
    void onResponseReceive() {
      networkMetrics.networkClientRoundTripTime.update(time.milliseconds() - requestDequeuedAtMs,
          TimeUnit.MILLISECONDS);
      networkMetrics.networkClientTotalTime.update(time.milliseconds() - requestQueuedAtMs, TimeUnit.MILLISECONDS);
    }
  }
}
