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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Metrics for the network layer
 */
public class NetworkMetrics {
  // Selector metrics
  public final Counter sendInFlight;
  public final Counter selectorConnectionClosed;
  public final Counter selectorConnectionCreated;
  public final Counter selectorSelectCount;
  public final Histogram selectorSelectTime;
  public final Counter selectorIOCount;
  public final Histogram selectorIOTime;
  public final Counter selectorReadyKeyCount;
  public final Counter selectorPrepareKeyCount;
  public final Counter selectorReadKeyCount;
  public final Counter selectorWriteKeyCount;
  public final Counter selectorNioCloseErrorCount;
  public final Counter selectorDisconnectedErrorCount;
  public final Counter selectorIOErrorCount;
  public final Counter selectorKeyOperationErrorCount;
  public final Counter selectorCloseKeyErrorCount;
  public final Counter selectorCloseSocketErrorCount;
  private final List<AtomicLong> selectorActiveConnectionsList;
  private final List<Set<String>> selectorUnreadyConnectionsList;

  // Transmission metrics
  // The time from NetworkSend create to send start
  public final Histogram transmissionSendPendingTime;
  // From the first byte to all data write to socket channel
  public final Histogram transmissionSendAllTime;
  // From last byte of request sent out to first byte of response received
  public final Histogram transmissionRoundTripTime;
  // From the first byte to all data read from socket channel
  public final Histogram transmissionReceiveAllTime;
  // The bytes rate to send the entire response
  public final Meter transmissionSendBytesRate;
  //Tthe bytes rate to receive the entire request
  public final Meter transmissionReceiveBytesRate;

  // For a single read/write in transmission
  // The time to send data in one write call
  public final Histogram transmissionSendTime;
  // The size of date sent in one write call
  public final Histogram transmissionSendSize;
  // The time to send data in one read call
  public final Histogram transmissionReceiveTime;
  // The size of date sent in one read call
  public final Histogram transmissionReceiveSize;

  // SSL metrics
  public final Counter sslFactoryInitializationCount;
  public final Counter sslTransmissionInitializationCount;
  public final Counter sslFactoryInitializationErrorCount;
  public final Counter sslTransmissionInitializationErrorCount;
  public final Histogram sslHandshakeTime;
  public final Counter sslHandshakeCount;
  public final Counter sslHandshakeErrorCount;
  // The count of renegotiation after initial handshake done
  public final Counter sslRenegotiationCount;
  public final Meter sslEncryptionTimeInUsPerKB;
  public final Meter sslDecryptionTimeInUsPerKB;

  // NetworkClient metrics
  public final Timer networkClientSendAndPollTime;
  public final Timer networkClientRequestQueueTime;
  public final Timer networkClientRoundTripTime;
  // NetworkClient request queuing time plus round trip time.
  public final Timer networkClientTotalTime;

  public final Counter connectionCheckoutTimeoutError;
  public final Counter connectionNotAvailable;
  public final Counter connectionReachLimit;
  public final Counter connectionDisconnected;
  public final Counter connectionReplenished;
  public final Counter networkClientIOError;
  public final Counter networkClientException;
  private List<AtomicLong> networkClientPendingRequestList;

  public NetworkMetrics(MetricRegistry registry) {
    sendInFlight = registry.counter(MetricRegistry.name(Selector.class, "SendInFlight"));
    selectorConnectionClosed = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionClosed"));
    selectorConnectionCreated = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionCreated"));
    selectorSelectCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorSelectCount"));
    selectorIOCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorIOCount"));
    selectorReadyKeyCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorReadyKeyCount"));
    selectorPrepareKeyCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorPrepareKeyCount"));
    selectorReadKeyCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorReadKeyCount"));
    selectorWriteKeyCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorWriteKeyCount"));
    selectorSelectTime = registry.histogram(MetricRegistry.name(Selector.class, "SelectorSelectTime"));
    selectorIOTime = registry.histogram(MetricRegistry.name(Selector.class, "SelectorIOTime"));
    selectorNioCloseErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorNioCloseErrorCount"));
    selectorDisconnectedErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorDisconnectedErrorCount"));
    selectorIOErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorIoErrorCount"));
    selectorKeyOperationErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorKeyOperationErrorCount"));
    selectorCloseKeyErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorCloseKeyErrorCount"));
    selectorCloseSocketErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorCloseSocketErrorCount"));

    transmissionSendPendingTime =
        registry.histogram(MetricRegistry.name(Selector.class, "TransmissionSendPendingTime"));
    transmissionSendAllTime = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionSendAllTime"));
    transmissionRoundTripTime = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionRoundTripTime"));
    transmissionReceiveAllTime = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionReceiveAllTime"));
    transmissionSendBytesRate = registry.meter(MetricRegistry.name(Selector.class, "TransmissionSendBytesRate"));
    transmissionReceiveBytesRate = registry.meter(MetricRegistry.name(Selector.class, "TransmissionReceiveBytesRate"));
    transmissionSendTime = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionSendTime"));
    transmissionSendSize = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionSendSize"));
    transmissionReceiveTime = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionReceiveTime"));
    transmissionReceiveSize = registry.histogram(MetricRegistry.name(Selector.class, "TransmissionReceiveSize"));

    sslFactoryInitializationCount =
        registry.counter(MetricRegistry.name(Selector.class, "SslFactoryInitializationCount"));
    sslFactoryInitializationErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SslFactoryInitializationErrorCount"));
    sslTransmissionInitializationCount =
        registry.counter(MetricRegistry.name(Selector.class, "SslTransmissionInitializationCount"));
    sslTransmissionInitializationErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SslTransmissionInitializationErrorCount"));
    sslHandshakeTime = registry.histogram(MetricRegistry.name(Selector.class, "SslHandshakeTime"));
    sslHandshakeCount = registry.counter(MetricRegistry.name(Selector.class, "SslHandshakeCount"));
    sslHandshakeErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SslHandshakeErrorCount"));
    sslRenegotiationCount = registry.counter(MetricRegistry.name(Selector.class, "SslRenegotiationCount"));
    sslEncryptionTimeInUsPerKB = registry.meter(MetricRegistry.name(Selector.class, "SslEncryptionTimeInUsPerKB"));
    sslDecryptionTimeInUsPerKB = registry.meter(MetricRegistry.name(Selector.class, "SslDecryptionTimeInUsPerKB"));

    networkClientSendAndPollTime =
        registry.timer(MetricRegistry.name(NetworkClient.class, "NetworkClientSendAndPollTime"));
    networkClientRequestQueueTime =
        registry.timer(MetricRegistry.name(NetworkClient.class, "NetworkClientRequestQueueTime"));
    networkClientRoundTripTime = registry.timer(MetricRegistry.name(NetworkClient.class, "NetworkClientRoundTripTime"));
    networkClientTotalTime = registry.timer(MetricRegistry.name(NetworkClient.class, "NetworkClientTotalTime"));
    connectionCheckoutTimeoutError =
        registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionCheckoutTimeoutError"));
    connectionNotAvailable = registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionNotAvailable"));
    connectionReachLimit = registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionReachLimit"));
    connectionDisconnected = registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionDisconnected"));
    connectionReplenished = registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionReplenished"));
    networkClientIOError = registry.counter(MetricRegistry.name(NetworkClient.class, "NetworkClientIOError"));
    networkClientException = registry.counter(MetricRegistry.name(NetworkClient.class, "NetworkClientException"));

    selectorActiveConnectionsList = new ArrayList<>();
    selectorUnreadyConnectionsList = new ArrayList<>();
    networkClientPendingRequestList = new ArrayList<>();

    final Gauge<Long> selectorActiveConnectionsCount = () -> {
      long activeConnectionsCount = 0;
      for (AtomicLong activeConnection : selectorActiveConnectionsList) {
        activeConnectionsCount += activeConnection.get();
      }
      return activeConnectionsCount;
    };
    registry.register(MetricRegistry.name(Selector.class, "SelectorActiveConnectionsCount"),
        selectorActiveConnectionsCount);

    final Gauge<Long> selectorUnreadyConnectionsCount = () -> {
      long unreadyConnectionCount = 0;
      for (Set<String> unreadyConnection : selectorUnreadyConnectionsList) {
        unreadyConnectionCount += unreadyConnection.size();
      }
      return unreadyConnectionCount;
    };
    registry.register(MetricRegistry.name(Selector.class, "SelectorUnreadyConnectionsCount"),
        selectorUnreadyConnectionsCount);

    final Gauge<Long> networkClientPendingRequestsCount = () -> {
      long pendingRequestsCount = 0;
      for (AtomicLong pendingRequest : networkClientPendingRequestList) {
        pendingRequestsCount += pendingRequest.get();
      }
      return pendingRequestsCount;
    };
    registry.register(MetricRegistry.name(NetworkClient.class, "NetworkClientPendingConnectionsCount"),
        networkClientPendingRequestsCount);
  }

  /**
   * Registers the number of active connections for a selector
   * @param numActiveConnections count of current active connections
   */
  void registerSelectorActiveConnections(final AtomicLong numActiveConnections) {
    selectorActiveConnectionsList.add(numActiveConnections);
  }

  /**
   * Registers the number of unready connections(SSL handshaking) for a selector
   * @param unreadyConnections count of unready connections.
   */
  void registerSelectorUnreadyConnections(final Set<String> unreadyConnections) {
    selectorUnreadyConnectionsList.add(unreadyConnections);
  }

  /**
   * Registers the count of pending connections to be checked out by the Network client
   * @param numPendingConnections the count of pending connections to be checked out
   */
  void registerNetworkClientPendingConnections(final AtomicLong numPendingConnections) {
    networkClientPendingRequestList.add(numPendingConnections);
  }
}

class ServerNetworkMetrics extends NetworkMetrics {
  // SocketRequestResponseChannel metrics
  private final List<Gauge<Integer>> responseQueueSize;
  private final Gauge<Integer> requestQueueSize;

  // SocketServer metrics
  public final Counter acceptConnectionErrorCount;
  public final Counter acceptorShutDownErrorCount;
  public final Counter processorShutDownErrorCount;
  public final Counter processNewResponseErrorCount;
  public Gauge<Integer> numberOfProcessorThreads;

  public ServerNetworkMetrics(final SocketRequestResponseChannel channel, MetricRegistry registry,
      final List<Processor> processorThreads) {
    super(registry);
    requestQueueSize = channel::getRequestQueueSize;
    registry.register(MetricRegistry.name(SocketRequestResponseChannel.class, "RequestQueueSize"), requestQueueSize);
    responseQueueSize = new ArrayList<Gauge<Integer>>(channel.getNumberOfProcessors());

    for (int i = 0; i < channel.getNumberOfProcessors(); i++) {
      final int index = i;
      responseQueueSize.add(i, () -> channel.getResponseQueueSize(index));
      registry.register(MetricRegistry.name(SocketRequestResponseChannel.class, i + "-ResponseQueueSize"),
          responseQueueSize.get(i));
    }
    numberOfProcessorThreads = () -> getLiveThreads(processorThreads);
    registry.register(MetricRegistry.name(SocketServer.class, "NumberOfProcessorThreads"), numberOfProcessorThreads);

    acceptConnectionErrorCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "AcceptConnectionErrorCount"));
    acceptorShutDownErrorCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "AcceptorShutDownErrorCount"));
    processorShutDownErrorCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "ProcessorShutDownErrorCount"));
    processNewResponseErrorCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "ProcessNewResponseErrorCount"));
  }

  private int getLiveThreads(List<Processor> replicaThreads) {
    int count = 0;
    for (Processor thread : replicaThreads) {
      if (thread.isRunning()) {
        count++;
      }
    }
    return count;
  }
}
