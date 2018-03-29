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
import java.util.ArrayList;
import java.util.List;
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
  public final Counter selectorNioCloseErrorCount;
  public final Counter selectorDisconnectedErrorCount;
  public final Counter selectorIOErrorCount;
  public final Counter selectorKeyOperationErrorCount;
  public final Counter selectorCloseKeyErrorCount;
  public final Counter selectorCloseSocketErrorCount;
  private final List<AtomicLong> selectorActiveConnectionsList;

  // Plaintext metrics
  // the bytes rate to receive the entire request
  public final Meter plaintextReceiveBytesRate;
  // the bytes rate to send the entire response
  public final Meter plaintextSendBytesRate;
  // the time to receive 1KB data in one read call
  public final Histogram plaintextReceiveTimeInUsPerKB;
  // the time to send 1KB data in one write call
  public final Histogram plaintextSendTimeInUsPerKB;
  // the time to send data in one write call
  public final Histogram plaintextSendTime;

  // SSL metrics
  public final Counter sslFactoryInitializationCount;
  public final Counter sslTransmissionInitializationCount;
  public final Counter sslFactoryInitializationErrorCount;
  public final Counter sslTransmissionInitializationErrorCount;
  public final Histogram sslHandshakeTime;
  public final Counter sslHandshakeCount;
  public final Counter sslHandshakeErrorCount;
  // the bytes rate to receive the entire request
  public final Meter sslReceiveBytesRate;
  // the bytes rate to send the entire response
  public final Meter sslSendBytesRate;
  // the time to receive 1KB data in one read call
  public final Histogram sslReceiveTimeInUsPerKB;
  // the time to send 1KB data in one write call
  public final Histogram sslSendTimeInUsPerKB;
  public final Histogram sslEncryptionTimeInUsPerKB;
  public final Histogram sslDecryptionTimeInUsPerKB;
  // the time to send data in one write call
  public final Histogram sslSendTime;
  // the count of renegotiation after initial handshake done
  public final Counter sslRenegotiationCount;

  // NetworkClient metrics
  public final Histogram networkClientSendAndPollTime;
  public final Histogram requestQueueTime;
  public final Histogram requestSendTime;
  public final Histogram requestSendTotalTime;
  public final Histogram requestResponseRoundTripTime;
  public final Histogram requestResponseTotalTime;

  public final Counter connectionTimeOutError;
  public final Counter networkClientIOError;
  public final Counter networkClientException;
  private List<AtomicLong> networkClientPendingRequestList;

  public NetworkMetrics(MetricRegistry registry) {
    sendInFlight = registry.counter(MetricRegistry.name(Selector.class, "SendInFlight"));
    selectorConnectionClosed = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionClosed"));
    selectorConnectionCreated = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionCreated"));
    selectorSelectCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorSelectCount"));
    selectorIOCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorIOCount"));
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
    plaintextReceiveBytesRate = registry.meter(MetricRegistry.name(Selector.class, "PlaintextReceiveBytesRate"));
    plaintextSendBytesRate = registry.meter(MetricRegistry.name(Selector.class, "PlaintextSendBytesRate"));
    plaintextReceiveTimeInUsPerKB =
        registry.histogram(MetricRegistry.name(Selector.class, "PlaintextReceiveTimeInUsPerKB"));
    plaintextSendTimeInUsPerKB = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextSendTimeInUsPerKB"));
    plaintextSendTime = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextSendTime"));
    sslReceiveBytesRate = registry.meter(MetricRegistry.name(Selector.class, "SslReceiveBytesRate"));
    sslSendBytesRate = registry.meter(MetricRegistry.name(Selector.class, "SslSendBytesRate"));
    sslEncryptionTimeInUsPerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslEncryptionTimeInUsPerKB"));
    sslDecryptionTimeInUsPerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslDecryptionTimeInUsPerKB"));
    sslReceiveTimeInUsPerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslReceiveTimeInUsPerKB"));
    sslSendTimeInUsPerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslSendTimeInUsPerKB"));
    sslSendTime = registry.histogram(MetricRegistry.name(Selector.class, "SslSendTime"));
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

    networkClientSendAndPollTime =
        registry.histogram(MetricRegistry.name(NetworkClient.class, "NetworkClientSendAndPollTime"));
    requestQueueTime = registry.histogram(MetricRegistry.name(NetworkClient.class, "RequestQueueTime"));
    requestSendTime = registry.histogram(MetricRegistry.name(NetworkClient.class, "RequestSendTime"));
    requestSendTotalTime = registry.histogram(MetricRegistry.name(NetworkClient.class, "RequestSendTotalTime"));
    requestResponseRoundTripTime =
        registry.histogram(MetricRegistry.name(NetworkClient.class, "RequestResponseRoundTripTime"));
    requestResponseTotalTime = registry.histogram(MetricRegistry.name(NetworkClient.class, "RequestResponseTotalTime"));
    connectionTimeOutError = registry.counter(MetricRegistry.name(NetworkClient.class, "ConnectionTimeOutError"));
    networkClientIOError = registry.counter(MetricRegistry.name(NetworkClient.class, "NetworkClientIOError"));
    networkClientException = registry.counter(MetricRegistry.name(NetworkClient.class, "NetworkClientException"));

    selectorActiveConnectionsList = new ArrayList<>();
    networkClientPendingRequestList = new ArrayList<>();

    final Gauge<Long> selectorActiveConnectionsCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        long activeConnectionsCount = 0;
        for (AtomicLong activeConnection : selectorActiveConnectionsList) {
          activeConnectionsCount += activeConnection.get();
        }
        return activeConnectionsCount;
      }
    };
    registry.register(MetricRegistry.name(Selector.class, "SelectorActiveConnectionsCount"),
        selectorActiveConnectionsCount);

    final Gauge<Long> networkClientPendingRequestsCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        long pendingRequestsCount = 0;
        for (AtomicLong pendingRequest : networkClientPendingRequestList) {
          pendingRequestsCount += pendingRequest.get();
        }
        return pendingRequestsCount;
      }
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
    requestQueueSize = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return channel.getRequestQueueSize();
      }
    };
    registry.register(MetricRegistry.name(SocketRequestResponseChannel.class, "RequestQueueSize"), requestQueueSize);
    responseQueueSize = new ArrayList<Gauge<Integer>>(channel.getNumberOfProcessors());

    for (int i = 0; i < channel.getNumberOfProcessors(); i++) {
      final int index = i;
      responseQueueSize.add(i, new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return channel.getResponseQueueSize(index);
        }
      });
      registry.register(MetricRegistry.name(SocketRequestResponseChannel.class, i + "-ResponseQueueSize"),
          responseQueueSize.get(i));
    }
    numberOfProcessorThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads(processorThreads);
      }
    };
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
