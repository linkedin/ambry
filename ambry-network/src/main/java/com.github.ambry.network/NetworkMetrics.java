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
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Metrics for the network layer
 */
public class NetworkMetrics {
  private final MetricRegistry registry;

  // Selector metrics
  public final Counter sendInFlight;
  public final Counter selectorConnectionClosed;
  public final Counter selectorConnectionCreated;
  public final Counter selectorSelectRate;
  public final Histogram selectorSelectTime;
  public final Counter selectorIORate;
  public final Histogram selectorIOTime;
  public final Counter selectorNioCloseErrorCount;
  public final Counter selectorDisconnectedErrorCount;
  public final Counter selectorIOErrorCount;
  public final Counter selectorKeyOperationErrorCount;
  public final Counter selectorCloseKeyErrorCount;
  public final Counter selectorCloseSocketErrorCount;
  public Gauge<Long> selectorActiveConnections;
  public final Map<String, SelectorNodeMetric> selectorNodeMetricMap;

  // Plaintext metrics
  // the bytes rate to receive the entire request
  public final Histogram plaintextReceiveBytesRate;
  // the bytes rate to send the entire response
  public final Histogram plaintextSendBytesRate;
  // the time to receive 1KB data in one read call
  public final Histogram plaintextReceiveTimePerKB;
  // the time to send data in one write call
  public final Histogram plaintextSendTimePerKB;

  // SSL metrics
  public final Counter sslFactoryInitializationCount;
  public final Counter sslTransmissionInitializationCount;
  public final Counter sslFactoryInitializationErrorCount;
  public final Counter sslTransmissionInitializationErrorCount;
  public final Histogram sslHandshakeTime;
  public final Counter sslHandshakeCount;
  public final Counter sslHandshakeErrorCount;
  // the bytes rate to receive the entire request
  public final Histogram sslReceiveBytesRate;
  // the bytes rate to send the entire response
  public final Histogram sslSendBytesRate;
  // the time to receive 1KB data in one read call
  public final Histogram sslReceiveTimePerKB;
  // the time to send data in one write call
  public final Histogram sslSendTimePerKB;
  public final Histogram sslEncryptionTimePerKB;
  public final Histogram sslDecryptionTimePerKB;
  // the count of renegotiation after initial handshake done
  public final Counter sslRenegotiationCount;

  public NetworkMetrics(MetricRegistry registry) {
    this.registry = registry;
    sendInFlight = registry.counter(MetricRegistry.name(Selector.class, "SendInFlight"));
    selectorConnectionClosed = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionClosed"));
    selectorConnectionCreated = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionCreated"));
    selectorSelectRate = registry.counter(MetricRegistry.name(Selector.class, "SelectorSelectRate"));
    selectorIORate = registry.counter(MetricRegistry.name(Selector.class, "SelectorIORate"));
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
    plaintextReceiveBytesRate = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextReceiveBytesRate"));
    plaintextSendBytesRate = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextSendBytesRate"));
    plaintextReceiveTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextReceiveTimePerKB"));
    plaintextSendTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "PlaintextSendTimePerKB"));
    sslReceiveBytesRate = registry.histogram(MetricRegistry.name(Selector.class, "SslReceiveBytesRate"));
    sslSendBytesRate = registry.histogram(MetricRegistry.name(Selector.class, "SslSendBytesRate"));
    sslEncryptionTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslEncryptionTimePerKB"));
    sslDecryptionTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslDecryptionTimePerKB"));
    sslReceiveTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslReceiveTimePerKB"));
    sslSendTimePerKB = registry.histogram(MetricRegistry.name(Selector.class, "SslSendTimePerKB"));
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
    selectorNodeMetricMap = new HashMap<String, SelectorNodeMetric>();
  }

  public void initializeSelectorMetricsIfRequired(final AtomicLong activeConnections) {
    selectorActiveConnections = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return activeConnections.get();
      }
    };
  }

  public void initializeSelectorNodeMetricIfRequired(String hostname, int port) {
    if (!selectorNodeMetricMap.containsKey(hostname + port)) {
      SelectorNodeMetric nodeMetric = new SelectorNodeMetric(registry, hostname, port);
      selectorNodeMetricMap.put(hostname + port, nodeMetric);
    }
  }

  public void updateNodeSendMetric(String hostName, int port, long bytesSentCount, long timeTakenToSendInMs) {
    if (!selectorNodeMetricMap.containsKey(hostName + port)) {
      throw new IllegalArgumentException("Node " + hostName + " with port " + port + " does not exist in metric map");
    }
    SelectorNodeMetric nodeMetric = selectorNodeMetricMap.get(hostName + port);
    nodeMetric.sendCount.inc();
    nodeMetric.bytesSentCount.inc(bytesSentCount);
    nodeMetric.bytesSentLatency.update(timeTakenToSendInMs);
  }

  public void updateNodeReceiveMetric(String hostName, int port, long bytesReceivedCount, long timeTakenToReceiveInMs) {
    if (!selectorNodeMetricMap.containsKey(hostName + port)) {
      throw new IllegalArgumentException("Node " + hostName + " with port " + port + " does not exist in metric map");
    }
    SelectorNodeMetric nodeMetric = selectorNodeMetricMap.get(hostName + port);
    nodeMetric.bytesReceivedCount.inc(bytesReceivedCount);
    nodeMetric.bytesReceivedLatency.update(timeTakenToReceiveInMs);
  }

  class SelectorNodeMetric {
    public final Counter sendCount;
    public final Histogram bytesSentLatency;
    public final Histogram bytesReceivedLatency;
    public final Counter bytesSentCount;
    public final Counter bytesReceivedCount;

    public SelectorNodeMetric(MetricRegistry registry, String hostname, int port) {
      sendCount = registry.counter(MetricRegistry.name(Selector.class, hostname + "-" + port + "-SendCount"));
      bytesSentLatency =
          registry.histogram(MetricRegistry.name(Selector.class, hostname + "-" + port + "- BytesSentLatencyInMs"));
      bytesReceivedLatency =
          registry.histogram(MetricRegistry.name(Selector.class, hostname + "-" + port + "- BytesReceivedLatencyInMs"));
      bytesSentCount = registry.counter(MetricRegistry.name(Selector.class, hostname + "-" + port + "-BytesSentCount"));
      bytesReceivedCount =
          registry.counter(MetricRegistry.name(Selector.class, hostname + "-" + port + "-BytesReceivedCount"));
    }
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
