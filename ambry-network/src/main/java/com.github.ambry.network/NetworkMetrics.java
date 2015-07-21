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

  // SocketRequestResponseChannel metrics
  private final List<Gauge<Integer>> responseQueueSize;
  private final Gauge<Integer> requestQueueSize;

  // SocketServer metrics
  public final Counter sendInFlight;
  public final Counter selectorBytesSentCount;
  public final Counter selectorBytesReceivedCount;
  public final Counter acceptConnectionErrorCount;
  public final Counter acceptorShutDownErrorCount;
  public final Counter processNewResponseErrorCount;
  public Gauge<Integer> numberOfProcessorThreads;

  // Selector metrics
  public final Counter selectorConnectionClosed;
  public final Counter selectorConnectionCreated;
  public final Histogram selectorBytesSent;
  public final Histogram selectorBytesReceived;
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

  public NetworkMetrics(final SocketRequestResponseChannel channel, MetricRegistry registry,
      final List<Processor> processorThreads) {
    this.registry = registry;
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
    sendInFlight = registry.counter(MetricRegistry.name(SocketServer.class, "SendInFlight"));
    selectorConnectionClosed = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionClosed"));
    selectorConnectionCreated = registry.counter(MetricRegistry.name(Selector.class, "SelectorConnectionCreated"));
    selectorSelectRate = registry.counter(MetricRegistry.name(Selector.class, "SelectorSelectRate"));
    selectorIORate = registry.counter(MetricRegistry.name(Selector.class, "SelectorIORate"));
    selectorSelectTime = registry.histogram(MetricRegistry.name(Selector.class, "SelectorSelectTime"));
    selectorIOTime = registry.histogram(MetricRegistry.name(Selector.class, "SelectorIOTime"));
    selectorBytesReceived = registry.histogram(MetricRegistry.name(Selector.class, "SelectorBytesReceived"));
    selectorBytesSent = registry.histogram(MetricRegistry.name(Selector.class, "SelectorBytesSent"));
    selectorNioCloseErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorNioCloseErrorCount"));
    selectorDisconnectedErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorDisconnectedErrorCount"));
    selectorIOErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorIoErrorCount"));
    selectorKeyOperationErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorKeyOperationErrorCount"));
    selectorCloseKeyErrorCount = registry.counter(MetricRegistry.name(Selector.class, "SelectorCloseKeyErrorCount"));
    selectorCloseSocketErrorCount =
        registry.counter(MetricRegistry.name(Selector.class, "SelectorCloseSocketErrorCount"));

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
    processNewResponseErrorCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "ProcessNewResponseErrorCount"));
    selectorBytesSentCount = registry.counter(MetricRegistry.name(SocketServer.class, "SelectorBytesSentCount"));
    selectorBytesReceivedCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "SelectorBytesReceivedCount"));
    selectorNodeMetricMap = new HashMap<String, SelectorNodeMetric>();
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
