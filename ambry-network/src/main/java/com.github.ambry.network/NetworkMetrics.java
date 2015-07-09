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
  private final List<Gauge<Integer>> responseQueueSize;
  private final Gauge<Integer> requestQueueSize;
  public final Counter sendInFlight;

  public final Counter selectorConnectionClosed;
  public final Counter selectorConnectionCreated;
  public final Histogram selectorBytesSent;
  public final Histogram selectorBytesReceived;
  public final Counter selectorBytesSentCount;
  public final Counter selectorBytesReceivedCount;
  public final Counter selectorSelectRate;
  public final Histogram selectorSelectTime;
  public final Counter selectorIORate;
  public final Histogram selectorIOTime;
  public Gauge<Long> selectorActiveConnections;
  public final Map<String, SelectorNodeMetric> selectorNodeMetricMap;

  public NetworkMetrics(final SocketRequestResponseChannel channel, MetricRegistry registry) {
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
    selectorBytesSentCount = registry.counter(MetricRegistry.name(SocketServer.class, "SelectorBytesSentCount"));
    selectorBytesReceivedCount =
        registry.counter(MetricRegistry.name(SocketServer.class, "SelectorBytesReceivedCount"));
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
