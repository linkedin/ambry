package com.github.ambry.network;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.List;


/**
 * Metrics for the network layer
 */
public class NetworkMetrics {

  private final List<Gauge<Integer>> responseQueueSize;
  private final Gauge<Integer> requestQueueSize;
  public final Counter sendInFlight;

  public NetworkMetrics(final SocketRequestResponseChannel channel, MetricRegistry registry) {
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
  }
}
