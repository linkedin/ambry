package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;


/**
 * The response at the network layer
 */
class SocketServerResponse implements Response {

  private final int processor;
  private final Request request;
  private final Send output;
  private final ServerNetworkResponseMetrics metrics;
  private long startQueueTimeInMs;

  SocketServerResponse(Request request, Send output, ServerNetworkResponseMetrics metrics) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest) request).getProcessor();
    this.metrics = metrics;
  }

  @Override
  public Send getPayload() {
    return output;
  }

  @Override
  public Request getRequest() {
    return request;
  }

  int getProcessor() {
    return processor;
  }

  void onEnqueueIntoResponseQueue() {
    this.startQueueTimeInMs = SystemTime.getInstance().milliseconds();
  }

  void onDequeueFromResponseQueue() {
    if (metrics != null) {
      metrics.updateQueueTime(SystemTime.getInstance().milliseconds() - startQueueTimeInMs);
    }
  }

  ServerNetworkResponseMetrics getMetrics() {
    return metrics;
  }
}
