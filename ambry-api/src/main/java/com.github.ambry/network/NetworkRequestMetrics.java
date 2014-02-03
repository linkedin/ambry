package com.github.ambry.network;

import com.github.ambry.metrics.MetricsHistogram;

/**
 * An interface to track a set of metrics for a network request
 */
public class NetworkRequestMetrics {
  private MetricsHistogram responseQueueTime;
  private MetricsHistogram responseSendTime;
  private MetricsHistogram requestTotalTime;
  private long timeSpentTillNow;

  public NetworkRequestMetrics(MetricsHistogram responseQueueTime,
                               MetricsHistogram responseSendTime,
                               MetricsHistogram requestTotalTime,
                               long timeSpentTillNow) {
    this.responseQueueTime = responseQueueTime;
    this.responseSendTime = responseSendTime;
    this.requestTotalTime = requestTotalTime;
    this.timeSpentTillNow = timeSpentTillNow;
  }

  public void updateResponseQueueTime(long value) {
    responseQueueTime.update(value);
    timeSpentTillNow += value;
  }

  public void updateResponseSendTime(long value) {
    responseSendTime.update(value);
    timeSpentTillNow += value;
    requestTotalTime.update(timeSpentTillNow);
  }
}
