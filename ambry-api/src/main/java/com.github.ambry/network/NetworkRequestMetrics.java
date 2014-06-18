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
  private MetricsHistogram responseSendTimeBySize;
  private MetricsHistogram requestTotalTimeBySize;

  public NetworkRequestMetrics(MetricsHistogram responseQueueTime, MetricsHistogram responseSendTime,
      MetricsHistogram requestTotalTime, MetricsHistogram responseSendTimeBySize,
      MetricsHistogram requestTotalTimeBySize, long timeSpentTillNow) {
    this.responseQueueTime = responseQueueTime;
    this.responseSendTime = responseSendTime;
    this.requestTotalTime = requestTotalTime;
    this.timeSpentTillNow = timeSpentTillNow;
    this.responseSendTimeBySize = responseSendTimeBySize;
    this.requestTotalTimeBySize = requestTotalTimeBySize;
  }

  public void updateResponseQueueTime(long value) {
    responseQueueTime.update(value);
    timeSpentTillNow += value;
  }

  public void updateResponseSendTime(long value) {
    responseSendTime.update(value);
    if (responseSendTimeBySize != null) {
      responseSendTimeBySize.update(value);
    }
    timeSpentTillNow += value;
    requestTotalTime.update(timeSpentTillNow);
    if (requestTotalTimeBySize != null) {
      requestTotalTimeBySize.update(timeSpentTillNow);
    }
  }
}
