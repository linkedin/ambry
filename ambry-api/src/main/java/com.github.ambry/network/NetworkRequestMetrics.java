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
  private MetricsHistogram responseSendTimeBasedOnBlobSize;
  private MetricsHistogram requestTotalTimeBasedOnBlobSize;

  public NetworkRequestMetrics(MetricsHistogram responseQueueTime, MetricsHistogram responseSendTime,
      MetricsHistogram requestTotalTime, MetricsHistogram responseSendTimeBasedOnBlobSize,
      MetricsHistogram requestTotalTimeBasedOnBlobSize, long timeSpentTillNow) {
    this.responseQueueTime = responseQueueTime;
    this.responseSendTime = responseSendTime;
    this.requestTotalTime = requestTotalTime;
    this.timeSpentTillNow = timeSpentTillNow;
    this.responseSendTimeBasedOnBlobSize = responseSendTimeBasedOnBlobSize;
    this.requestTotalTimeBasedOnBlobSize = requestTotalTimeBasedOnBlobSize;
  }

  public void updateResponseQueueTime(long value) {
    responseQueueTime.update(value);
    timeSpentTillNow += value;
  }

  public void updateResponseSendTime(long value) {
    responseSendTime.update(value);
    if(responseSendTimeBasedOnBlobSize != null) {
      responseSendTimeBasedOnBlobSize.update(value);
    }
    timeSpentTillNow += value;
    requestTotalTime.update(timeSpentTillNow);
    if(requestTotalTimeBasedOnBlobSize != null) {
      requestTotalTimeBasedOnBlobSize.update(timeSpentTillNow);
    }
  }
}
