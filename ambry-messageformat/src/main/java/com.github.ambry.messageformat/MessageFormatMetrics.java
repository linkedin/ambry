package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/**
 * Metrics for messageformat
 */
public class MessageFormatMetrics {
  public final Histogram calculateOffsetMessageFormatSendTime;

  public MessageFormatMetrics(MetricRegistry registry) {
    calculateOffsetMessageFormatSendTime =
            registry.histogram(MetricRegistry.name(MessageFormatSend.class, "CalculateOffsetMessageFormatSendTime"));
  }
}
