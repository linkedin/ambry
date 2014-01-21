package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/**
 * Metrics for messageformat
 */
public class MessageFormatMetrics {
  public final Histogram calculateOffsetMessageSendTime;

  public MessageFormatMetrics(MetricRegistry registry) {
    calculateOffsetMessageSendTime =
            registry.histogram(MetricRegistry.name(MessageFormatSend.class, "CalculateOffsetMessageSendTime"));
  }
}
