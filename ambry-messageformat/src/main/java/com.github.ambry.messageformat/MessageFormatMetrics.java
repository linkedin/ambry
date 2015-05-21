package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for messageformat
 */
public class MessageFormatMetrics {
  public final Histogram calculateOffsetMessageFormatSendTime;
  public final Histogram readHeaderVersionTime;
  public final Histogram readHeaderTime;
  public final Histogram verifyHeaderAndStoreKeyTime;
  public final Histogram calculateTotalSizeTime;

  public MessageFormatMetrics(MetricRegistry registry) {
    calculateOffsetMessageFormatSendTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "CalculateOffsetMessageFormatSendTime"));
    readHeaderVersionTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "ReadHeaderVersionTime"));
    readHeaderTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "ReadHeaderTime"));
    verifyHeaderAndStoreKeyTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "VerifyHeaderAndStoreKeyTime"));
    calculateTotalSizeTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "CalculateTotalSizeTime"));
  }
}
