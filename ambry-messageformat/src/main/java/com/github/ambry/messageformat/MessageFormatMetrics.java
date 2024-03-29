/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.messageformat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for messageformat
 */
public class MessageFormatMetrics {
  public final Histogram calculateOffsetMessageFormatSendTime;
  public final Counter messageFormatExceptionCount;

  public MessageFormatMetrics(MetricRegistry registry) {
    calculateOffsetMessageFormatSendTime =
        registry.histogram(MetricRegistry.name(MessageFormatSend.class, "CalculateOffsetMessageFormatSendTime"));
    messageFormatExceptionCount =
        registry.counter(MetricRegistry.name(MessageFormatSend.class, "MessageFormatExceptionCount"));
  }
}
