/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import org.junit.Assert;
import org.junit.Test;

public class CompressionMetricsTest {

  @Test
  public void testConstructor() {
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    metrics.decompressSuccessRate.mark();
    Assert.assertEquals(1, metrics.decompressSuccessRate.getCount());

    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics("ABC");
    algorithmMetrics.compressRate.mark();
    Assert.assertEquals(1, algorithmMetrics.compressRate.getCount());
  }
}
