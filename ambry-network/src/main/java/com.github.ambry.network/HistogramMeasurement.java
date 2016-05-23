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
package com.github.ambry.network;

import com.codahale.metrics.Histogram;
import com.github.ambry.metrics.MetricsHistogram;


/**
 * Class to assist in updating the {@link Histogram} with a value
 */
public class HistogramMeasurement implements MetricsHistogram {

  private final Histogram histogram;

  public HistogramMeasurement(Histogram histogram) {
    this.histogram = histogram;
  }

  @Override
  public void update(long value) {
    histogram.update(value);
  }

  @Override
  public void update(int value) {
    histogram.update(value);
  }
}
