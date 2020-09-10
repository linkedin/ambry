/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.codahale.metrics.MetricRegistry;


/**
 * Factory class to create instance of {@link PassThroughThrottler}.
 */
public class PassThroughThrottlerFactory implements ThrottlerFactory {

  /**
   * Constructor for {@link PassThroughThrottlerFactory}.
   * @param metricRegistry {@link MetricRegistry} object.
   * @param throttlingMode {@link ThrottlingMode} object.
   */
  public PassThroughThrottlerFactory(MetricRegistry metricRegistry, ThrottlingMode throttlingMode, boolean isHttp2) {

  }

  @Override
  public Throttler getThrottler() {
    return new PassThroughThrottler();
  }
}
