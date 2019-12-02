/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.utils;

import com.codahale.metrics.Clock;
import java.util.concurrent.TimeUnit;


public class MockClock extends Clock {
  private long tick = 0;

  @Override
  public long getTick() {
    return tick;
  }

  public void tick(int seconds) {
    // Meter's TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5)
    tick += TimeUnit.SECONDS.toNanos(seconds) + 1;
  }
}
