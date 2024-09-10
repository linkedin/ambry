/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import java.util.concurrent.atomic.AtomicLong;


public class AzureContainerMetrics {
  Long id;
  AtomicLong drift;

  public AzureContainerMetrics(Long id) {
    this.id = id;
    drift = new AtomicLong(0);
  }

  public Long getDrift() {
    return drift.get();
  }

  public void compareAndSet(long expect, long update) {
    this.drift.compareAndSet(expect, update);
  }
}
