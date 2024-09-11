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


/**
 * A class that holds all metrics pertaining to one Azure Storage Container.
 */
public class AzureStorageContainerMetrics {
  /**
   * id is the unique identifier of the azure-container or ambry-partition.
   */
  Long id;
  /**
   * drift is the number of bytes that the azure-container is behind or ahead of the associated ambry-partition.
   * Although we don't emit a positive drift, it is possible to have a positive drift if the azure-container is ahead
   * of a bootstrapping ambry-partition.
   */
  AtomicLong drift;

  public AzureStorageContainerMetrics(Long id) {
    this.id = id;
    drift = new AtomicLong(0);
  }

  public Long getDrift() {
    return drift.get();
  }

  public void setDrift(long expect, long update) {
    this.drift.compareAndSet(expect, update);
  }
}
