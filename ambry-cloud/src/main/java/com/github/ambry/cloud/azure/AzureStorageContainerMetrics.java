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

import java.util.concurrent.ConcurrentHashMap;
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
   * lag is the number of bytes that the azure-container is behind or ahead of the associated ambry-partition.
   * Although we don't emit a positive drift, it is possible to have a positive drift if the azure-container is ahead
   * of a bootstrapping ambry-partition.
   */
  ConcurrentHashMap<String, AtomicLong> replicaLag;

  public AzureStorageContainerMetrics(Long id) {
    this.id = id;
    replicaLag = new ConcurrentHashMap<>();
  }

  public void addPartitionReplica(String hostname) {
    replicaLag.putIfAbsent(hostname, new AtomicLong(Long.MAX_VALUE));
  }

  public void removePartitionReplica(String hostname) {
    replicaLag.remove(hostname);
  }

  public Long getPartitionLag() {
    return replicaLag.values().stream().map(AtomicLong::get).reduce(Long.MAX_VALUE, Long::min);
  }

  public void setPartitionReplicaLag(String hostname, long update) {
    this.replicaLag.get(hostname).compareAndSet(this.replicaLag.get(hostname).get(), update);
  }

}
