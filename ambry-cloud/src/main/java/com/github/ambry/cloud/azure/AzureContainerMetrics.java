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
