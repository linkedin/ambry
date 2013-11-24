package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;

import java.util.List;

/**
 * The store manager that handles all the stores
 */
public class StoreManager {

  private StoreConfig config;
  private Scheduler scheduler;
  private ReadableMetricsRegistry registry;
  private List<String> dataDirs;

  public StoreManager(StoreConfig config,
                      Scheduler scheduler,
                      ReadableMetricsRegistry registry,
                      List<String> dataDirs) {
    this.config = config;
    this.scheduler = scheduler;
    this.registry = registry;
    this.dataDirs = dataDirs;
  }
}
