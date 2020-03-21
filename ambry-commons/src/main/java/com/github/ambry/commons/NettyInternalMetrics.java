/**
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
package com.github.ambry.commons;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.utils.Utils;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to register and collect netty internal metrics, like ByteBuf memory usage, etc.
 * To use this metric, please do
 * <p>
 *   {@link NettyInternalMetrics} nettyInternalMetrics = new NettyInternalMetric(new MetricRegistry(), nettyConfig);
 *   nettyInternalMetrics.start();
 * </p>
 *
 * This would start collecting netty metrics.
 *
 * To Stop collecting, please do
 * <p>
 *   nettyInternalMetrics.stop();
 * </p>
 */
public class NettyInternalMetrics {

  private static final Logger logger = LoggerFactory.getLogger(NettyInternalMetrics.class);
  private final MetricRegistry registry;
  private final NettyConfig config;
  private ScheduledExecutorService scheduler = null;
  private AtomicBoolean started = new AtomicBoolean();

  /**
   * Constructor to create a {@link NettyInternalMetrics};
   * @param registry Registry to registry the metrics.
   * @param config {@link NettyConfig}.
   */
  public NettyInternalMetrics(MetricRegistry registry, NettyConfig config) {
    Objects.requireNonNull(registry, "Registry is null");
    Objects.requireNonNull(config, "Netty config is null");
    this.registry = registry;
    this.config = config;
  }

  private volatile int numDirectArenas;
  private volatile int numHeapArenas;
  private volatile int numThreadLocalCaches;
  private volatile long usedHeapMemory;
  private volatile long usedDirectMemory;
  private volatile long numHeapTotalAllocations;
  private volatile long numHeapTotalDeallocations;
  private volatile long numHeapTotalActiveAllocations;
  private volatile long numDirectTotalAllocations;
  private volatile long numDirectTotalDeallocations;
  private volatile long numDirectTotalActiveAllocations;

  private int getNumDirectArenas() {
    return numDirectArenas;
  }

  private int getNumHeapArenas() {
    return numHeapArenas;
  }

  private int getNumThreadLocalCaches() {
    return numThreadLocalCaches;
  }

  private long getUsedHeapMemory() {
    return usedHeapMemory;
  }

  private long getUsedDirectMemory() {
    return usedDirectMemory;
  }

  private long getNumHeapTotalAllocations() {
    return numHeapTotalAllocations;
  }

  private long getNumHeapTotalDeallocations() {
    return numHeapTotalDeallocations;
  }

  private long getNumHeapTotalActiveAllocations() {
    return numHeapTotalActiveAllocations;
  }

  private long getNumDirectTotalAllocations() {
    return numDirectTotalAllocations;
  }

  private long getNumDirectTotalDeallocations() {
    return numDirectTotalDeallocations;
  }

  private long getNumDirectTotalActiveAllocations() {
    return numDirectTotalActiveAllocations;
  }

  private void register() {
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberDirectArenas"),
        (Gauge<Integer>) this::getNumDirectArenas);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberHeapArenas"),
        (Gauge<Integer>) this::getNumHeapArenas);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberThreadLocalCaches"),
        (Gauge<Integer>) this::getNumThreadLocalCaches);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "UsedHeapMemory"),
        (Gauge<Long>) this::getUsedHeapMemory);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "UsedDirectMemory"),
        (Gauge<Long>) this::getUsedDirectMemory);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberHeapTotalAllocations"),
        (Gauge<Long>) this::getNumHeapTotalAllocations);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberHeapTotalDeallocations"),
        (Gauge<Long>) this::getNumHeapTotalDeallocations);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberHeapTotalActiveAllocations"),
        (Gauge<Long>) this::getNumHeapTotalActiveAllocations);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberDirectTotalAllocations"),
        (Gauge<Long>) this::getNumDirectTotalAllocations);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberDirectTotalDeallocations"),
        (Gauge<Long>) this::getNumDirectTotalDeallocations);
    registry.register(MetricRegistry.name(NettyInternalMetrics.class, "NumberDirectTotalActiveAllocations"),
        (Gauge<Long>) this::getNumDirectTotalActiveAllocations);
  }

  /**
   * Start to collect metrics.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      register();
      scheduler = Utils.newScheduler(1, false);
      scheduler.scheduleAtFixedRate(new NettyInternalMetricCollector(), 0, config.nettyMetricsRefreshIntervalSeconds,
          TimeUnit.SECONDS);
      logger.info("Schedule netty internal metric collector");
    }
  }

  /**
   * Stop collecting metrics. This method would stop this singleton effectively. Do not reuse this single after
   * calling this method.
   */
  public void stop() {
    if (started.compareAndSet(true, false)) {
      if (scheduler != null) {
        Utils.shutDownExecutorService(scheduler, config.nettyMetricsStopWaitTimeoutSeconds, TimeUnit.SECONDS);
        logger.info("De-scheduled for collecting netty internal metrics");
      }
    }
  }

  private class NettyInternalMetricCollector implements Runnable {

    @Override
    public void run() {
      PooledByteBufAllocatorMetric metric = PooledByteBufAllocator.DEFAULT.metric();
      numDirectArenas = metric.numDirectArenas();
      numHeapArenas = metric.numHeapArenas();
      numThreadLocalCaches = metric.numThreadLocalCaches();
      usedHeapMemory = metric.usedHeapMemory();
      usedDirectMemory = metric.usedDirectMemory();
      List<PoolArenaMetric> heapArenaMetrics = metric.heapArenas();
      List<PoolArenaMetric> directArenaMetrics = metric.directArenas();
      numHeapTotalAllocations = heapArenaMetrics.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
      numHeapTotalDeallocations = heapArenaMetrics.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();
      numHeapTotalActiveAllocations = heapArenaMetrics.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
      numDirectTotalAllocations = directArenaMetrics.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
      numDirectTotalDeallocations = directArenaMetrics.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();
      numDirectTotalActiveAllocations =
          directArenaMetrics.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
    }
  }
}
