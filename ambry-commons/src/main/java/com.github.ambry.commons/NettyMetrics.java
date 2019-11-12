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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A single class to register and collect netty-related metrics.
 * To use this metric, please do
 * <p>
 *   NettyMetrics nettyMetrics = NettyMetrics.INSTANCE().metricRegistry(new MetricRegistry()).nettyConfig(nettyConfig);
 *   nettyMetrics.start();
 * </p>
 *
 * This would start collecting netty metrics.
 *
 * To Stop collecting, please do
 * <p>
 *   neettyMetrics.stop();
 * </p>
 */
public class NettyMetrics {

  private static final Logger logger = LoggerFactory.getLogger(NettyMetrics.class);
  private static NettyMetrics singleton = new NettyMetrics();

  /**
   * Return the singleton instance of this class.
   * @return The {@link NettyMetrics} in singleton;
   */
  public static NettyMetrics INSTANCE() {
    return singleton;
  }

  private NettyMetrics() {}

  private volatile MetricRegistry registry;
  private volatile NettyConfig config;
  private Thread collectorThread;
  private AtomicBoolean started = new AtomicBoolean();
  private CountDownLatch stopLatch = new CountDownLatch(1);

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

  /**
   * Set the {@link MetricRegistry} for netty metrics. If not set, a new {@link MetricRegistry} would
   * be created before collecting metrics.
   * @param registry The {@link MetricRegistry}.
   * @return This singleton NettyMetrics.
   */
  public NettyMetrics metricRegistry(MetricRegistry registry) {
    Objects.requireNonNull(registry, "Registry is null");
    this.registry = registry;
    return this;
  }

  /**
   * Set the {@link NettyConfig} for netty metrics. If not set, a new {@link NettyConfig} would
   * be created before collecting metris.
   * @param config  The {@link NettyConfig}
   * @return This singleton NettyMetrics.
   */
  public NettyMetrics nettyConfig(NettyConfig config) {
    Objects.requireNonNull(config, "NettyConfig is null");
    this.config = config;
    return this;
  }

  private void initializeConfig() {
    NettyConfig thisConfig = this.config;
    if (thisConfig == null) {
      thisConfig = new NettyConfig(new VerifiableProperties(new Properties()));
      this.config = thisConfig;
    }
  }

  private void register() {
    MetricRegistry thisRegistry = this.registry;
    if (thisRegistry == null) {
      thisRegistry = new MetricRegistry();
      this.registry = thisRegistry;
    }

    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberDirectArenas"),
        (Gauge<Integer>) () -> getNumDirectArenas());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberHeapArenas"),
        (Gauge<Integer>) () -> getNumHeapArenas());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberThreadLocalCaches"),
        (Gauge<Integer>) () -> getNumThreadLocalCaches());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "UsedHeapMemory"),
        (Gauge<Long>) () -> getUsedHeapMemory());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "UsedDirectMemory"),
        (Gauge<Long>) () -> getUsedDirectMemory());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberHeapTotalAlloocations"),
        (Gauge<Long>) () -> getNumHeapTotalAllocations());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberHeapTotalDealloocations"),
        (Gauge<Long>) () -> getNumHeapTotalDeallocations());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberHeapTotalActiveAlloocations"),
        (Gauge<Long>) () -> getNumHeapTotalActiveAllocations());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberDirectTotalAlloocations"),
        (Gauge<Long>) () -> getNumDirectTotalAllocations());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberDirectTotalDealloocations"),
        (Gauge<Long>) () -> getNumDirectTotalDeallocations());
    thisRegistry.register(MetricRegistry.name(NettyMetrics.class, "NumberDirectTotalActiveAlloocations"),
        (Gauge<Long>) () -> getNumDirectTotalActiveAllocations());
  }

  /**
   * Start to collect metrics.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      initializeConfig();
      register();
      collectorThread = Utils.newThread("netty-metrics", new NettyMetricCollector(), false);
      collectorThread.start();
      logger.trace("Thread is started to collect netty metrics");
    }
  }

  /**
   * Stop collecting metrics. This method would stop this singleton effectively. Do not reuse this single after
   * calling this method.
   */
  public void stop() {
    if (started.compareAndSet(true, false)) {
      collectorThread.interrupt();
      try {
        stopLatch.await(config.nettyMetricsStopWaitTimeoutSeconds, TimeUnit.SECONDS);
        logger.trace("Thread is stopped for collecting netty metrics");
      } catch (InterruptedException e) {
        logger.error("Failed to stop collector thread", e);
      }
    }
  }

  private class NettyMetricCollector implements Runnable {

    @Override
    public void run() {
      while (true) {
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
        numHeapTotalActiveAllocations =
            heapArenaMetrics.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
        numDirectTotalAllocations = directArenaMetrics.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
        numDirectTotalDeallocations = directArenaMetrics.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();
        numDirectTotalActiveAllocations =
            directArenaMetrics.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();

        try {
          Thread.sleep(config.nettyMetricsRefreshIntervalSeconds * 1000);
        } catch (InterruptedException e) {
          stopLatch.countDown();
          return;
        }
      }
    }
  }
}
