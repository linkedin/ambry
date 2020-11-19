/*
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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to report hardware usage.
 */
public class HardwareUsageMeter {
  private static final Logger logger = LoggerFactory.getLogger(HardwareUsageMeter.class);
  private AtomicLong cpuLastSampleTime = new AtomicLong(0);
  private final int cpuSamplingPeriodMs;
  private int cpuPercentage;
  private AtomicLong memoryLastSampleTime = new AtomicLong(0);
  private final int memorySamplingPeriodMs;
  private int memoryPercentage;
  private long maxMemory = Runtime.getRuntime().maxMemory();
  private BufferPoolMXBean directMemoryMxBean;
  private OperatingSystemMXBean osBean;

  public HardwareUsageMeter(int cpuSamplingPeriodMs, int memorySamplingPeriodMs) {
    this.cpuSamplingPeriodMs = cpuSamplingPeriodMs;
    this.memorySamplingPeriodMs = memorySamplingPeriodMs;

    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    directMemoryMxBean = pools.stream().filter(bean -> bean.getName().equals("direct")).findFirst().orElse(null);
    if (directMemoryMxBean == null) {
      logger.error("Couldn't get directMemoryMxBean");
    }
    osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
  }

  private int getCpuPercentage() {
    // In experiments, osBean.getSystemCpuLoad() takes 15-25 ms
    if (System.currentTimeMillis() <= cpuLastSampleTime.get() + cpuSamplingPeriodMs) {
      return cpuPercentage;
    }
    cpuPercentage = (int) (osBean.getSystemCpuLoad() * 100);
    cpuLastSampleTime.set(System.currentTimeMillis());
    logger.trace("CPU percentage: {}", cpuPercentage);
    return cpuPercentage;
  }

  private int getMemoryPercentage() {
    // In experiments, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() takes 2-6ms
    // directMemoryMxBean.getMemoryUsed() takes 2-15ms
    if (System.currentTimeMillis() <= memoryLastSampleTime.get() + memorySamplingPeriodMs) {
      return memoryPercentage;
    }
    long usedHeapMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    memoryPercentage = (int) ((usedHeapMemory + directMemoryMxBean.getMemoryUsed()) * 100 / maxMemory);
    memoryLastSampleTime.set(System.currentTimeMillis());
    logger.trace("Memory total: {}  heap: {}, percentage: {}", maxMemory, usedHeapMemory, memoryPercentage);
    return memoryPercentage;
  }

  int getHardwareResourcePercentage(HardwareResource hardwareResource) {
    if (hardwareResource.equals(HardwareResource.MEMORY)) {
      return getMemoryPercentage();
    } else if (hardwareResource.equals(HardwareResource.CPU)) {
      return getCpuPercentage();
    }
    throw new IllegalArgumentException("Invalid Hardware Resource " + hardwareResource);
  }
}
