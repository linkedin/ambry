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

import com.sun.management.HotSpotDiagnosticMXBean;
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
  private AtomicLong heapMemoryLastSampleTime = new AtomicLong(0);
  private final int memorySamplingPeriodMs;
  private long maxHeapMemory = Runtime.getRuntime().maxMemory();
  private int heapMemoryPercentage;

  private final long maxDirectMemory;
  private int directMemoryPercentage;
  private AtomicLong directMemoryLastSampleTime = new AtomicLong(0);
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

    // MaxDirectMemorySize comes from jvm option -XX:MaxDirectMemorySize. If this option is not set, max direct memory equals to
    // the value of Runtime.getRuntime().maxMemory(). https://stackoverflow.com/questions/3773775/default-for-xxmaxdirectmemorysize
    long valueFromVmOption = Long.parseLong(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
        .getVMOption("MaxDirectMemorySize")
        .getValue());
    if (valueFromVmOption == 0) {
      maxDirectMemory = maxHeapMemory;
    } else {
      maxDirectMemory = valueFromVmOption;
    }

    logger.info("Max heap memory {}MB, Max direct memory: {}MB", maxHeapMemory / 1024 / 1024,
        maxDirectMemory / 1024 / 1024);
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

  private int getHeapMemoryPercentage() {
    // In experiments, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() takes 2-6ms
    if (System.currentTimeMillis() <= heapMemoryLastSampleTime.get() + memorySamplingPeriodMs) {
      return heapMemoryPercentage;
    }
    long usedHeapMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    heapMemoryPercentage = (int) (usedHeapMemory * 100 / maxHeapMemory);
    heapMemoryLastSampleTime.set(System.currentTimeMillis());
    logger.trace("Heap memory total: {}  used: {}, percentage: {}", maxHeapMemory, usedHeapMemory,
        heapMemoryPercentage);
    return heapMemoryPercentage;
  }

  private int getDirectMemoryPercentage() {
    // directMemoryMxBean.getMemoryUsed() takes 2-15ms
    if (System.currentTimeMillis() <= directMemoryLastSampleTime.get() + memorySamplingPeriodMs) {
      return directMemoryPercentage;
    }
    long usedDirectMemory = directMemoryMxBean.getMemoryUsed();
    directMemoryPercentage = (int) (usedDirectMemory * 100 / maxDirectMemory);
    directMemoryLastSampleTime.set(System.currentTimeMillis());
    logger.trace("Direct memory total: {}  used: {}, percentage: {}", maxDirectMemory, usedDirectMemory,
        directMemoryPercentage);
    return directMemoryPercentage;
  }

  int getHardwareResourcePercentage(HardwareResource hardwareResource) {
    if (hardwareResource.equals(HardwareResource.HEAP_MEMORY)) {
      return getHeapMemoryPercentage();
    } else if (hardwareResource.equals(HardwareResource.DIRECT_MEMORY)) {
      return getDirectMemoryPercentage();
    } else if (hardwareResource.equals(HardwareResource.CPU)) {
      return getCpuPercentage();
    }
    throw new IllegalArgumentException("Invalid Hardware Resource " + hardwareResource);
  }
}
