/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.metrics;

import com.github.ambry.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;


/**
 * Straight up ripoff of Hadoop's metrics2 JvmMetrics class.
 */
public class JvmMetrics extends MetricsHelper implements Runnable {

  class CounterTuple {
    private Counter counter1;
    private Counter counter2;

    public CounterTuple(Counter counter1, Counter counter2) {
      this.counter1 = counter1;
      this.counter2 = counter2;
    }

    public Counter getCounter1() {
      return counter1;
    }

    public Counter getCounter2() {
      return counter2;
    }
  }

  private final float M = 1024 * 1024.0F;

  private MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  private List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
  private ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private Map<String, CounterTuple> gcBeanCounters = new HashMap<String, CounterTuple>();
  private Scheduler executor = new Scheduler(1, true);
  private Logger logger = LoggerFactory.getLogger(getClass());

  // jvm metrics
  Gauge gMemNonHeapUsedM = newGauge("mem-non-heap-used-mb", 0.0F);
  Gauge gMemNonHeapCommittedM = newGauge("mem-non-heap-committed-mb", 0.0F);
  Gauge gMemHeapUsedM = newGauge("mem-heap-used-mb", 0.0F);
  Gauge gMemHeapCommittedM = newGauge("mem-heap-committed-mb", 0.0F);
  Gauge gThreadsNew = newGauge("threads-new", 0L);
  Gauge gThreadsRunnable = newGauge("threads-runnable", 0L);
  Gauge gThreadsBlocked = newGauge("threads-blocked", 0L);
  Gauge gThreadsWaiting = newGauge("threads-waiting", 0L);
  Gauge gThreadsTimedWaiting = newGauge("threads-timed-waiting", 0L);
  Gauge gThreadsTerminated = newGauge("threads-terminated", 0L);
  Counter cGcCount = newCounter("gc-count");
  Counter cGcTimeMillis = newCounter("gc-time-millis");

  public JvmMetrics(MetricsRegistry registry) {
    super(registry);
  }

  public void start() {
    executor.startup();
    executor.schedule("jvmmetrics", this, 0, 5, TimeUnit.SECONDS);
  }

  public void run() {
    logger.debug("updating jvm metrics");

    updateMemoryUsage();
    updateGcUsage();
    updateThreadUsage();

    logger.debug("updated metrics to: [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]", gMemNonHeapUsedM,
        gMemNonHeapCommittedM, gMemHeapUsedM, gMemHeapCommittedM, gThreadsNew, gThreadsRunnable, gThreadsBlocked,
        gThreadsWaiting, gThreadsTimedWaiting, gThreadsTerminated, cGcCount, cGcTimeMillis);
  }

  public void stop() {
    executor.shutdown();
  }

  private void updateMemoryUsage() {
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    gMemNonHeapUsedM.set(memNonHeap.getUsed() / M);
    gMemNonHeapCommittedM.set(memNonHeap.getCommitted() / M);
    gMemHeapUsedM.set(memHeap.getUsed() / M);
    gMemHeapCommittedM.set(memHeap.getCommitted() / M);
  }

  private void updateGcUsage() {
    long count = 0l;
    long timeMillis = 0l;

    for (GarbageCollectorMXBean gcBean : gcBeans) {
      long c = gcBean.getCollectionCount();
      long t = gcBean.getCollectionTime();
      CounterTuple gcInfo = getGcInfo(gcBean.getName());
      gcInfo.getCounter1().inc(c - gcInfo.getCounter1().getCount());
      gcInfo.getCounter2().inc(t - gcInfo.getCounter2().getCount());
      count += c;
      timeMillis += t;
    }

    cGcCount.inc(count - cGcCount.getCount());
    cGcTimeMillis.inc(timeMillis - cGcTimeMillis.getCount());
  }

  private CounterTuple getGcInfo(String gcName) {
    CounterTuple tuple = gcBeanCounters.get(gcName);
    if (tuple != null) {
      return tuple;
    } else {
      CounterTuple newTuple =
          new CounterTuple(newCounter("%s-gc-count" + gcName), newCounter("%s-gc-time-millis" + gcName));
      gcBeanCounters.put(gcName, newTuple);
      return newTuple;
    }
  }

  private void updateThreadUsage() {
    long threadsNew = 0l;
    long threadsRunnable = 0l;
    long threadsBlocked = 0l;
    long threadsWaiting = 0l;
    long threadsTimedWaiting = 0l;
    long threadsTerminated = 0l;
    long[] threadIds = threadMXBean.getAllThreadIds();

    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 0);
    for (ThreadInfo threadInfo : threadInfos) {
      // check to ensure threadInfo is not null due to race conditions
      if (threadInfo != null) {
        switch (threadInfo.getThreadState()) {
          case NEW:
            threadsNew += 1;
            break;
          case RUNNABLE:
            threadsRunnable += 1;
            break;
          case BLOCKED:
            threadsBlocked += 1;
            break;
          case WAITING:
            threadsWaiting += 1;
            break;
          case TIMED_WAITING:
            threadsTimedWaiting += 1;
            break;
          case TERMINATED:
            threadsTerminated += 1;
            break;
        }
      }
    }
    gThreadsNew.set(threadsNew);
    gThreadsRunnable.set(threadsRunnable);
    gThreadsBlocked.set(threadsBlocked);
    gThreadsWaiting.set(threadsWaiting);
    gThreadsTimedWaiting.set(threadsTimedWaiting);
    gThreadsTerminated.set(threadsTerminated);
  }
}

