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
 *
 */

package com.github.ambry.clustermap;

import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link DataNodeConfigSource} that combines two sources. The "primary" acts as the source of
 * truth for users of this class, but results obtained are compared against the "secondary" source. This class can be
 * useful for safe migrations between different backing stores.
 */
public class CompositeDataNodeConfigSource implements DataNodeConfigSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeDataNodeConfigSource.class);
  static final long CHECK_PERIOD_MS = TimeUnit.MINUTES.toMillis(5);
  static final int MAX_PERIODS_WITH_INCONSISTENCY = 3;
  private final DataNodeConfigSource primarySource;
  private final DataNodeConfigSource secondarySource;
  private final DataNodeConfigSourceMetrics metrics;
  private final Time time;
  private final AtomicBoolean recordingListenersRegistered = new AtomicBoolean(false);

  /**
   * @param primarySource the source of truth for users of this class.
   * @param secondarySource all operations are also attempted against this source, but if the results of the operation
   *                        differ from the primary, inconsistencies are just recorded with logs and metrics.
   * @param time a {@link Time} instance used for consistency verification.
   * @param metrics a {@link DataNodeConfigSourceMetrics} instance to use.
   */
  public CompositeDataNodeConfigSource(DataNodeConfigSource primarySource, DataNodeConfigSource secondarySource,
      Time time, DataNodeConfigSourceMetrics metrics) {
    this.primarySource = primarySource;
    this.secondarySource = secondarySource;
    this.time = time;
    this.metrics = metrics;
  }

  @Override
  public void addDataNodeConfigChangeListener(DataNodeConfigChangeListener listener) throws Exception {
    if (recordingListenersRegistered.compareAndSet(false, true)) {
      ConfigChangeRecorder primaryListener = new ConfigChangeRecorder(listener);
      ConfigChangeRecorder secondaryListener = new ConfigChangeRecorder(null);
      primarySource.addDataNodeConfigChangeListener(primaryListener);
      try {
        secondarySource.addDataNodeConfigChangeListener(secondaryListener);
        Utils.newThread("DataNodeConfigConsistencyChecker",
            new ConsistencyChecker(time, metrics, primaryListener, secondaryListener), false).start();
      } catch (Exception e) {
        metrics.addListenerSwallowedErrorCount.inc();
        LOGGER.error("Error from secondarySource.addDataNodeConfigChangeListener({})", listener, e);
      }
    } else {
      // only set up consistency checking for the first listener. Other listeners will just use with the primary src
      primarySource.addDataNodeConfigChangeListener(listener);
    }
  }

  @Override
  public boolean set(DataNodeConfig config) {
    boolean primaryResult = primarySource.set(config);
    try {
      boolean secondaryResult = secondarySource.set(config);
      if (primaryResult != secondaryResult) {
        metrics.setSwallowedErrorCount.inc();
        LOGGER.error("Results differ for set({}). sourceOfTruth={}, secondarySource={}", config, primaryResult,
            secondaryResult);
      }
    } catch (Exception e) {
      metrics.setSwallowedErrorCount.inc();
      LOGGER.error("Error from secondarySource.set({})", config, e);
    }
    return primaryResult;
  }

  @Override
  public DataNodeConfig get(String instanceName) {
    DataNodeConfig primaryResult = primarySource.get(instanceName);
    try {
      DataNodeConfig secondaryResult = secondarySource.get(instanceName);
      if (!Objects.equals(primaryResult, secondaryResult)) {
        metrics.getSwallowedErrorCount.inc();
        LOGGER.error("Results differ for get({}). sourceOfTruth={}, secondarySource={}", instanceName, primaryResult,
            secondaryResult);
      }
    } catch (Exception e) {
      metrics.getSwallowedErrorCount.inc();
      LOGGER.error("Error from secondarySource.get({})", instanceName, e);
    }
    return primaryResult;
  }

  /**
   * Long running background task to periodically check for consistency between two listeners.
   */
  private static class ConsistencyChecker implements Runnable {
    private final Time time;
    private final DataNodeConfigSourceMetrics metrics;
    private final ConfigChangeRecorder primaryListener;
    private final ConfigChangeRecorder secondaryListener;

    /**
     * @param time {@link Time} instance.
     * @param metrics {@link DataNodeConfigSourceMetrics} instance.
     * @param primaryListener the first {@link ConfigChangeRecorder} to compare.
     * @param secondaryListener the second {@link ConfigChangeRecorder} to compare.
     */
    public ConsistencyChecker(Time time, DataNodeConfigSourceMetrics metrics, ConfigChangeRecorder primaryListener,
        ConfigChangeRecorder secondaryListener) {
      this.time = time;
      this.metrics = metrics;
      this.primaryListener = primaryListener;
      this.secondaryListener = secondaryListener;
    }

    @Override
    public void run() {
      long lastSuccessTimeMs = time.milliseconds();
      while (!Thread.currentThread().isInterrupted()) {
        try {
          time.sleep(CHECK_PERIOD_MS);
        } catch (InterruptedException e) {
          LOGGER.warn("ConsistencyChecker sleep interrupted, shutting down");
          Thread.currentThread().interrupt();
          break;
        }
        long currentTimeMs = time.milliseconds();
        Map<String, DataNodeConfig> primaryConfigsSeen = primaryListener.configsSeen;
        Map<String, DataNodeConfig> secondaryConfigsSeen = secondaryListener.configsSeen;
        if (primaryConfigsSeen.equals(secondaryConfigsSeen)) {
          lastSuccessTimeMs = currentTimeMs;
          LOGGER.debug("Sources consistent at {}", currentTimeMs);
        } else if ((currentTimeMs - CHECK_PERIOD_MS * MAX_PERIODS_WITH_INCONSISTENCY) >= lastSuccessTimeMs) {
          metrics.listenerInconsistencyCount.inc();
          LOGGER.warn("Inconsistency detected for multiple periods. primary={}, secondary={}", primaryConfigsSeen,
              secondaryConfigsSeen);
        } else {
          metrics.listenerTransientInconsistencyCount.inc();
          LOGGER.debug("Inconsistency detected at {}, waiting to see if state converges. primary={}, secondary={}",
              currentTimeMs, primaryConfigsSeen, secondaryConfigsSeen);
        }
      }
    }
  }

  /**
   * A listener that keeps a map of all of the configs it has seen that can be used for checking consistency.
   */
  private static class ConfigChangeRecorder implements DataNodeConfigChangeListener {
    final Map<String, DataNodeConfig> configsSeen = new ConcurrentHashMap<>();
    private final DataNodeConfigChangeListener listener;

    /**
     * @param listener if non-null, this listener will be notified on any change.
     */
    public ConfigChangeRecorder(DataNodeConfigChangeListener listener) {
      this.listener = listener;
    }

    @Override
    public void onDataNodeConfigChange(Iterable<DataNodeConfig> configs) {
      if (listener != null) {
        listener.onDataNodeConfigChange(configs);
      }
      for (DataNodeConfig config : configs) {
        configsSeen.put(config.getInstanceName(), config);
      }
    }
  }
}
