/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for managing compaction of a {@link BlobStore}.
 */
class CompactionManager {
  static final String THREAD_NAME_PREFIX = "StoreCompactionThread-";

  private final String mountPath;
  private final StoreConfig storeConfig;
  private final Time time;
  private final Collection<BlobStore> stores;
  private final CompactionExecutor compactionExecutor;
  private final StorageManagerMetrics metrics;
  private final CompactionPolicy compactionPolicy;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private Thread compactionThread;

  /**
   * Creates a CompactionManager that handles scheduling and executing compaction.
   * @param mountPath the mount path of all the stores for which compaction may be executed.
   * @param storeConfig the {@link StoreConfig} that contains configurationd details.
   * @param stores the {@link Collection} of {@link BlobStore} that compaction needs to be executed for.
   * @param metrics the {@link StorageManagerMetrics} to use.
   * @param time the {@link Time} instance to use.
   */
  CompactionManager(String mountPath, StoreConfig storeConfig, Collection<BlobStore> stores,
      StorageManagerMetrics metrics, Time time) {
    this.mountPath = mountPath;
    this.storeConfig = storeConfig;
    this.stores = stores;
    this.time = time;
    this.metrics = metrics;
    compactionExecutor = storeConfig.storeEnableCompaction ? new CompactionExecutor() : null;
    try {
      CompactionPolicyFactory compactionPolicyFactory =
          Utils.getObj(storeConfig.storeCompactionPolicyFactory, storeConfig, time);
      compactionPolicy = compactionPolicyFactory.getCompactionPolicy();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Error creating compaction policy using compactionPolicyFactory " + storeConfig.storeCompactionPolicyFactory);
    }
  }

  /**
   * Enables the compaction manager allowing it execute compactions if required.
   */
  void enable() {
    if (compactionExecutor != null) {
      logger.info("Compaction thread started for {}", mountPath);
      compactionThread = Utils.newThread(THREAD_NAME_PREFIX + mountPath, compactionExecutor, true);
      compactionThread.start();
    }
  }

  /**
   * Disables the compaction manager which disallows any new compactions.
   */
  void disable() {
    if (compactionExecutor != null) {
      compactionExecutor.disable();
    }
  }

  /**
   * Awaits the termination of all pending jobs of the compaction manager.
   */
  void awaitTermination() {
    if (compactionExecutor != null && compactionThread != null) {
      try {
        compactionThread.join(2000);
      } catch (InterruptedException e) {
        metrics.compactionManagerTerminateErrorCount.inc();
        logger.error("Compaction thread join wait for {} was interrupted", mountPath);
      }
    }
  }

  /**
   * @return {@code true} if the compaction thread is running. {@code false} otherwise.
   */
  boolean isCompactionExecutorRunning() {
    return compactionExecutor != null && compactionExecutor.isRunning;
  }

  /**
   * Get compaction details for a given {@link BlobStore} if any
   * @param blobStore the {@link BlobStore} for which compation details are requested
   * @return the {@link CompactionDetails} containing the details about log segments that needs to be compacted.
   * {@code null} if compaction is not required
   * @throws StoreException when {@link BlobStore} is not started
   */
  CompactionDetails getCompactionDetails(BlobStore blobStore) throws StoreException {
    return blobStore.getCompactionDetails(compactionPolicy);
  }

  /**
   * A {@link Runnable} that cycles through the stores and executes compaction if required.
   */
  private class CompactionExecutor implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitCondition = lock.newCondition();
    private final Set<BlobStore> storesToSkip = new HashSet<>();
    private final long waitTimeMs = storeConfig.storeCompactionCheckFrequencyInHours * Time.SecsPerHour * Time.MsPerSec;

    private volatile boolean enabled = true;

    volatile boolean isRunning = false;

    /**
     * Starts by resuming any compactions that were left halfway. In steady state, it cycles through the stores at a
     * configurable frequency and runs compaction as required.
     */
    @Override
    public void run() {
      isRunning = true;
      try {
        logger.info("Starting compaction thread for {}", mountPath);
        // complete any compactions in progress
        for (BlobStore store : stores) {
          logger.trace("{} being checked for resume", store);
          if (store.isStarted()) {
            logger.trace("{} is started and eligible for resume check", store);
            metrics.markCompactionStart(false);
            try {
              store.maybeResumeCompaction();
            } catch (Exception e) {
              metrics.compactionErrorCount.inc();
              logger.error("Compaction of store {} failed on resume. Continuing with the next store", store, e);
              storesToSkip.add(store);
            } finally {
              metrics.markCompactionStop();
            }
          }
        }
        // continue to do compactions as required.
        while (enabled) {
          try {
            long startTimeMs = time.milliseconds();
            for (BlobStore store : stores) {
              logger.trace("{} being checked for compaction", store);
              try {
                if (!enabled) {
                  logger.trace("Breaking out because compaction thread is disabled for {}", mountPath);
                  break;
                }
                if (store.isStarted() && !storesToSkip.contains(store)) {
                  logger.trace("{} is started and eligible for compaction check", store);
                  CompactionDetails details = getCompactionDetails(store);
                  if (details != null) {
                    logger.trace("Generated {} as details for {}", details, store);
                    metrics.markCompactionStart(true);
                    store.compact(details);
                    metrics.markCompactionStop();
                  }
                }
              } catch (Exception e) {
                metrics.compactionErrorCount.inc();
                logger.error("Compaction of store {} failed. Continuing with the next store", store, e);
                storesToSkip.add(store);
              }
            }
            lock.lock();
            try {
              if (enabled) {
                long timeElapsed = time.milliseconds() - startTimeMs;
                logger.trace("Going to wait for {} ms in compaction thread at {}", waitTimeMs - timeElapsed, mountPath);
                time.await(waitCondition, waitTimeMs - timeElapsed);
              }
            } finally {
              lock.unlock();
            }
          } catch (Exception e) {
            metrics.compactionExecutorErrorCount.inc();
            logger.error("Compaction executor for {} encountered an error. Continuing", mountPath, e);
          }
        }
      } finally {
        isRunning = false;
        logger.info("Stopping compaction thread for {}", mountPath);
      }
    }

    /**
     * Disables the executor by disallowing scheduling of any new compaction jobs.
     */
    void disable() {
      lock.lock();
      try {
        enabled = false;
        waitCondition.signal();
      } finally {
        lock.unlock();
      }
      logger.info("Disabled compaction thread for {}", mountPath);
    }
  }
}

