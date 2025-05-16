/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.utils.ExtendableTimer;
import java.util.Objects;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a bootstrap session for a partition.
 * It manages timers for deferral and total time since compaction was disabled.
 */
public class BootstrapSession {

  /**
   * The ID of the snapshot being bootstrapped.
   */
  private final String snapShotId;

  /**
   * The ID of the node that is bootstrapping the partition.
   * Typically hostName will be sent from the bootstrapping node.
   */
  private final String bootstrappingNodeId;

  /**
   * The ID of the partition being bootstrapped.
   */
  private final PartitionId partitionId;

  /**
   * Timer for deferring the compaction.
   * It will expire after a certain time and call the onTimerExpiryHandler.
   * Extendable to allow for extending the timer.
   */
  private final ExtendableTimer deferralTimer;

  /**
   * Timer for tracking the total time since compaction was disabled.
   * It will expire after a certain time and call the onTimerExpiryHandler.
   * Not extendable.
   */
  private final ExtendableTimer totalTimerSinceCompactionDisabled;

  /**
   * Configuration for the disk manager.
   */
  private final DiskManagerConfig diskManagerConfig;

  private static final Logger logger = LoggerFactory.getLogger(BootstrapSession.class);

  /**
   * Constructor for BootstrapSession.
   *
   * @param partitionId          The ID of the partition being bootstrapped.
   * @param snapShotId           The ID of the snapshot being bootstrapped.
   * @param bootstrappingNodeId  The ID of the node that is bootstrapping the partition.
   * @param diskManagerConfig     The configuration for the disk manager.
   * @param onDefaultTimerExpiryHandler The handler to be called when the default timer expires.
   * @param onTotalTimerExpiryHandler The handler to be called when the total timer expires.
   */
  public BootstrapSession(@Nonnull PartitionId partitionId, @Nonnull String snapShotId, @Nonnull String bootstrappingNodeId,
      @Nonnull DiskManagerConfig diskManagerConfig, @Nonnull BiConsumer<String, PartitionId> onDefaultTimerExpiryHandler,
      @Nonnull BiConsumer<String, PartitionId> onTotalTimerExpiryHandler) {
    Objects.requireNonNull(partitionId, "partitionId cannot be null");
    Objects.requireNonNull(snapShotId, "snapShotId cannot be null");
    Objects.requireNonNull(bootstrappingNodeId, "bootstrappingNodeId cannot be null");
    Objects.requireNonNull(diskManagerConfig, "diskManagerConfig cannot be null");
    Objects.requireNonNull(onDefaultTimerExpiryHandler, "onDefaultTimerExpiryHandler cannot be null");
    Objects.requireNonNull(onTotalTimerExpiryHandler, "onTotalTimerExpiryHandler cannot be null");

    this.snapShotId = snapShotId;
    this.bootstrappingNodeId = bootstrappingNodeId;
    this.partitionId = partitionId;
    this.diskManagerConfig = diskManagerConfig;

    this.deferralTimer = new ExtendableTimer(diskManagerConfig.diskManagerDeferredCompactionDefaultTimerTimeoutMilliseconds, () -> {
      logger.info("Deferral timer expired for snapshot: " + snapShotId);
      onDefaultTimerExpiryHandler.accept(bootstrappingNodeId, partitionId);

      logger.info("Deferral timer expired for snapshot: " + snapShotId + ", stopping the timer");
      stop();
    });

    this.totalTimerSinceCompactionDisabled = new ExtendableTimer(diskManagerConfig.diskManagerDeferredCompactionTotalTimerTimeoutMilliseconds, () -> {
      logger.info("TotalTimerSinceCompactionDisabled timer expired for snapshot: " + snapShotId);
      onTotalTimerExpiryHandler.accept(bootstrappingNodeId, partitionId);

      logger.info("TotalTimerSinceCompactionDisabled timer expired for snapshot: " + snapShotId + ", stopping the timer");
      stop();
    });
  }

  /**
   * Starts the timers.
   * The deferral timer and the total-timer-since-compaction-was-disabled timers are started.
   */
  void start() {
    deferralTimer.start();
    totalTimerSinceCompactionDisabled.start();
  }

  /**
   * Stops the timers.
   * The deferral timer and the total-timer-since-compaction-was-disabled timers are stopped.
   */
  void stop() {
    deferralTimer.cancel();
    totalTimerSinceCompactionDisabled.cancel();
  }

  /**
   * Extends the deferral timer by a certain number of milliseconds.
   * Only the default deferral timer can be extended.
   * @param timeInMs The time in milliseconds to extend the deferral timer.
   */
  public void extendDeferralTimer(long timeInMs) {
    deferralTimer.extend(timeInMs);
  }

  /**
   * Extends the deferral timer to the default timeout.
   * This is used when the deferral timer is extended to the default timeout.
   */
  public void extendDeferralTimer() {
    deferralTimer.extend(diskManagerConfig.diskManagerDeferredCompactionDefaultTimerTimeoutMilliseconds);
  }

  /**
   * Gets the ID of the snapshot being bootstrapped.
   * @return The ID of the snapshot being bootstrapped.
   */
  String getSnapShotId() {
    return snapShotId;
  }

  /**
   * Gets the ID of the node that is bootstrapping the partition.
   * @return The ID of the node that is bootstrapping the partition.
   */
  String getBootstrappingNodeId() {
    return bootstrappingNodeId;
  }

  /**
   * Gets the ID of the partition being bootstrapped.
   * @return The ID of the partition being bootstrapped.
   */
  PartitionId getPartitionId() {
    return partitionId;
  }
}
