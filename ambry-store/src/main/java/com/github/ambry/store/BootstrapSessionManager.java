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
import java.util.HashMap;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the bootstrap sessions on the disk coming for different partitions from different bootstrapping nodes.
 * It allows adding, removing, and retrieving bootstrap sessions, as well as enabling and disabling the manager.
 */
public class BootstrapSessionManager {

  // Map structure - { hostname : { partitionId : BootstrapSession } }
  private final HashMap<String, HashMap<Long, BootstrapSession>> bootstrapSessionsMap = new HashMap<>();

  /**
   * Configuration for the disk manager.
   */
  private final DiskManagerConfig diskManagerConfig;

  /**
   * Flag to indicate if the manager is running or not.
   * If it is not running, all operations will throw an exception.
   */
  private boolean isRunning = false;

  /**
   * Function to control compaction for the blob store.
   * It takes a PartitionId and a boolean indicating whether to enable or disable compaction.
   * Returns true if the operation is successful, false otherwise.
   */
  private BiFunction<PartitionId, Boolean, Boolean> controlCompactionForBlobStore;

  private static final Logger logger = LoggerFactory.getLogger(BootstrapSessionManager.class);

  /**
   * Constructor for BootstrapSessionManager.
   * @param controlCompactionForBlobStore Function to control compaction for the blob store.
   */
  public BootstrapSessionManager(@Nonnull DiskManagerConfig diskManagerConfig,
      @Nonnull BiFunction<PartitionId, Boolean, Boolean> controlCompactionForBlobStore) {
    Objects.requireNonNull(diskManagerConfig, "diskManagerConfig cannot be null");
    Objects.requireNonNull(controlCompactionForBlobStore, "controlCompactionForBlobStore cannot be null");

    this.diskManagerConfig = diskManagerConfig;
    this.controlCompactionForBlobStore = controlCompactionForBlobStore;
  }

  /**
   * Adds a new bootstrap session and starts it.
   * @param partitionId The partition ID for the session.
   * @param snapShotId The snapshot ID for the session.
   * @param bootstrappingNodeId The bootstrapping node ID for the session.
   */
  public void addAndStartBootstrapSession(@Nonnull PartitionId partitionId, @Nonnull String snapShotId,
      @Nonnull String bootstrappingNodeId) {
    Objects.requireNonNull(partitionId, "partitionId cannot be null");
    Objects.requireNonNull(snapShotId, "snapShotId cannot be null");
    Objects.requireNonNull(bootstrappingNodeId, "bootstrappingNodeId cannot be null");
    validateBootstrapSessionManagerIsRunningOrThrow();

    boolean isSucceeded = controlCompactionForBlobStore.apply(partitionId, false);
    if (!isSucceeded) {
      logger.error("Failed to disable compaction for partition: " + partitionId);
      throw new IllegalStateException("Failed to disable compaction for partition: " + partitionId);
    }
    BootstrapSession session = new BootstrapSession(partitionId, snapShotId, bootstrappingNodeId,
        diskManagerConfig, enableCompactionOnTimerExpiryHandler, enableCompactionOnTimerExpiryHandler);
    bootstrapSessionsMap
      .computeIfAbsent(bootstrappingNodeId, k -> new HashMap<>())
      .put(partitionId.getId(), session);
    session.start();
  }

  /**
   * Removes the bootstrap session for a given partition ID and bootstrapping node ID.
   * @param partitionId The partition ID for the session.
   * @param bootstrappingNodeId The bootstrapping node ID for the session.
   */
  public void removeBootstrapSession(@Nonnull PartitionId partitionId, @Nonnull String bootstrappingNodeId) {
    Objects.requireNonNull(partitionId, "partitionId cannot be null");
    Objects.requireNonNull(bootstrappingNodeId, "bootstrappingNodeId cannot be null");
    validateBootstrapSessionManagerIsRunningOrThrow();

    BootstrapSession session = getBootstrapSession(partitionId, bootstrappingNodeId);
    if (session != null) {
      session.stop();
      bootstrapSessionsMap.get(bootstrappingNodeId).remove(partitionId.getId());
    }
  }

  /**
   * Gets the bootstrap session for a given partition ID and bootstrapping node ID.
   * @param partitionId The partition ID for the session.
   * @param bootstrappingNodeId The bootstrapping node ID for the session.
   * returns the bootstrap session if it exists, null otherwise.
   */
  public BootstrapSession getBootstrapSession(@Nonnull PartitionId partitionId, @Nonnull String bootstrappingNodeId) {
    Objects.requireNonNull(partitionId, "partitionId cannot be null");
    Objects.requireNonNull(bootstrappingNodeId, "bootstrappingNodeId cannot be null");
    validateBootstrapSessionManagerIsRunningOrThrow();

    return bootstrapSessionsMap.getOrDefault(bootstrappingNodeId, new HashMap<>())
        .getOrDefault(partitionId.getId(), null);
  }

  /**
   * Enables the BootstrapSessionManager.
   * This method should be called before any other operations can be performed.
   * Idempotent operation as re-enabling the manager if it is already running has no effect.
   */
  public void enable() {
    if (!isRunning) {
      isRunning = true;
      logger.info("BootstrapSessionManager is enabled.");
    } else {
      logger.warn("BootstrapSessionManager is already enabled.");
    }
  }

  /**
   * Disables the BootstrapSessionManager.
   * This method should be called when the manager is no longer needed.
   * Idempotent operation as re-disabling the manager if it is already disabled has no effect.
   */
  public void disable() {
    if (isRunning) {
      isRunning = false;
      clearAllSessions();
      logger.info("BootstrapSessionManager is disabled.");
    } else {
      logger.warn("BootstrapSessionManager is already disabled.");
    }
  }

  /**
   * Clears all bootstrap sessions.
   * This method stops all sessions and clears the map.
   */
  public void clearAllSessions() {
    for (HashMap<Long, BootstrapSession> sessions : bootstrapSessionsMap.values()) {
      for (BootstrapSession session : sessions.values()) {
        session.stop();
      }
    }
    bootstrapSessionsMap.clear();
  }

  /**
   * Handler to be called when the timer expires for enabling compaction.
   * It takes the bootstrapping node ID and the partition ID as parameters.
   */
  private final BiConsumer<String, PartitionId> enableCompactionOnTimerExpiryHandler = (bootstrappingNodeId, partitionId) -> {
    logger.info("Enabling compaction for partition: " + partitionId);
    boolean isSucceeded = controlCompactionForBlobStore.apply(partitionId, true);
    if (!isSucceeded) {
      logger.error("Failed to enable compaction for partition: " + partitionId);
      throw new IllegalStateException("Failed to enable compaction for partition: " + partitionId);
    }

    // Remove the session from the map
    // if bootstrappingNodeId has no sessions, remove the bootstrappingNodeId key from the map
    logger.info("Removing bootstrap session for bootstrappingNodeId: " + bootstrappingNodeId + ", partitionId: " + partitionId);

    bootstrapSessionsMap.computeIfPresent(bootstrappingNodeId, (k, v) -> {
      v.remove(partitionId.getId());
      return v.isEmpty() ? null : v;
    });
  };

  /**
   * Checks if the BootstrapSessionManager is running.
   * @return true if the manager is running, false otherwise.
   */
  private void validateBootstrapSessionManagerIsRunningOrThrow() {
    if (!isRunning) {
      logger.error("BootstrapSessionManager is not running.");
      throw new IllegalStateException("BootstrapSessionManager is not running.");
    }
  }
}
