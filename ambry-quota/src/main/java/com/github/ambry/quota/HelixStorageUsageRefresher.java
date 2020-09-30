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
package com.github.ambry.quota;

import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.server.StatsSnapshot;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.zookeeper.data.Stat;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Helix implementation of {@link StorageUsageRefresher}. A Helix aggregation task, created in ambry-server, would persist
 * current storage usage in ZooKeeper periodically. This implementation would fetch the storage usage from zookeeper using
 * {@link HelixPropertyStore} and subscribe to the change. This implementation also periodically refetch the storage usage
 * from ZooKeeper to keep it up-to-date.
 */
public class HelixStorageUsageRefresher implements StorageUsageRefresher {
  // These two constant fields are shared by other classes. TODO: reuse these two constant fields.
  public static final String AGGREGATED_CONTAINER_STORAGE_USAGE_PATH = "Aggregated_AccountReport";
  public static final String VALID_SIZE_FILED_NAME = "valid_data_size";

  private static final Logger logger = LoggerFactory.getLogger(HelixStorageUsageRefresher.class);

  private final HelixPropertyStore<ZNRecord> helixStore;
  private final AtomicReference<Map<String, Map<String, Long>>> containerStorageUsageRef = new AtomicReference<>(null);
  private final AtomicReference<Listener> callback = new AtomicReference<>(null);
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;

  /**
   * Constructor to create a {@link HelixStorageUsageRefresher}.
   * @param helixStore The {@link HelixPropertyStore} used to fetch storage usage and subscribe to change.
   * @param scheduler The {@link ScheduledExecutorService} to schedule a task to periodically refresh the usage.
   * @param config The {@link StorageQuotaConfig}.
   */
  public HelixStorageUsageRefresher(HelixPropertyStore<ZNRecord> helixStore, ScheduledExecutorService scheduler,
      StorageQuotaConfig config) {
    this.helixStore = Objects.requireNonNull(helixStore, "HelixPropertyStore can't be null");
    this.scheduler = scheduler;
    this.config = config;
    subscribeToChange();
    initialFetchAndSchedule();
  }

  /**
   * Fetch the storage usage and schedule a task to periodically refresh the storage usage.
   */
  private void initialFetchAndSchedule() {
    Runnable updater = () -> {
      try {
        Map<String, Map<String, Long>> initialValue =
            fetchContainerStorageUsageFromPath(AGGREGATED_CONTAINER_STORAGE_USAGE_PATH);
        containerStorageUsageRef.set(initialValue);
      } catch (Exception e) {
        logger.error("Failed to parse the container usage from znode " + AGGREGATED_CONTAINER_STORAGE_USAGE_PATH, e);
        // If we already have a container usage map in memory, then don't replace it with empty map.
        containerStorageUsageRef.compareAndSet(null, Collections.EMPTY_MAP);
      }
    };
    updater.run();

    if (scheduler != null) {
      int initialDelay = new Random().nextInt(config.refresherPollingIntervalMs + 1);
      scheduler.scheduleAtFixedRate(updater, initialDelay, config.refresherPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info(
          "Background storage usage updater will fetch storage usage from remote starting {} ms from now and repeat with interval={} ms",
          initialDelay, config.refresherPollingIntervalMs);
    }
  }

  /**
   * Subscribe to storage usage change via helix property store. It will update the in memory cache of the storage usage
   * and invoke the callback if present.
   */
  private void subscribeToChange() {
    helixStore.subscribe(AGGREGATED_CONTAINER_STORAGE_USAGE_PATH, new HelixPropertyListener() {
      @Override
      public void onDataChange(String path) {
        refreshOnUpdate(path);
      }

      @Override
      public void onDataCreate(String path) {
        refreshOnUpdate(path);
      }

      @Override
      public void onDataDelete(String path) {
        // This is a no-op when a ZNRecord for aggregated storage usage is deleted.
        logger.warn("StorageUsage is unexpectedly deleted for at path {}", path);
      }
    });
  }

  @Override
  public Map<String, Map<String, Long>> getContainerStorageUsage() {
    return Collections.unmodifiableMap(containerStorageUsageRef.get());
  }

  @Override
  public void registerListener(Listener cb) {
    Objects.requireNonNull(cb, "Callback has to be non-null");
    if (!callback.compareAndSet(null, cb)) {
      throw new IllegalStateException("Callback already registered");
    }
  }

  /**
   * Fetch the container storage usage from the given ZooKeeper path and update the in memory cache. Invoke callback
   * if present.
   * @param path The ZooKeeper path to fetch the storage usage.
   */
  private void refreshOnUpdate(String path) {
    try {
      Map<String, Map<String, Long>> storageUsage = fetchContainerStorageUsageFromPath(path);
      containerStorageUsageRef.set(storageUsage);
      if (callback.get() != null) {
        callback.get().onNewContainerStorageUsage(Collections.unmodifiableMap(storageUsage));
      }
    } catch (Exception e) {
      logger.error("Failed to refresh storage usage on update of path = {}", path, e);
      containerStorageUsageRef.compareAndSet(null, Collections.EMPTY_MAP);
    }
  }

  /**
   * Fetch container storage usage from the given ZooKeeper path.
   * @param path The ZooKeeper path to fetch storage usage.
   * @return The map representing container storage usage in bytes.
   * @throws IOException
   */
  private Map<String, Map<String, Long>> fetchContainerStorageUsageFromPath(String path) throws IOException {
    Stat stat = new Stat();
    ZNRecord znRecord = helixStore.get(AGGREGATED_CONTAINER_STORAGE_USAGE_PATH, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      logger.info("The ZNRecord to read does not exist on path={}", AGGREGATED_CONTAINER_STORAGE_USAGE_PATH);
      return Collections.EMPTY_MAP;
    }
    return parseContainerStorageUsageFromZNRecord(znRecord);
  }

  /**
   * Parse the container storage usage from given {@link ZNRecord}.
   * @param znRecord The {@link ZNRecord} that contains storage usage in json format.
   * @return The map representing container storage usage in bytes.
   * @throws IOException
   */
  private Map<String, Map<String, Long>> parseContainerStorageUsageFromZNRecord(ZNRecord znRecord) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    String serializedStr = znRecord.getSimpleField(VALID_SIZE_FILED_NAME);
    StatsSnapshot statsSnapshot =
        mapper.readValue(new ByteArrayInputStream(serializedStr.getBytes()), StatsSnapshot.class);
    Map<String, Map<String, Long>> accountStorageUsages = new HashMap<>();
    Map<String, StatsSnapshot> accountSubMap = statsSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> accountSubMapEntry : accountSubMap.entrySet()) {
      String accountKey = accountSubMapEntry.getKey();
      // AccountKey's format is A[accountId], use substring to get the real accountId
      String accountId = accountKey.substring(2, accountKey.length() - 1);
      Map<String, Long> containerUsages = new HashMap<>();
      accountStorageUsages.put(accountId, containerUsages);
      Map<String, StatsSnapshot> containerSubMap = accountSubMapEntry.getValue().getSubMap();
      for (Map.Entry<String, StatsSnapshot> containerSubMapEntry : containerSubMap.entrySet()) {
        String containerKey = containerSubMapEntry.getKey();
        // ContainerKey's format is C[containerId], use substring to get the real containerId
        String containerId = containerKey.substring(2, containerKey.length() - 1);
        containerUsages.put(containerId, containerSubMapEntry.getValue().getValue());
      }
    }
    return accountStorageUsages;
  }
}
