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
package com.github.ambry.quota.storage;

import java.util.Map;


/**
 * An interface to initialize the storage usage of each container and save them in memory. It should also refresh
 * the in memory cache if there is any change of storage usage.
 */
public interface StorageUsageRefresher {

  /**
   * Return the current storage usage for each container. The returned map should be structured as such:
   * The key of the map is the account id in string format and the value of the map is the storage usage of each
   * container under this account.
   * The container usage map's key is the container is in string format, and the value is storage usage of this container.
   * @return The current storage usage for each container.
   * @throws Exception
   */
  Map<String, Map<String, Long>> getContainerStorageUsage() throws Exception;

  /**
   * A listener interface registered with {@link StorageUsageRefresher}. It will be invoked every time when there is a
   * change in the storage usage. The new storage usage will be passed as the parameter. Notice this is a unmodifiable
   * map.
   */
  interface Listener {
    void onNewContainerStorageUsage(Map<String, Map<String, Long>> containerStorageUsage);
  }

  /**
   * Register your listener to {@link StorageUsageRefresher}. A refresher should only have one callback and it can't be
   * registered multiple times.
   * @param listener The listener to register.
   */
  void registerListener(Listener listener);
}
