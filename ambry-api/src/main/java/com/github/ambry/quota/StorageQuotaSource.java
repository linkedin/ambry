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

import java.util.Map;


/**
 * The interface of the source of storage quota for each container.
 */
public interface StorageQuotaSource {
  /**
   * Return the storage quota of each container. The returned map should be structured as such:
   * The key of the map is the account id in string format and the value of the map is the storage quota of each
   * container under this account.
   * The container usage map's key is the container id in string format, and the value is storage quota in bytes of
   * this container.
   * @return The storage quota for each container.
   */
  Map<String, Map<String, Long>> getContainerQuota();

  /**
   * A listener interface registered with {@link StorageQuotaSource}. It will be invoked every time when there is a
   * change in the storage quota. The new storage quota will be passed as the parameter. Notice this is a unmodifiable
   * map.
   */
  interface Listener {
    void onNewContainerStorageQuota(Map<String, Map<String, Long>> containerStorageQuota);
  }

  /**
   * Register your listener to {@link StorageQuotaSource}. A source should only have one callback and it can't be
   * registered multiple times.
   * @param listener The listener to register.
   */
  void registerListener(Listener listener);
}
