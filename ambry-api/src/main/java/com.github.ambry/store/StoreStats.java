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
package com.github.ambry.store;

import com.github.ambry.utils.Pair;


/**
 * Exposes important stats related to a {@link Store}.
 */
interface StoreStats {

  /**
   * Gets the total size of valid data of the store. Any blob that is not deleted nor expired are considered to be valid
   * @return the valid data size of the {@link Store} at a specific point in time
   */
  public Pair<Long, Long> getValidDataSize();

  /**
   * Gets the current used capacity of the {@link Store}. Total bytes that are not available for new content are
   * considered to be used.
   * @return the used data size of the {@link Store}
   */
  public long getUsedCapacity();

  /**
   * Gets the total capacity of the {@link Store}. This represents the total disk capacity occupied by the {@link Store}
   * @return the total capacity of the {@link Store}.
   */
  public long getTotalCapacity();
}
