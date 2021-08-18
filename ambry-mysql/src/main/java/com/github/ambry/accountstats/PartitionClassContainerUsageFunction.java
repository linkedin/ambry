/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.accountstats;

/**
 * A callback function to call when processing aggregated partition class container usage.
 */
@FunctionalInterface
public interface PartitionClassContainerUsageFunction {

  /**
   * Process aggregated partition class container usage.
   * @param partitionClassName The partition class name
   * @param accountId the account id
   * @param containerId the container id
   * @param storageUsage the storage usage
   * @param updatedAt the updated at time in milliseconds
   */
  void apply(String partitionClassName, short accountId, short containerId, long storageUsage, long updatedAt);
}
