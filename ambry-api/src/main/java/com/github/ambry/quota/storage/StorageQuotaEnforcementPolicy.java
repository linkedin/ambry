/**
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
package com.github.ambry.quota.storage;

/**
 * The interface to decide whether to throttle a request based on the storage quota and usage.
 */
public interface StorageQuotaEnforcementPolicy {
  /**
   * Return true if the request should be throttled, otherwise, return false.
   * @param storageQuota The storage quota.
   * @param currentUsage The current usage.
   * @return
   */
  boolean shouldThrottleRequest(long storageQuota, long currentUsage);
}
