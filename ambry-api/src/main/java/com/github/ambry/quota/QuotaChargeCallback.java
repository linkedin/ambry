/*
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
package com.github.ambry.quota;

/**
 * Callback for charging request cost against quota. Used by {@link QuotaEnforcer}s to charge quota for a request.
 */
public interface QuotaChargeCallback {

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request.
   * @param chunkSize of the chunk.
   * @throws QuotaException In case request needs to be throttled.
   */
  void charge(long chunkSize) throws QuotaException;

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request. Call this method
   * when the quota charge doesn't depend on the chunk size.
   * @throws QuotaException In case request needs to be throttled.
   */
  void charge() throws QuotaException;

  /**
   * Check if request should be throttled based on quota usage.
   * @return {@code true} if request usage exceeds limit and request should be throttled. {@code false} otherwise.
   */
  boolean check();

  /**
   * Check if usage is allowed to exceed the quota limit.
   * @return {@code true} if usage is allowed to exceed the quota limit. {@code false} otherwise.
   */
  boolean quotaExceedAllowed();

  /**
   * @return QuotaResource object.
   * @throws QuotaException in case of any errors.
   */
  QuotaResource getQuotaResource() throws QuotaException;

  /**
   * @return QuotaMethod object.
   */
  QuotaMethod getQuotaMethod();
}
