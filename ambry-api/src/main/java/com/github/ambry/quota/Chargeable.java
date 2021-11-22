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
package com.github.ambry.quota;

/**
 * A {@link Chargeable} is a operation that can be charged against quota.
 */
public interface Chargeable {

  /**
   * Charge the cost of the operation against relevant quota if usage is within quota.
   *
   * @return {@code true} if usage is within quota and quota was charged. {@code false} otherwise.
   */
  boolean checkAndCharge();

  /**
   * Check if its allowed for usage to exceed quota.
   *
   * @return {@code true} if usage is allowed to exceed quota. {@code false} otherwise.
   */
  boolean quotaExceedAllowed();

  /**
   * @return the {@link QuotaResource} whose operation is being charged.
   */
  QuotaResource getQuotaResource();
}