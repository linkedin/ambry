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

import com.github.ambry.config.RouterConfig;


/**
 * The {@link Chargeable} interface provides methods to charge against quota for operations.
 * Implementations of this interface should ensure that when multiple parallel requests are sent for the same chunk
 * (due to configs like {@link RouterConfig#routerGetRequestParallelism}), then each chunk is charged only once.
 * If any quota compliance operation fails due to {@link QuotaException}, implementations should factor in
 * {@link QuotaException#isRetryable()} to decide if the quota operation should be tried again.
 *
 * In order to do quota compliance operations, the implementations will typically use {@link QuotaChargeCallback}.
 */
public interface Chargeable {

  /**
   * Charges the cost for this operation against quota of the quota resource of this operation, if checks to allow the
   * request are successful. See {@link QuotaChargeCallback#checkAndCharge} for an explanation of various ways in which
   * quota checks can be successful.
   * Cost is calculated by the implementations, typically based on the size and type of the request. For example, for
   * put and get requests, cost depends on the size of data. For delete request, there is a fixed cost.
   *
   * @param shouldCheckExceedAllowed if {@code true} then it should be checked if usage is allowed to exceed quota.
   * @return QuotaAction representing the recommended action to take.
   */
  QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed);

  /**
   * @return the {@link QuotaResource} whose operation is being charged.
   */
  QuotaResource getQuotaResource();

  /**
   * @return the {@link QuotaMethod} of the request for which quota is being charged.
   */
  QuotaMethod getQuotaMethod();
}
