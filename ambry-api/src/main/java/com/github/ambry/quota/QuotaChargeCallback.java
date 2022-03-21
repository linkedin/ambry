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

import com.github.ambry.config.QuotaConfig;


/**
 * Callback for performing quota compliance operations, as each chunk of a user request is processed. The main job of
 * this callback is to translate quota compliance operations done for each chunk, into quota compliance operations
 * against the {@link QuotaResource} of the corresponding user request. Used by {@link Chargeable} implementations.
 *
 * Uses {@link QuotaManager} implementation for performing the quota operations. Hence the thread safety and atomicity
 * guarantees are same as that of the {@link QuotaManager} implementation used.
 * Uses {@link RequestQuotaCostPolicy} implementation to calculate the cost of the chunk request.
 */
public interface QuotaChargeCallback {

  /**
   * Charges the cost of chunk request against quota, if checks to allow a chunk request are successful. The cost to
   * charge is determined by the specified chunkSize, with help from {@link RequestQuotaCostPolicy} implementations.
   *
   * Checks to allow a request are:
   * 1. If usage is within quota, OR
   * 2. If shouldCheckExceedAllowed is set as {@code true}, and the implementation determines that quota is allowed to
   * exceed usage. For examples about when usage could be allowed to exceed quota, check {@link QuotaEnforcer}.
   *
   * If forceCharge is specified as {@code true}, then the charge must happen. In that case the implementations can skip
   * any checks for allowing the request, as those checks are inconsequential.
   *
   * @param shouldCheckExceedAllowed if set to {@code true}, check if usage is allowed to exceed quota.
   * @param forceCharge if set to {@code true}, then charge must happen. Implementation may decide to skip any checks as
   *                    they are inconsequential.
   * @param chunkSize of the chunk.
   * @throws QuotaException In case of any exception.
   * @return QuotaAction representing the recommended action to take for quota compliance.
   */
  QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge, long chunkSize) throws QuotaException;

  /**
   * Charges the cost of chunk request against quota, if checks to allow a chunk request are successful. The cost to
   * charge is one unit of quota cost, as determined by {@link QuotaConfig#quotaAccountingUnit}.
   * For more details check {@link QuotaChargeCallback#checkAndCharge(boolean, boolean, long)}.
   *
   * @param shouldCheckExceedAllowed if set to {@code true}, check if usage is allowed to exceed quota.
   * @param forceCharge if set to {@code true}, then charge must happen. Implementation may decide to skip any checks as
   *                    they are inconsequential.
   * @throws QuotaException In case of any exception.
   * @return QuotaAction representing the recommended action to take for quota compliance.
   */
  default QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge) throws QuotaException {
    return checkAndCharge(shouldCheckExceedAllowed, forceCharge, getQuotaConfig().quotaAccountingUnit);
  }

  /**
   * @return QuotaResource object.
   * @throws QuotaException in case of any errors.
   */
  QuotaResource getQuotaResource() throws QuotaException;

  /**
   * @return QuotaMethod object.
   */
  QuotaMethod getQuotaMethod();

  /**
   * @return QuotaConfig object.
   */
  QuotaConfig getQuotaConfig();
}
