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

import com.github.ambry.quota.storage.QuotaOperation;
import java.util.Set;

/**
 * The quota for a particular Ambry resource.
 * @param <T> the type of the quota value.
 */
public class Quota<T> {
  private final QuotaMetric quotaMetric;
  private final T quotaValue;
  private final QuotaResource quotaResource;
  private final Set<QuotaOperation> quotaOperations;

  /**
   * Constructor for {@link Quota}.
   * @param quotaMetric {@link QuotaMetric} object.
   * @param quotaValue value of the quota limit.
   * @param quotaResource {@link QuotaResource} for which quota is specified.
   * @param quotaOperations {@link Set} of {@link QuotaOperation}s.
   */
  public Quota(QuotaMetric quotaMetric, T quotaValue, QuotaResource quotaResource,
      Set<QuotaOperation> quotaOperations) {
    this.quotaMetric = quotaMetric;
    this.quotaValue = quotaValue;
    this.quotaResource = quotaResource;
    this.quotaOperations = quotaOperations;
  }

  /**
   * @return QuotaMetric object.
   */
  public QuotaMetric getQuotaMetric() {
    return quotaMetric;
  }

  /**
   * @return Value of the quota.
   */
  public T getQuotaValue() {
    return quotaValue;
  }

  /**
   * @return QuotaResource object.
   */
  public QuotaResource getQuotaResource() {
    return quotaResource;
  }

  /**
   * @return Set of {@link QuotaOperation}s
   */
  public Set<QuotaOperation> getQuotaOperations() {
    return quotaOperations;
  }
}
