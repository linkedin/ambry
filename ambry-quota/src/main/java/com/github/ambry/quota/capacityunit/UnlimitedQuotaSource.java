/**
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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaMetric;
import com.github.ambry.quota.storage.QuotaOperation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import java.util.Arrays;
import java.util.HashSet;


public class UnlimitedQuotaSource implements QuotaSource {

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaOperation quotaOperation, QuotaMetric quotaMetric) {
    return new Quota<>(quotaMetric, Long.MAX_VALUE, quotaResource,
        new HashSet<>(Arrays.asList(quotaOperation)));
  }
}
