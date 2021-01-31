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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.storage.QuotaOperation;
import java.util.Arrays;
import java.util.HashSet;


/**
 * An implementation of {@link QuotaSource} that always returns a max value for {@link Quota} for all resources.
 */
public class UnlimitedQuotaSource implements QuotaSource {

  @Override
  public Quota getRequestQuota(QuotaResource quotaResource, QuotaOperation quotaOperation, QuotaName quotaName) {
    return new Quota<>(quotaName, Long.MAX_VALUE, quotaResource, new HashSet<>(Arrays.asList(quotaOperation)));
  }

  @Override
  public Quota getHostQuota(QuotaResource quotaResource, QuotaName quotaName) {
    return new Quota<>(quotaName, Long.MAX_VALUE, quotaResource, null);
  }
}
