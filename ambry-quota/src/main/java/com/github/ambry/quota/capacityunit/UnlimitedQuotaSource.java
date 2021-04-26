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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * An implementation of {@link QuotaSource} that always returns a max value for {@link Quota} for all resources.
 */
public class UnlimitedQuotaSource implements QuotaSource {
  private final Set<QuotaResource> quotaResourceList = new HashSet<>();

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    return new Quota<>(quotaName, Long.MAX_VALUE, quotaResource);
  }

  @Override
  public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
    quotaResourceList.addAll(quotaResources);
  }

  /**
   * @return List of {@link QuotaResource}s updated to the {@link QuotaSource}.
   */
  public Set<QuotaResource> getQuotaResourceList() {
    return quotaResourceList;
  }
}
