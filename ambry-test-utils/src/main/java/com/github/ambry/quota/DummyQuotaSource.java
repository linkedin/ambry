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

import java.util.Collection;


/**
 * Dummy {@link QuotaSource} implementation for test.
 */
public class DummyQuotaSource implements QuotaSource {

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    return null;
  }

  @Override
  public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
  }
}
