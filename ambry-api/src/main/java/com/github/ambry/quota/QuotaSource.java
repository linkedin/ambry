/*
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
import java.util.List;


/**
 * Interface representing the backend source from which quota for a resource can be fetched, and to which the current
 * usage of a resource can be saved.
 */
public interface QuotaSource {
  /**
   * Get the {@link Quota} for specified resource and operation.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaName {@link QuotaName} object.
   */
  Quota getQuota(QuotaResource quotaResource, QuotaName quotaName);

  /**
   * Update the quota for newly created {@link List} of {@link QuotaResource}s.
   * @param quotaResources {@link List} of new created {@link QuotaResource}s.
   */
  void updateNewQuotaResources(Collection<QuotaResource> quotaResources);
}
