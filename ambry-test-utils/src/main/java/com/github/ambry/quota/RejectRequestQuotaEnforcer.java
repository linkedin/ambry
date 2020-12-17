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
package com.github.ambry.quota;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;


public class RejectRequestQuotaEnforcer implements RequestQuotaEnforcer {
  private final QuotaSource quotaSource;

  public RejectRequestQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
  }

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo) {
    return new RejectEnforcementRecommendation(RejectRequestQuotaEnforcer.class.getSimpleName());
  }

  @Override
  public EnforcementRecommendation recommend(RestRequest restRequest) {
    return new RejectEnforcementRecommendation(RejectRequestQuotaEnforcer.class.getSimpleName());
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {
  }
}
