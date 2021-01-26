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

/**
 * {@link HostQuotaEnforcer} implementation for test that always rejects requests.
 */
public class RejectHostQuotaEnforcer implements HostQuotaEnforcer {
  private static final float DUMMY_REJECTABLE_USAGE_PERCENTAGE = 101;
  private static final int REJECT_HTTP_STATUS = 429;
  private static final boolean SHOULD_THROTTLE = true;

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation recommend() {
    return new AmbryEnforcementRecommendation(SHOULD_THROTTLE, DUMMY_REJECTABLE_USAGE_PERCENTAGE,
        RejectRequestQuotaEnforcer.class.getSimpleName(), REJECT_HTTP_STATUS, null);
  }

  @Override
  public void shutdown() {
  }
}
