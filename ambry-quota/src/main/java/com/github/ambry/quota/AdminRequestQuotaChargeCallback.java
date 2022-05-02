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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestRequest;
import java.util.Properties;


/**
 * A {@link QuotaChargeCallback} implementation for admin operations like RouterStore, that don't originate as part of
 * user requests, and hence dont have user quotas or {@link RestRequest} associated with them.
 */
public class AdminRequestQuotaChargeCallback implements QuotaChargeCallback {
  public static QuotaResource ADMIN_QUOTA_RESOURCE = new QuotaResource("ADMIN_RESOURCE", QuotaResourceType.ACCOUNT);
  private final boolean isReadRequest;

  public AdminRequestQuotaChargeCallback(boolean isReadRequest) {
    this.isReadRequest = isReadRequest;
  }

  @Override
  public QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge, long chunkSize) {
    return QuotaAction.ALLOW;
  }

  @Override
  public QuotaResource getQuotaResource() {
    return ADMIN_QUOTA_RESOURCE;
  }

  @Override
  public QuotaMethod getQuotaMethod() {
    return isReadRequest ? QuotaMethod.READ : QuotaMethod.WRITE;
  }

  @Override
  public QuotaConfig getQuotaConfig() {
    return new QuotaConfig(new VerifiableProperties(new Properties()));
  }
}
