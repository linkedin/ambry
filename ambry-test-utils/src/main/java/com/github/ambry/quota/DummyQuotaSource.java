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

import com.github.ambry.account.Account;
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
  public void updateNewQuotaResources(Collection<Account> accounts) {
  }

  @Override
  public void init() throws QuotaException {

  }

  @Override
  public boolean isReady() {
    return false;
  }

  @Override
  public float getUsage(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    return 0;
  }

  @Override
  public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) throws QuotaException {

  }

  @Override
  public float getSystemResourceUsage(QuotaName quotaName) throws QuotaException {
    return 0;
  }

  @Override
  public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) throws QuotaException {

  }

  @Override
  public void shutdown() {

  }
}
