/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;


public class AccountServiceMetricsWrapper extends AccountServiceMetrics {
  private final AccountServiceMetrics accountServiceMetricsNew;

  public AccountServiceMetricsWrapper(MetricRegistry metricRegistry) {
    super(metricRegistry, false);
    this.accountServiceMetricsNew = new AccountServiceMetrics(metricRegistry, true);
  }

  @Override
  void trackAccountDataInconsistency(CompositeAccountService compositeAccountService) {
    super.trackAccountDataInconsistency(compositeAccountService);
  }

  @Override
  void trackTimeSinceLastSync(Gauge<Integer> gauge, Gauge<Integer> gauge1) {
    super.trackTimeSinceLastSync(gauge, gauge1);
  }

  @Override
  void trackContainerCount(Gauge<Integer> gauge, Gauge<Integer> gauge1) {
    super.trackContainerCount(gauge, gauge1);
  }

  public AccountServiceMetrics getAccountServiceMetrics() {return this;}

  public AccountServiceMetrics getAccountServiceMetricsNew() {return this.accountServiceMetricsNew;}
}
