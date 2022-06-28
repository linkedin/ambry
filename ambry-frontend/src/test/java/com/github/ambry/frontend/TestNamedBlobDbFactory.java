/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.utils.SystemTime;


public class TestNamedBlobDbFactory implements NamedBlobDbFactory {
  private final TestNamedBlobDb namedBlobDb;

  public TestNamedBlobDbFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      AccountService accountService) {
    InternalConfig config = new InternalConfig(verifiableProperties);
    namedBlobDb = new TestNamedBlobDb(SystemTime.getInstance(), config.listMaxResults);
  }

  @Override
  public NamedBlobDb getNamedBlobDb() throws Exception {
    return namedBlobDb;
  }

  class InternalConfig {
    public final int listMaxResults;

    public InternalConfig(VerifiableProperties verifiableProperties) {
      this.listMaxResults =
          verifiableProperties.getIntInRange(MySqlNamedBlobDbConfig.LIST_MAX_RESULTS, 100, 1, Integer.MAX_VALUE);
    }
  }
}
