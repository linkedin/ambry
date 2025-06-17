/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.named.NamedBlobDb;
import java.util.Collections;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class NamedBlobMysqlDatabasePerfTest {
  @Test
  public void testTestTypeConstructor() throws Exception {
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAllAccounts()).thenReturn(Collections.emptyList());
    NamedBlobDb db = mock(NamedBlobDb.class);
    Properties props = new Properties();
    props.setProperty("db.datacenter", "datacenter");
    props.setProperty("db.host", "host");
    props.setProperty("db.name", "name");
    props.setProperty("db.user.name", "name");
    props.setProperty("parallelism", "10");
    props.setProperty("num.operations", "10");
    NamedBlobMysqlDatabasePerf.PerfConfig perfConfig =
        new NamedBlobMysqlDatabasePerf.PerfConfig(new VerifiableProperties(props));

    NamedBlobMysqlDatabasePerf.PerformanceTestWorker worker =
        (NamedBlobMysqlDatabasePerf.PerformanceTestWorker) NamedBlobMysqlDatabasePerf.TestType.LIST.getWorkerClass()
            .getConstructor(int.class, NamedBlobDb.class, AccountService.class, int.class,
                NamedBlobMysqlDatabasePerf.PerfConfig.class)
            .newInstance(0, db, accountService, 10, perfConfig);
    Assert.assertNotNull(worker);
  }
}
