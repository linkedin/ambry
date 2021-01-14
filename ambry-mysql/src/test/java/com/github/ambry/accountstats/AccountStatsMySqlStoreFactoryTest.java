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
package com.github.ambry.accountstats;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.github.ambry.mysql.MySqlUtils.DbEndpoint;


public class AccountStatsMySqlStoreFactoryTest {

  @Test
  public void testSetRewriteBatchedStatements() {
    List<DbEndpoint> endpoints =
        Arrays.asList(new DbEndpoint("jdbc:mysql://localhost/ambry_test_db", "dc1", false, "user", "password"),
            new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC", "dc1", true, "user2",
                "password2"),
            new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&characterEncoding=UTF-8", "dc1",
                true, "user3", "password3"),
            new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&rewriteBatchedStatements=true",
                "dc1", true, "user4", "password4"),
            new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&rewriteBatchedStatements=false",
                "dc1", true, "user5", "password5"),
            new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?rewriteBatchedStatements=false", "dc1", true, "user6",
                "password6"));

    List<DbEndpoint> expectedEndpoints = Arrays.asList(
        new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?rewriteBatchedStatements=true", "dc1", false, "user",
            "password"),
        new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&rewriteBatchedStatements=true", "dc1",
            true, "user2", "password2"), new DbEndpoint(
            "jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&characterEncoding=UTF-8&rewriteBatchedStatements=true",
            "dc1", true, "user3", "password3"),
        new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&rewriteBatchedStatements=true", "dc1",
            true, "user4", "password4"),
        new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?serverTimeZone=UTC&rewriteBatchedStatements=false", "dc1",
            true, "user5", "password5"),
        new DbEndpoint("jdbc:mysql://localhost/ambry_test_db?rewriteBatchedStatements=false", "dc1", true, "user6",
            "password6"));

    List<DbEndpoint> obtainedEndpoints = AccountStatsMySqlStoreFactory.setRewriteBatchedStatements(endpoints);
    assertEquals(expectedEndpoints, obtainedEndpoints);
  }
}
