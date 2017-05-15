/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import java.util.HashMap;
import java.util.Map;


/**
 * Mock {@link HelixAdminFactory} to instantiate {@link MockHelixAdmin}
 */
public class MockHelixAdminFactory extends HelixAdminFactory {
  Map<String, MockHelixAdmin> helixAdminMap = new HashMap<>();

  @Override
  public MockHelixAdmin getHelixAdmin(String zkAddr) {
    if (helixAdminMap.containsKey(zkAddr)) {
      return helixAdminMap.get(zkAddr);
    } else {
      MockHelixAdmin helixAdmin = new MockHelixAdmin();
      helixAdminMap.put(zkAddr, helixAdmin);
      return helixAdmin;
    }
  }

  Map<String, MockHelixAdmin> getAllHelixAdmins() {
    return helixAdminMap;
  }
}
