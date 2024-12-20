/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import org.json.JSONException;
import org.json.JSONObject;


// Permits Replica to be constructed with a null Partition
public class TestReplica extends Replica {
  public TestReplica(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    super(hardwareLayout, null, jsonObject);
  }

  public TestReplica(TestUtils.TestHardwareLayout hardwareLayout, Disk disk) throws JSONException {
    super(null, disk, hardwareLayout.clusterMapConfig);
  }

  @Override
  public void validatePartition() {
    // Null OK
  }
}
