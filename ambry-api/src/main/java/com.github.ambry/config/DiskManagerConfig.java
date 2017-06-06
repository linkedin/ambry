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

package com.github.ambry.config;

public class DiskManagerConfig {

  /**
   * The name of the folder where the reserve disk segment pool will be.
   * A directory with this name will be created at the root of each disk.
   */
  @Config("disk.reserve.file.dir.name")
  @Default("reserve-pool")
  public final String diskReserveFileDirName;

  @Config("disk.required.swap.segments.per.size")
  @Default("1")
  public final int diskRequiredSwapSegmentsPerSize;

  public DiskManagerConfig(VerifiableProperties verifiableProperties) {
    diskReserveFileDirName = verifiableProperties.getString("disk.reserve.file.dir.name", "reserve-pool");
    diskRequiredSwapSegmentsPerSize =
        verifiableProperties.getIntInRange("disk.required.swap.segments.per.size", 1, 0, 1000);
  }
}
