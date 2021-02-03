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
package com.github.ambry.quota.storage;

import com.github.ambry.config.StorageQuotaConfig;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;


/**
 * Factory for {@link JSONStringStorageQuotaSource}.
 */
public class JSONStringStorageQuotaSourceFactory implements StorageQuotaSourceFactory {
  private final StorageQuotaSource source;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSourceFactory}.
   * @param scheduler The {@link ScheduledExecutorService} to use.
   * @param config The {@link StorageQuotaConfig} to use.
   * @throws IOException
   */
  public JSONStringStorageQuotaSourceFactory(ScheduledExecutorService scheduler, StorageQuotaConfig config)
      throws IOException {
    source = new JSONStringStorageQuotaSource(config);
  }

  @Override
  public StorageQuotaSource getStorageQuotaSource() {
    return source;
  }
}
