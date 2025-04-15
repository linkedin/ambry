/*
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

package com.github.ambry.config;

public class InMemoryAccountConfig extends AccountServiceConfig {
  /** Prefix used for all configuration options of the in-memory account service.*/
  public static final String IN_MEMORY_ACCOUNT_PREFIX = "inmemory.account.";

  public static final String FILE_PATH = IN_MEMORY_ACCOUNT_PREFIX + "file.path";
  public static final String FILE_PATH_DEFAULT = "/tmp/accounts.json";

  @Config(FILE_PATH)
  @Default(FILE_PATH_DEFAULT)
  public final String inMemoryAccountFilePath;

  public InMemoryAccountConfig(VerifiableProperties verifiableProperties) {
    super(verifiableProperties);
    inMemoryAccountFilePath = verifiableProperties.getString(FILE_PATH, FILE_PATH_DEFAULT);
  }
}
