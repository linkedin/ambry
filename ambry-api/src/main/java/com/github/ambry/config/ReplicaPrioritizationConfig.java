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

package com.github.ambry.config;

public class ReplicaPrioritizationConfig {
  public static final String REPLICA_PRIORITIZATION_STRATEGY_FOR_FILE_COPY = "replica.prioritization.strategy.for.file.copy";
  @Config(REPLICA_PRIORITIZATION_STRATEGY_FOR_FILE_COPY)
  public final ReplicaPrioritizationStrategy replicaPrioritizationStrategy;

  public ReplicaPrioritizationConfig(VerifiableProperties verifiableProperties) {
    replicaPrioritizationStrategy = ReplicaPrioritizationStrategy.valueOf(verifiableProperties.getString(REPLICA_PRIORITIZATION_STRATEGY_FOR_FILE_COPY,
        ReplicaPrioritizationStrategy.FirstComeFirstServe.name()));
  }
}


