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
