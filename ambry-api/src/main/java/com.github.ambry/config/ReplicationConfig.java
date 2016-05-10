/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

/**
 * The configs for the replication layer
 */
public class ReplicationConfig {
  /**
   * The factory class the replication uses to creates its tokens
   */
  @Config("replication.token.factory")
  @Default("com.github.ambry.store.StoreFindTokenFactory")
  public final String replicationTokenFactory;

  /**
   * The number of replica threads on each server that runs the replication protocol for intra dc replication
   */
  @Config("replication.no.of.intra.dc.replica.threads")
  @Default("1")
  public final int replicationNumOfIntraDCReplicaThreads;

  /**
   * The number of replica threads on each server that runs the replication protocol for inter dc replication
   */
  @Config("replication.no.of.inter.dc.replica.threads")
  @Default("1")
  public final int replicationNumOfInterDCReplicaThreads;

  /**
   * The timeout to get a connection checkout from the connection pool for replication
   */
  @Config("replication.connection.pool.checkout.timeout.ms")
  @Default("5000")
  public final int replicationConnectionPoolCheckoutTimeoutMs;

  /**
   * The flush interval for persisting the replica tokens to disk
   */
  @Config("replication.token.flush.interval.seconds")
  @Default("300")
  public final int replicationTokenFlushIntervalSeconds;

  /**
   * The initial delay to start the replica token flush thread
   */
  @Config("replication.token.flush.delay.seconds")
  @Default("5")
  public final int replicationTokenFlushDelaySeconds;

  /**
   * The fetch size is an approximate total size that a remote server would return on a fetch request.
   * This is not guaranteed to be always obeyed. For example, if a single blob is larger than the fetch size
   * the entire blob would be returned
   */
  @Config("replication.fetch.size.in.bytes")
  @Default("1048576")
  public final long replicationFetchSizeInBytes;

  /**
   * The time for which replication waits between replication of remote replicas of a partition
   */
  @Config("replication.wait.time.between.replicas.ms")
  @Default("1000")
  public final int replicaWaitTimeBetweenReplicasMs;

  /**
   * The max lag above which replication does not wait between replicas. A larger value would slow down replication
   * while reduces the chance of conflicts with direct puts. A smaller value would speed up replication but
   * increase the chance of conflicts with direct puts
   */
  @Config("replication.max.lag.for.wait.time.in.bytes")
  @Default("5242880")
  public final long replicationMaxLagForWaitTimeInBytes;

  /**
   * Whether message stream should be tested for validity so that only valid ones are considered during replication
   */
  @Config("replication.validate.message.stream")
  @Default("false")
  public final boolean replicationValidateMessageStream;

  public ReplicationConfig(VerifiableProperties verifiableProperties) {

    replicationTokenFactory =
        verifiableProperties.getString("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    replicationNumOfIntraDCReplicaThreads =
        verifiableProperties.getInt("replication.no.of.intra.dc.replica.threads", 1);
    replicationNumOfInterDCReplicaThreads =
        verifiableProperties.getInt("replication.no.of.inter.dc.replica.threads", 1);
    replicationConnectionPoolCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("replication.connection.pool.checkout.timeout.ms", 5000, 1000, 10000);
    replicationTokenFlushIntervalSeconds =
        verifiableProperties.getIntInRange("replication.token.flush.interval.seconds", 300, 5, Integer.MAX_VALUE);
    replicationTokenFlushDelaySeconds =
        verifiableProperties.getIntInRange("replication.token.flush.delay.seconds", 5, 1, Integer.MAX_VALUE);
    replicationFetchSizeInBytes =
        verifiableProperties.getLongInRange("replication.fetch.size.in.bytes", 1048576, 0, 20971520);
    replicaWaitTimeBetweenReplicasMs =
        verifiableProperties.getIntInRange("replication.wait.time.between.replicas.ms", 1000, 0, 1000000);
    replicationMaxLagForWaitTimeInBytes =
        verifiableProperties.getLongInRange("replication.max.lag.for.wait.time.in.bytes", 5242880, 0, 104857600);
    replicationValidateMessageStream = verifiableProperties.getBoolean("replication.validate.message.stream", false);
  }
}
