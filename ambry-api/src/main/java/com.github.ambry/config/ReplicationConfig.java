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
   * The time (in ms) to sleep between replication cycles to throttle the replica thread in case the thread handles
   * intra datacenter replicas
   */
  @Config("replication.intra.replica.thread.throttle.sleep.duration.ms")
  @Default("0")
  public final long replicationIntraReplicaThreadThrottleSleepDurationMs;

  /**
   * The time (in ms) to sleep between replication cycles to throttle the replica thread in case the thread handles
   * inter datacenter replicas
   */
  @Config("replication.inter.replica.thread.throttle.sleep.duration.ms")
  @Default("0")
  public final long replicationInterReplicaThreadThrottleSleepDurationMs;

  /**
   * The time (in ms) to sleep between replication cycles when the replica thread is not doing any useful work
   */
  @Config("replication.replica.thread.idle.sleep.duration.ms")
  @Default("0")
  public final long replicationReplicaThreadIdleSleepDurationMs;

  /**
   * The time (in ms) to temporarily disable replication for a replica in order to reduce wasteful network calls
   */
  @Config("replication.synced.replica.backoff.duration.ms")
  @Default("0")
  public final long replicationSyncedReplicaBackoffDurationMs;

  /**
   * The fetch size is an approximate total size that a remote server would return on a fetch request.
   * This is not guaranteed to be always obeyed. For example, if a single blob is larger than the fetch size
   * the entire blob would be returned
   */
  @Config("replication.fetch.size.in.bytes")
  @Default("1048576")
  public final long replicationFetchSizeInBytes;

  /**
   * If true, replication Get requests asks for deleted and expired blobs as well to succeed in scenarios where blobs
   * get deleted or expired after replication metadata exchange.
   */
  @Config("replication.include.all")
  @Default("true")
  public final boolean replicationIncludeAll;

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
    replicationIntraReplicaThreadThrottleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.intra.replica.thread.throttle.sleep.duration.ms", 0, 0,
            Long.MAX_VALUE);
    replicationInterReplicaThreadThrottleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.inter.replica.thread.throttle.sleep.duration.ms", 0, 0,
            Long.MAX_VALUE);
    replicationReplicaThreadIdleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.replica.thread.idle.sleep.duration.ms", 0, 0, Long.MAX_VALUE);
    replicationSyncedReplicaBackoffDurationMs =
        verifiableProperties.getLongInRange("replication.synced.replica.backoff.duration.ms", 0, 0, Long.MAX_VALUE);
    replicationFetchSizeInBytes =
        verifiableProperties.getLongInRange("replication.fetch.size.in.bytes", 1048576, 1, Long.MAX_VALUE);
    replicationIncludeAll = verifiableProperties.getBoolean("replication.include.all", true);
  }
}
