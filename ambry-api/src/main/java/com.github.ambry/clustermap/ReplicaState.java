/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

/**
 * The states of ambry replica that are managed by Helix controller.
 * Ambry router makes decision based on these states when routing requests.
 */
public enum ReplicaState {
  /**
   * Initial state of replica when starting up.
   * Router should not send any request to replica in this state.
   */
  OFFLINE,

  /**
   * Bootstrap state is an intermediate state between OFFLINE and STANDBY.
   * This state allows replica to do some bootstrap work like checking replication lag and catching up with peers.
   * GET, DELETE, TTLUpdate can be routed to replica in this state.
   */
  BOOTSTRAP,

  /**
   * The state in which replica is fully functioning. That is, it can receive all types of requests.
   */
  STANDBY,

  /**
   * The state in which replica is fully functioning. For replicas from same partition in same DC, at most one is in
   * LEADER state. Currently we don't distinguish LEADER from STANDBY and they both can receive all types of requests.
   * In the future, we may leverage LEADER replica to perform cross-dc replication.
   */
  LEADER,

  /**
   * Inactive is an intermediate state between OFFLINE and STANDBY.
   * This state allows replica to do some decommission work like making sure its peers have caught up with it.
   * Only GET request is allowed to be routed to inactive replica.
   */
  INACTIVE,

  /**
   * When replica behaves unexpectedly and Helix makes replica in this state.
   */
  ERROR,

  /**
   * End state of a replica that is decommissioned.
   */
  DROPPED
}
