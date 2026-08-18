/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.github.ambry.replication.continuous.ReplicaStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link ReplicaThread#shouldAutoPrunePriority}. The predicate is
 * {@code status == ACTIVE && lag < threshold}: prune only when the priority replica is healthy AND
 * caught up. Do NOT prune on transient outage (OFFLINE with stale low lag) — the operator's bias
 * should persist across the outage and resume once the peer recovers.
 */
public class ReplicaThreadPriorityAutoPruneTest {

  private final Map<RemoteReplicaInfo, ReplicaStatus> statuses = new HashMap<>();
  private final Map<RemoteReplicaInfo, Long> lags = new HashMap<>();
  private final Function<RemoteReplicaInfo, ReplicaStatus> statusFn = statuses::get;
  private final ToLongFunction<RemoteReplicaInfo> lagFn = lags::get;

  /** All replicas ACTIVE with lag below threshold ⇒ prune. */
  @Test
  public void testPruneWhenAllActiveAndBelowThreshold() {
    List<RemoteReplicaInfo> infos = Arrays.asList(
        mockReplicaInfo(ReplicaStatus.ACTIVE, 100L),
        mockReplicaInfo(ReplicaStatus.ACTIVE, 200L));

    assertTrue("ACTIVE + lag < threshold ⇒ prune",
        ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
  }

  /** Lag at threshold (exclusive) ⇒ do NOT prune. */
  @Test
  public void testNoPruneWhenLagAtThreshold() {
    List<RemoteReplicaInfo> infos = Collections.singletonList(mockReplicaInfo(ReplicaStatus.ACTIVE, 1000L));

    assertFalse("lag == threshold should NOT prune (strict less-than)",
        ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
  }

  /** Any replica with lag >= threshold ⇒ do NOT prune. */
  @Test
  public void testNoPruneWhenOneReplicaStillLagging() {
    List<RemoteReplicaInfo> infos = Arrays.asList(
        mockReplicaInfo(ReplicaStatus.ACTIVE, 100L),
        mockReplicaInfo(ReplicaStatus.ACTIVE, 5000L));

    assertFalse("one lagging replica blocks prune (all-or-nothing)",
        ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
  }

  /**
   * Replica is OFFLINE with stale-but-low cached lag ⇒ do NOT prune. This is the bug-fix scenario:
   * with the original predicate (OFFLINE && lag < threshold) the priority would be silently dropped
   * on transient outage, losing the operator's bias when the peer recovers.
   */
  @Test
  public void testNoPruneWhenOfflineWithStaleLag() {
    List<RemoteReplicaInfo> infos = Collections.singletonList(mockReplicaInfo(ReplicaStatus.OFFLINE, 100L));

    assertFalse("OFFLINE with stale low lag must NOT prune — operator bias must persist across outage",
        ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
  }

  /** Any replica with non-ACTIVE status blocks prune (mixed-state ACTIVE + STANDBY example). */
  @Test
  public void testNoPruneWhenAnyReplicaNotActive() {
    for (ReplicaStatus nonActive : new ReplicaStatus[]{ReplicaStatus.UNKNOWN, ReplicaStatus.OFFLINE,
        ReplicaStatus.STANDBY, ReplicaStatus.STANDBY_TIMED_OUT_ON_NO_PROGRESS}) {
      statuses.clear();
      lags.clear();
      List<RemoteReplicaInfo> infos = Arrays.asList(
          mockReplicaInfo(ReplicaStatus.ACTIVE, 100L),
          mockReplicaInfo(nonActive, 100L));

      assertFalse("status " + nonActive + " on any replica blocks prune",
          ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
    }
  }

  /** Single replica, ACTIVE with lag below threshold ⇒ prune. */
  @Test
  public void testPruneWhenSingleActiveBelowThreshold() {
    List<RemoteReplicaInfo> infos = Collections.singletonList(mockReplicaInfo(ReplicaStatus.ACTIVE, 0L));

    assertTrue("singleton ACTIVE replica below threshold ⇒ prune",
        ReplicaThread.shouldAutoPrunePriority(infos, 1000L, statusFn, lagFn));
  }

  /** Zero lag is the canonical "fully caught up" case. */
  @Test
  public void testPruneWhenZeroLagActive() {
    List<RemoteReplicaInfo> infos = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      infos.add(mockReplicaInfo(ReplicaStatus.ACTIVE, 0L));
    }

    assertTrue("all replicas at zero lag and ACTIVE ⇒ prune",
        ReplicaThread.shouldAutoPrunePriority(infos, 1L, statusFn, lagFn));
  }

  /**
   * Empty list returns false: a priority partition with no replicas in this thread is not a
   * candidate for auto-prune here (some other thread that owns the replicas decides). Pins the
   * self-defending behavior so the call site doesn't have to gate on emptiness separately.
   */
  @Test
  public void testEmptyListReturnsFalse() {
    assertFalse("empty list ⇒ not a prune candidate (defended inside the predicate)",
        ReplicaThread.shouldAutoPrunePriority(Collections.emptyList(), 1000L, statusFn, lagFn));
  }

  private RemoteReplicaInfo mockReplicaInfo(ReplicaStatus status, long lag) {
    RemoteReplicaInfo info = mock(RemoteReplicaInfo.class);
    statuses.put(info, status);
    lags.put(info, lag);
    return info;
  }
}
