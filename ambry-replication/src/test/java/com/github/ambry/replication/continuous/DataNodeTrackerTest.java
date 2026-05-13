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
package com.github.ambry.replication.continuous;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.replication.RemoteReplicaInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link DataNodeTracker#chunkReplicas}. Covers both the legacy sequential-slice path
 * (flag off) and the sort-by-lag + round-robin path (flag on). Scope is intentionally limited to
 * the chunking helper; the {@link DataNodeTracker} constructor's only added behavior is "call
 * {@code chunkReplicas} with the right args" and is exercised through {@code ReplicaThread} tests.
 *
 * Lag values for mock replicas are stored in a side-channel {@link #lagOf} map; the lag extractor
 * passed to {@code chunkReplicas} reads from this map. This avoids the test having to invoke the
 * package-private {@code RemoteReplicaInfo.getRemoteLagFromLocalInBytes()} from a different package.
 */
public class DataNodeTrackerTest {

  private final Map<RemoteReplicaInfo, Long> lagOf = new HashMap<>();
  private final ToLongFunction<RemoteReplicaInfo> extractor = lagOf::get;

  /** Flag off: behavior matches sequential slicing via {@code Utils.partitionList}. */
  @Test
  public void testChunkReplicasSpreadDisabledIsSequentialSlice() {
    // 7 replicas, max chunk size 3 → expect [[0,1,2],[3,4,5],[6]] in input order.
    List<RemoteReplicaInfo> replicas = makeReplicas(7, i -> 0L);  // lags don't matter when spread=false

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 3, false, extractor);

    assertEquals(3, chunks.size());
    assertEquals(replicas.subList(0, 3), chunks.get(0));
    assertEquals(replicas.subList(3, 6), chunks.get(1));
    assertEquals(replicas.subList(6, 7), chunks.get(2));
  }

  /** Flag on: top laggers land in distinct chunks. */
  @Test
  public void testChunkReplicasSpreadDistributesLaggersAcrossChunks() {
    // 6 replicas, lags [10, 20, 30, 40, 50, 60], max chunk size 3 → 2 chunks.
    // After sort DESC: [60,50,40,30,20,10] → round-robin: chunk0=[60,40,20], chunk1=[50,30,10].
    // Both top laggers (60 and 50) MUST be in different chunks.
    List<RemoteReplicaInfo> replicas = makeReplicas(6, i -> 10L * (i + 1));

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 3, true, extractor);

    assertEquals(2, chunks.size());
    assertEquals("Each chunk should be at most 3 replicas", 3, chunks.get(0).size());
    assertEquals(3, chunks.get(1).size());
    // Verify the top two laggers are not co-located.
    assertNotEquals("Top two laggers must land in different chunks", findChunk(chunks, 60L), findChunk(chunks, 50L));
    // Verify each chunk's lag composition matches the round-robin pattern.
    assertEquals(Arrays.asList(60L, 40L, 20L), lagsOf(chunks.get(0)));
    assertEquals(Arrays.asList(50L, 30L, 10L), lagsOf(chunks.get(1)));
  }

  /** Flag on: equal-lag replicas keep their input order (Java's stable sort). */
  @Test
  public void testChunkReplicasSpreadStableSortOnEqualLags() {
    // 6 replicas all with lag=0 and known partition-id order. After a stable sort by lag DESC,
    // their relative order must be unchanged. Round-robin into 2 chunks of size 3 then gives
    // chunk0=[p0,p2,p4], chunk1=[p1,p3,p5].
    long[] lags = new long[]{0, 0, 0, 0, 0, 0};
    String[] partitionIds = new String[]{"p0", "p1", "p2", "p3", "p4", "p5"};

    List<List<RemoteReplicaInfo>> chunks =
        DataNodeTracker.chunkReplicas(makeReplicasWithIds(lags, partitionIds), 3, true, extractor);

    assertEquals(2, chunks.size());
    assertEquals(Arrays.asList("p0", "p2", "p4"), partitionPathsOf(chunks.get(0)));
    assertEquals(Arrays.asList("p1", "p3", "p5"), partitionPathsOf(chunks.get(1)));
  }

  /** Empty input: returns zero chunks, matching {@code Utils.partitionList}'s behavior. */
  @Test
  public void testChunkReplicasEmptyList() {
    assertTrue(DataNodeTracker.chunkReplicas(Collections.emptyList(), 3, false, extractor).isEmpty());
    assertTrue("Spread path must match sequential path for empty input",
        DataNodeTracker.chunkReplicas(Collections.emptyList(), 3, true, extractor).isEmpty());
  }

  /** Flag on, single replica: returns one chunk with that replica. */
  @Test
  public void testChunkReplicasSpreadSingleReplica() {
    List<RemoteReplicaInfo> replicas = makeReplicas(1, i -> 42L);

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 3, true, extractor);

    assertEquals(1, chunks.size());
    assertEquals(1, chunks.get(0).size());
    assertEquals(42L, (long) lagOf.get(chunks.get(0).get(0)));
  }

  /** Flag on, max chunk size >= list size: returns one chunk containing all replicas (sorted). */
  @Test
  public void testChunkReplicasSpreadSingleChunkWhenMaxExceedsSize() {
    List<RemoteReplicaInfo> replicas = makeReplicas(3, i -> (long) i);  // lags 0, 1, 2

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 10, true, extractor);

    assertEquals(1, chunks.size());
    // Sorted DESC: lags should be [2, 1, 0].
    assertEquals(Arrays.asList(2L, 1L, 0L), lagsOf(chunks.get(0)));
  }

  /** maxActiveGroupSize <= 0: matches legacy fallback (single chunk with all replicas, unchanged order). */
  @Test
  public void testChunkReplicasNonPositiveMaxReturnsSingleChunk() {
    List<RemoteReplicaInfo> replicas = makeReplicas(5, i -> 10L);

    List<List<RemoteReplicaInfo>> chunksSpreadOff = DataNodeTracker.chunkReplicas(replicas, 0, false, extractor);
    List<List<RemoteReplicaInfo>> chunksSpreadOn = DataNodeTracker.chunkReplicas(replicas, 0, true, extractor);

    assertEquals(1, chunksSpreadOff.size());
    assertEquals(replicas, chunksSpreadOff.get(0));
    assertEquals("max<=0 takes the same single-chunk fallback regardless of spread flag", 1, chunksSpreadOn.size());
    assertEquals(replicas, chunksSpreadOn.get(0));
  }

  /**
   * Conservation: every input replica appears exactly once across all chunks (no drops, no duplicates).
   * The cycle iteration depends on this — losing a replica would silently skip replication progress.
   */
  @Test
  public void testChunkReplicasConservesAllReplicas() {
    List<RemoteReplicaInfo> replicas = makeReplicas(11, i -> (long) (i * 7));

    for (boolean spread : new boolean[]{false, true}) {
      for (int maxSize : new int[]{1, 2, 3, 5, 11, 100}) {
        List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, maxSize, spread, extractor);
        Set<RemoteReplicaInfo> flattened = chunks.stream().flatMap(List::stream).collect(Collectors.toSet());
        long totalCount = chunks.stream().mapToLong(List::size).sum();
        String tag = "spread=" + spread + " maxSize=" + maxSize;
        assertEquals(tag + ": total replica count must equal input size", replicas.size(), totalCount);
        assertEquals(tag + ": all input replicas must be present, no duplicates",
            new HashSet<>(replicas), flattened);
      }
    }
  }

  /**
   * Chunk size cap: every chunk respects {@code maxActiveGroupSize} for awkward sizes that don't
   * divide evenly (N=10, M=3 → 4 chunks of 3/3/2/2; N=7, M=2 → 4 chunks of 2/2/2/1; etc).
   */
  @Test
  public void testChunkReplicasRespectsMaxSizeForAwkwardSizes() {
    int[][] cases = new int[][]{{10, 3}, {7, 2}, {13, 4}, {5, 2}, {1, 1}};
    for (int[] c : cases) {
      int n = c[0], m = c[1];
      List<RemoteReplicaInfo> replicas = makeReplicas(n, i -> (long) i);
      for (boolean spread : new boolean[]{false, true}) {
        List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, m, spread, extractor);
        for (int i = 0; i < chunks.size(); i++) {
          assertTrue("N=" + n + " M=" + m + " spread=" + spread + " chunk[" + i + "].size=" + chunks.get(i).size(),
              chunks.get(i).size() <= m);
        }
        int expectedChunkCount = (int) Math.ceil((double) n / m);
        assertEquals("N=" + n + " M=" + m + " spread=" + spread + ": chunk count",
            expectedChunkCount, chunks.size());
      }
    }
  }

  /**
   * Lag is snapshotted before sorting, so {@link RemoteReplicaInfo#getRemoteLagFromLocalInBytes} is
   * read exactly once per replica regardless of sort cost. Production-availability regression guard:
   * a previous version of the helper read the extractor from inside the comparator, which would
   * (a) call the extractor O(N log N) times instead of N, and (b) under concurrent mutation of the
   * underlying field by another thread, could observe inconsistent lag values across compares and
   * trigger {@code TimSort: Comparison method violates its general contract!}, killing the cycle.
   *
   * Verifies the snapshot pattern directly: the extractor is called exactly {@code replicas.size()}
   * times. If anyone reverts the snapshot, the call count balloons to the sort's comparison budget
   * and this test fails — a deterministic guard that doesn't rely on TimSort's best-effort
   * inconsistency detection.
   */
  @Test
  public void testChunkReplicasCallsExtractorOncePerReplica() {
    List<RemoteReplicaInfo> replicas = makeReplicas(100, i -> (long) i);
    int[] callCount = {0};
    ToLongFunction<RemoteReplicaInfo> countingExtractor = r -> {
      callCount[0]++;
      return lagOf.get(r);
    };

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 5, true, countingExtractor);

    assertEquals("Snapshot must read extractor exactly once per replica; inline-in-comparator would "
        + "be O(N log N). Observed " + callCount[0] + " calls for " + replicas.size() + " replicas.",
        replicas.size(), callCount[0]);
    // Conservation still holds.
    Set<RemoteReplicaInfo> flat = chunks.stream().flatMap(List::stream).collect(Collectors.toSet());
    assertEquals(new HashSet<>(replicas), flat);
  }

  /** Negative lag values sort highest (most behind), still without exception. */
  @Test
  public void testChunkReplicasSpreadHandlesNegativeAndZeroLag() {
    // Note: lag can momentarily be negative if totalBytesReadFromLocalStore briefly exceeds
    // localStore size (pre-existing edge case in RemoteReplicaInfo). Verify sort is total and stable.
    List<RemoteReplicaInfo> replicas = new ArrayList<>();
    replicas.add(makeReplica(0L, "a"));
    replicas.add(makeReplica(-5L, "b"));
    replicas.add(makeReplica(100L, "c"));
    replicas.add(makeReplica(0L, "d"));

    List<List<RemoteReplicaInfo>> chunks = DataNodeTracker.chunkReplicas(replicas, 2, true, extractor);

    // Sorted DESC: [100, 0(a), 0(d), -5]. Round-robin into 2 chunks: chunk0=[100, 0(d)], chunk1=[0(a), -5].
    assertEquals(2, chunks.size());
    assertEquals(Arrays.asList(100L, 0L), lagsOf(chunks.get(0)));
    assertEquals(Arrays.asList(0L, -5L), lagsOf(chunks.get(1)));
  }

  // ---- helpers ----

  private interface LagFn {
    long lag(int i);
  }

  /** Creates {@code count} mock RemoteReplicaInfos with sequential partition ids "0".."count-1". */
  private List<RemoteReplicaInfo> makeReplicas(int count, LagFn lagFn) {
    List<RemoteReplicaInfo> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      out.add(makeReplica(lagFn.lag(i), Integer.toString(i)));
    }
    return out;
  }

  private List<RemoteReplicaInfo> makeReplicasWithIds(long[] lags, String[] partitionIds) {
    assertEquals(lags.length, partitionIds.length);
    List<RemoteReplicaInfo> out = new ArrayList<>(lags.length);
    for (int i = 0; i < lags.length; i++) {
      out.add(makeReplica(lags[i], partitionIds[i]));
    }
    return out;
  }

  private RemoteReplicaInfo makeReplica(long lag, String partitionIdPath) {
    PartitionId partitionId = mock(PartitionId.class);
    when(partitionId.toPathString()).thenReturn(partitionIdPath);
    ReplicaId replicaId = mock(ReplicaId.class);
    when(replicaId.getPartitionId()).thenReturn(partitionId);
    RemoteReplicaInfo replica = mock(RemoteReplicaInfo.class);
    when(replica.getReplicaId()).thenReturn(replicaId);
    lagOf.put(replica, lag);
    return replica;
  }

  private int findChunk(List<List<RemoteReplicaInfo>> chunks, long lag) {
    for (int i = 0; i < chunks.size(); i++) {
      for (RemoteReplicaInfo r : chunks.get(i)) {
        if (lagOf.get(r) == lag) {
          return i;
        }
      }
    }
    fail("Lag " + lag + " not found in any chunk");
    return -1;
  }

  private List<Long> lagsOf(List<RemoteReplicaInfo> chunk) {
    return chunk.stream().map(lagOf::get).collect(Collectors.toList());
  }

  private static List<String> partitionPathsOf(List<RemoteReplicaInfo> chunk) {
    return chunk.stream().map(r -> r.getReplicaId().getPartitionId().toPathString()).collect(Collectors.toList());
  }
}
