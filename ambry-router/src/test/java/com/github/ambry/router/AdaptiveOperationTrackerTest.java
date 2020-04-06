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
package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.Resource;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.CachedHistogram;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link AdaptiveOperationTracker} that tests the adaptability based on request latencies. This class
 * only tests features not already tested in {@link OperationTrackerTest}.
 *
 * The status of an operation is represented as in the following format:
 *
 * [local unsent count] - [local inflight count] - [local succeeded count] - [local failed count];
 * [remote unsent count] - [remote inflight count] - [remote succeeded count] - [remote failed count]
 *
 * For example: 3-0-0-0; 9-0-0-0
 */
public class AdaptiveOperationTrackerTest {
  private static final int REPLICA_COUNT = 6;
  private static final int PORT = 6666;
  private static final double QUANTILE = 0.9;
  private static final Pair<Long, Long> LOCAL_COLO_LATENCY_RANGE = new Pair<>(0L, 58L);
  private static final Pair<Long, Long> CROSS_COLO_LATENCY_RANGE = new Pair<>(120L, 220L);

  private final List<MockDataNodeId> datanodes;
  private final MockPartitionId mockPartition;
  private final String localDcName;
  private final MockClusterMap mockClusterMap;
  private final Map<PartitionId, LinkedList<ReplicaId>> partitionAndInflightReplicas = new HashMap<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();
  private final Time time = new MockTime();
  private final Histogram localColoTracker;
  private final Histogram crossColoTracker;
  private final Counter pastDueCounter;
  private final long MIN_DATA_POINTS_REQUIRED;
  private NonBlockingRouterMetrics routerMetrics;
  private RouterConfig defaultRouterConfig;
  private OperationTrackerScope trackerScope;

  /**
   * Constructor that sets up state.
   */
  public AdaptiveOperationTrackerTest() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Arrays.asList("mockMountPath0", "mockMountPath1", "mockMountPath2");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1")));
    localDcName = datanodes.get(0).getDatacenterName();
    mockPartition = new MockPartitionId();
    for (int i = 0; i < REPLICA_COUNT; i++) {
      mockPartition.replicaIds.add(new MockReplicaId(PORT, mockPartition, datanodes.get(i % datanodes.size()), 0));
    }
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", localDcName);
    defaultRouterConfig = new RouterConfig(new VerifiableProperties(props));
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, defaultRouterConfig);
    localColoTracker = routerMetrics.getBlobLocalDcLatencyMs;
    crossColoTracker = routerMetrics.getBlobCrossDcLatencyMs;
    pastDueCounter = routerMetrics.getBlobPastDueCount;
    MIN_DATA_POINTS_REQUIRED = defaultRouterConfig.routerOperationTrackerMinDataPointsRequired;
    trackerScope = OperationTrackerScope.Datacenter;
  }

  /**
   * Tests that requests are discounted from the parallelism count once they move beyond the tolerance quantile.
   * @throws InterruptedException
   */
  @Test
  public void adaptationTest() throws InterruptedException {
    primeTracker(localColoTracker, MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(QUANTILE);
    double crossColoCutoff = crossColoTracker.getSnapshot().getValue(QUANTILE);

    OperationTracker ot = getOperationTracker(createRouterConfig(true, REPLICA_COUNT, 2, 6, null, true), mockPartition);
    // 3-0-0-0; 3-0-0-0
    sendRequests(ot, 2);
    // 1-2-0-0; 3-0-0-0
    // sleep for less than the cutoff
    time.sleep((long) localColoCutoff - 2);
    sendRequests(ot, 0);
    // push it over the edge
    time.sleep(5);
    // should send two requests because both of the oldest requests are past their due times
    // the second of the two requests is a cross colo request
    sendRequests(ot, 2);
    // 0-3-0-0; 2-1-0-0
    time.sleep((long) localColoCutoff + 2);
    // second cross colo request sent (local colo request is past due but the first cross colo request is not past due).
    sendRequests(ot, 1);
    // 0-3-0-0; 1-2-0-0
    long sleepTime = (long) localColoCutoff + 2;
    time.sleep(sleepTime);
    // no requests should be sent.
    // for first cross colo request, 2 * (localColoCutoff + 2) <= 2 * (57 * 0.9 + 2) = 106.6 < 120 * 0.9 <= crossColoCutoff
    sendRequests(ot, 0);
    // 0-3-0-0; 1-2-0-0
    sleepTime = (long) (crossColoCutoff - localColoCutoff) + 2;
    time.sleep(sleepTime);
    // third cross colo request sent (first cross colo request is past due)
    sendRequests(ot, 1);
    // 0-3-0-0; 0-3-0-0
    time.sleep((long) crossColoCutoff + 2);
    // no more replicas left to send requests to
    sendRequests(ot, 0);
    // generate a response for every request and make sure there are no errors
    for (int i = 0; i < REPLICA_COUNT; i++) {
      assertFalse("Operation should not be done", ot.isDone());
      ot.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be REPLICA_COUNT - 2 (note that pastDueCounter is updated only when Iterator.remove() is called)
    assertEquals("Past due counter is inconsistent", REPLICA_COUNT - 2, pastDueCounter.getCount());
  }

  /**
   * Test that the max number of inflight requests should not exceed configured number.
   * @throws InterruptedException
   */
  @Test
  public void clampMaxInflightRequestsTest() throws InterruptedException {
    primeTracker(localColoTracker, MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(QUANTILE);
    double crossColoCutoff = crossColoTracker.getSnapshot().getValue(QUANTILE);
    // set max inflight number = 2 and excludeTimeout = false in this test
    OperationTracker ot = getOperationTracker(createRouterConfig(true, 3, 2, 3, null, false), mockPartition);
    // 3-0-0-0; 3-0-0-0
    sendRequests(ot, 2);
    // 1-2-0-0; 3-0-0-0
    // sleep for less than the cutoff
    time.sleep((long) localColoCutoff - 2);
    sendRequests(ot, 0);
    // push it over the edge
    time.sleep(5);
    // should only send one request because (inflight num + 1) == routerConfig.routerOperationTrackerMaxInflightRequests
    sendRequests(ot, 1);
    // 0-3-0-0; 3-0-0-0
    // mark one request TIMED_OUT
    ot.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(), TrackedRequestFinalState.TIMED_OUT);
    // should send out 1 request
    sendRequests(ot, 1);
    // 0-2-0-1; 2-1-0-0

    // mark one request FAILURE
    ot.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(), TrackedRequestFinalState.FAILURE);
    time.sleep((long) (crossColoCutoff - localColoCutoff) + 2);

    // should send out 1 request
    sendRequests(ot, 1);
    // 0-1-0-2; 1-2-0-0

    // mark 3 inflight requests SUCCESS
    while (!partitionAndInflightReplicas.get(mockPartition).isEmpty()) {
      assertFalse("Operation should not be done", ot.isDone());
      ot.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be 3 (note that pastDueCounter is updated only when Iterator.remove() is called)
    assertEquals("Past due counter is not expected", 3, pastDueCounter.getCount());
  }

  /**
   * Tests that adaptive tracker uses separate partition-level histogram to determine if inflight requests are past due.
   * @throws Exception
   */
  @Test
  public void partitionLevelAdaptiveTrackerTest() throws Exception {
    MockPartitionId mockPartition1 = new MockPartitionId(0L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    MockPartitionId mockPartition2 = new MockPartitionId(1L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    for (int i = 0; i < REPLICA_COUNT; i++) {
      mockPartition1.replicaIds.add(new MockReplicaId(PORT, mockPartition1, datanodes.get(i % datanodes.size()), 1));
      mockPartition2.replicaIds.add(new MockReplicaId(PORT, mockPartition2, datanodes.get(i % datanodes.size()), 2));
    }
    MockClusterMap clusterMap =
        new MockClusterMap(false, datanodes, 3, Arrays.asList(mockPartition1, mockPartition2), localDcName);
    trackerScope = OperationTrackerScope.Partition;
    RouterConfig routerConfig = createRouterConfig(true, 2, 1, 6, null, true);
    NonBlockingRouterMetrics originalMetrics = routerMetrics;
    routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    Counter pastDueCount = routerMetrics.getBlobPastDueCount;
    Map<Resource, CachedHistogram> localColoMap = routerMetrics.getBlobLocalDcResourceToLatency;
    Map<Resource, CachedHistogram> crossColoMap = routerMetrics.getBlobCrossDcResourceToLatency;
    // mock different distribution of Histogram for two partitions
    Histogram localHistogram1 = localColoMap.get(mockPartition1);
    Histogram localHistogram2 = localColoMap.get(mockPartition2);
    Histogram remoteHistogram1 = crossColoMap.get(mockPartition1);
    primeTracker(localHistogram1, routerConfig.routerOperationTrackerMinDataPointsRequired, new Pair<>(0L, 50L));
    primeTracker(localHistogram2, routerConfig.routerOperationTrackerMinDataPointsRequired, new Pair<>(100L, 120L));
    primeTracker(remoteHistogram1, routerConfig.routerOperationTrackerMinDataPointsRequired, new Pair<>(150L, 180L));
    OperationTracker tracker1 = getOperationTracker(routerConfig, mockPartition1);
    OperationTracker tracker2 = getOperationTracker(routerConfig, mockPartition2);
    double localColoCutoff1 = localHistogram1.getSnapshot().getValue(QUANTILE);
    double localColoCutoff2 = localHistogram2.getSnapshot().getValue(QUANTILE);
    double crossColoCutoff1 = remoteHistogram1.getSnapshot().getValue(QUANTILE);
    sendRequests(tracker2, 1);
    sendRequests(tracker1, 1);
    // partition1: 2-1-0-0, partition2: 2-1-0-0

    time.sleep((long) localColoCutoff1 + 1);
    // partition1 should send 2nd request, partition2 won't because its 1st request isn't past due.
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 0);
    // partition1: 1-2-0-0, partition2: 2-1-0-0

    time.sleep((long) (localColoCutoff2 - localColoCutoff1) + 2);
    // note that localColoCutoff2 > 2 * localColoCutoff1, then 2nd request of partition1 and 1st request of partition are both past due
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 1);
    // partition1: 0-3-0-0, partition2: 1-2-0-0

    time.sleep((long) localColoCutoff1 + 1);
    // 3rd local request of partition1 is past due and starts sending 1st cross-colo request
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 0);
    // partition1: 0-3-0-0(local), 2-1-0-0(remote);  partition2: 1-2-0-0(local)

    time.sleep((long) crossColoCutoff1 + 1);
    // 1st cross-colo request of partition1 is past due and 2nd local request of partition2 is past due.
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 1);
    // partition1: 0-3-0-0(local), 1-2-0-0(remote);  partition2: 0-3-0-0(local)

    // generate response for each request to make them successful
    for (int i = 0; i < 2; ++i) {
      assertFalse("Operation should not be done", tracker1.isDone() || tracker2.isDone());
      tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.SUCCESS);
      tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should have succeeded", tracker1.hasSucceeded() && tracker2.hasSucceeded());
    assertEquals("Past due counter is not expected", 4 + 2, pastDueCount.getCount());
    // complete remaining inflight requests and test different final state of request
    LinkedList<ReplicaId> inflightRequests1 = partitionAndInflightReplicas.get(mockPartition1);
    LinkedList<ReplicaId> inflightRequests2 = partitionAndInflightReplicas.get(mockPartition2);
    while (!inflightRequests1.isEmpty()) {
      tracker1.onResponse(inflightRequests1.poll(), TrackedRequestFinalState.FAILURE);
    }
    while (!inflightRequests2.isEmpty()) {
      tracker2.onResponse(inflightRequests2.poll(), TrackedRequestFinalState.TIMED_OUT);
    }
    // The number of data points in local colo histogram should be 5 (3 from partition1, 2 from partition2). Note that,
    // 3rd request of partition2 timed out which shouldn't be added to histogram.
    assertEquals("Mismatch in number of data points in local colo histogram", 5,
        routerMetrics.getBlobLocalDcLatencyMs.getCount());
    // The number of data points in cross colo histogram should be 2 (both of them come from partition1)
    assertEquals("Mismatch in number of data points in cross colo histogram", 2,
        routerMetrics.getBlobCrossDcLatencyMs.getCount());

    // additional test: mock new partition is dynamically added and adaptive operation track should be able to create
    // histogram on demand.
    MockPartitionId mockPartition3 = (MockPartitionId) clusterMap.createNewPartition(datanodes);
    OperationTracker tracker3 = getOperationTracker(routerConfig, mockPartition3);
    // send 1st request
    sendRequests(tracker3, 1);
    // attempt to send 2nd request to make tracker check histogram and create a new one associated with this partition
    // the oldest one hasn't passed due (because there are not enough data points in histogram), so 2nd is not sent
    sendRequests(tracker3, 0);
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.SUCCESS);
    // now it should be able to send 2nd request
    sendRequests(tracker3, 1);
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", tracker3.hasSucceeded());

    // restore the tracer scope and routerMetrics
    trackerScope = OperationTrackerScope.Datacenter;
    routerMetrics = originalMetrics;
  }

  /**
   * Tests that adaptive tracker uses separate node-level histogram to determine if inflight requests are past due.
   * @throws Exception
   */
  @Test
  public void nodeLevelAdaptiveTrackerTest() throws Exception {
    // Mock a simple partition layout for this test: Partition1 has two replicas, one on LocalHost1 and the other on RemoteHost1;
    // Similarly, Partition2 has two replicas, one on LocalHost2 and the other on RemoteHost1.
    MockPartitionId mockPartition1 = new MockPartitionId(1L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    MockPartitionId mockPartition2 = new MockPartitionId(2L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    // create a new list mock datanodes instead of using the default class member
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Arrays.asList("mockMountPath0", "mockMountPath1", "mockMountPath2");
    MockDataNodeId localHost1 = new MockDataNodeId("LocalHost1", portList, mountPaths, "dc-0");
    MockDataNodeId localHost2 = new MockDataNodeId("LocalHost2", portList, mountPaths, "dc-0");
    MockDataNodeId remoteHost1 = new MockDataNodeId("RemoteHost1", portList, mountPaths, "dc-1");
    List<MockDataNodeId> datanodes = new ArrayList<>(Arrays.asList(localHost1, localHost2, remoteHost1));
    // distribute replicas to nodes (Note that localDC name is still "dc-0" in current setup)
    mockPartition1.replicaIds.add(new MockReplicaId(PORT, mockPartition1, localHost1, 1));
    mockPartition2.replicaIds.add(new MockReplicaId(PORT, mockPartition2, localHost2, 2));
    mockPartition1.replicaIds.add(new MockReplicaId(PORT, mockPartition1, remoteHost1, 1));
    mockPartition2.replicaIds.add(new MockReplicaId(PORT, mockPartition2, remoteHost1, 2));
    MockClusterMap clusterMap =
        new MockClusterMap(false, datanodes, 3, Arrays.asList(mockPartition1, mockPartition2), localDcName);
    trackerScope = OperationTrackerScope.DataNode;
    RouterConfig routerConfig = createRouterConfig(true, 1, 1, 6, null, true);
    NonBlockingRouterMetrics originalMetrics = routerMetrics;
    routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    Counter pastDueCount = routerMetrics.getBlobPastDueCount;
    Map<Resource, CachedHistogram> localColoMap = routerMetrics.getBlobLocalDcResourceToLatency;
    Map<Resource, CachedHistogram> crossColoMap = routerMetrics.getBlobCrossDcResourceToLatency;
    // mock different latency distribution of local hosts and remote host
    Histogram localHistogram1 = localColoMap.get(localHost1);
    Histogram localHistogram2 = localColoMap.get(localHost2);
    primeTracker(localHistogram1, routerConfig.routerOperationTrackerMinDataPointsRequired, new Pair<>(0L, 50L));
    primeTracker(localHistogram2, routerConfig.routerOperationTrackerMinDataPointsRequired, new Pair<>(100L, 120L));
    double localHostCutoff1 = localHistogram1.getSnapshot().getValue(QUANTILE);
    double localHostCutoff2 = localHistogram2.getSnapshot().getValue(QUANTILE);
    OperationTracker tracker1 = getOperationTracker(routerConfig, mockPartition1);
    OperationTracker tracker2 = getOperationTracker(routerConfig, mockPartition2);
    // issue first request for both partitions in local DC
    sendRequests(tracker2, 1);
    sendRequests(tracker1, 1);
    // partition1: 0-1-0-0, partition2: 0-1-0-0

    time.sleep((long) localHostCutoff1 + 1);
    // partition1 should send 2nd request to RemoteNode1, partition2 won't because its 1st request isn't past due.
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 0);
    // partition1: 0-1-0-0(local), 0-1-0-0(remote); partition2: 0-1-0-0(local), 1-0-0-0(remote)

    time.sleep((long) (localHostCutoff2 - localHostCutoff1) + 2);
    sendRequests(tracker1, 0);
    sendRequests(tracker2, 1);
    // partition1: 0-1-0-0(local), 0-1-0-0(remote); partition2: 0-1-0-0(local), 0-1-0-0(remote)

    assertFalse("Operation should not be done", tracker1.isDone() || tracker2.isDone());
    // make local requests failed
    tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.TIMED_OUT);
    tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.FAILURE);
    // make remote requests successful
    tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.SUCCESS);
    tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", tracker1.hasSucceeded() && tracker2.hasSucceeded());
    // past due count should be 2 because requests to two local nodes didn't get response within threshold
    assertEquals("Past due counter is not expected", 2, pastDueCount.getCount());
    // number of data points in local colo histogram should be 1 because LocalHost2 finally responded FAILURE which would
    // update the histogram. Note that request to LocalHost1 became TIMED_OUT in the end which should not be counted.
    assertEquals("Mismatch in number of data points in local colo histogram", 1,
        routerMetrics.getBlobLocalDcLatencyMs.getCount());
    // number of data points in cross colo histogram should be 2 because both requests to RemoteHost1 succeeded and histogram
    // should be updated twice in this case.
    assertEquals("Mismatch in number of data points in cross colo histogram", 2,
        routerMetrics.getBlobCrossDcLatencyMs.getCount());

    // additional test: dynamically add 1 new partition and 2 new nodes. Each new node hosts a replica from new partition
    MockDataNodeId newNode1 = clusterMap.createNewDataNodes(1, "dc-0").get(0);
    MockDataNodeId newNode2 = clusterMap.createNewDataNodes(1, "dc-1").get(0);
    MockPartitionId mockPartition3 = new MockPartitionId(3L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    mockPartition3.replicaIds.add(new MockReplicaId(PORT, mockPartition3, newNode1, 1));
    mockPartition3.replicaIds.add(new MockReplicaId(PORT, mockPartition3, newNode2, 2));
    OperationTracker tracker3 = getOperationTracker(routerConfig, mockPartition3);
    // send 1st request
    sendRequests(tracker3, 1);
    // attempt to send 2nd one. This will trigger router metrics to create a histogram that associated with new node
    // However, there is no 2nd request out because new created histogram doesn't of enough data points.
    sendRequests(tracker3, 0);
    // make the 1st request fail
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.FAILURE);
    // 2nd request is sent
    sendRequests(tracker3, 1);
    // make the 2nd request succeed
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", tracker3.hasSucceeded());
    // restore the tracer scope and routerMetrics
    trackerScope = OperationTrackerScope.Datacenter;
    routerMetrics = originalMetrics;
  }

  /**
   * Tests that adaptive tracker uses separate disk-level histogram to determine if inflight requests are past due.
   * Mock a partition layout as follows for this test. This test also tests the case where new nodes and new partition
   * are dynamically added.
   *             |       |   Partition 1 |  Partition 2  | Partition 3 (added at runtime)
   * -------------------------------------------------------------------------------------
   * LocalHost1  | Disk0 |   Replica_1   |               |
   *             | Disk1 |               |  Replica_1    |
   * -------------------------------------------------------------------------------------
   * RemoteHost1 | Disk0 |   Replica_2   |  Replica_2    |
   *             | Disk1 |   Replica_3   |  Replica_3    |
   * -------------------------------------------------------------------------------------
   *  NewNode1   | Disk0 |               |               |          Replica_1
   *             | Disk1 |               |               |
   * -------------------------------------------------------------------------------------
   *  NewNod2    | Disk0 |               |               |
   *             | Disk1 |               |               |          Replica_2
   * @throws Exception
   */
  @Test
  public void diskLevelAdaptiveTrackerTest() throws Exception {
    MockPartitionId mockPartition1 = new MockPartitionId(1L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    MockPartitionId mockPartition2 = new MockPartitionId(2L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    // create a new list mock datanodes instead of using the default class member
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Arrays.asList("mockMountPath0", "mockMountPath1");
    MockDataNodeId localHost1 = new MockDataNodeId("LocalHost1", portList, mountPaths, "dc-0");
    MockDataNodeId remoteHost1 = new MockDataNodeId("RemoteHost1", portList, mountPaths, "dc-1");
    List<MockDataNodeId> datanodes = new ArrayList<>(Arrays.asList(localHost1, remoteHost1));
    // distribute replicas to nodes (Note that localDC name is still "dc-0" in current setup)
    ReplicaId partition1Replica1 = new MockReplicaId(PORT, mockPartition1, localHost1, 0);
    ReplicaId partition1Replica2 = new MockReplicaId(PORT, mockPartition1, remoteHost1, 0);
    ReplicaId partition1Replica3 = new MockReplicaId(PORT, mockPartition1, remoteHost1, 1);
    ReplicaId partition2Replica1 = new MockReplicaId(PORT, mockPartition2, localHost1, 1);
    mockPartition1.replicaIds.add(partition1Replica1);
    mockPartition1.replicaIds.add(partition1Replica2);
    mockPartition1.replicaIds.add(partition1Replica3);
    mockPartition2.replicaIds.add(partition2Replica1);
    mockPartition2.replicaIds.add(new MockReplicaId(PORT, mockPartition2, remoteHost1, 0));
    mockPartition2.replicaIds.add(new MockReplicaId(PORT, mockPartition2, remoteHost1, 1));
    MockClusterMap clusterMap =
        new MockClusterMap(false, datanodes, 2, Arrays.asList(mockPartition1, mockPartition2), localDcName);
    trackerScope = OperationTrackerScope.Disk;
    RouterConfig routerConfig = createRouterConfig(true, 1, 1, 6, null, true);
    NonBlockingRouterMetrics originalMetrics = routerMetrics;
    routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    Counter pastDueCount = routerMetrics.getBlobPastDueCount;
    Map<Resource, CachedHistogram> localColoMap = routerMetrics.getBlobLocalDcResourceToLatency;
    Map<Resource, CachedHistogram> crossColoMap = routerMetrics.getBlobCrossDcResourceToLatency;
    // mock different latency distribution of different disks
    Histogram localHostDisk0Histogram = localColoMap.get(partition1Replica1.getDiskId());
    Histogram localHostDisk1Histogram = localColoMap.get(partition2Replica1.getDiskId());
    Histogram remoteHostDisk0Histogram = crossColoMap.get(partition1Replica2.getDiskId());
    Histogram remoteHostDisk1Histogram = crossColoMap.get(partition1Replica3.getDiskId());
    primeTracker(localHostDisk0Histogram, routerConfig.routerOperationTrackerMinDataPointsRequired,
        new Pair<>(0L, 50L));
    primeTracker(localHostDisk1Histogram, routerConfig.routerOperationTrackerMinDataPointsRequired,
        new Pair<>(100L, 120L));
    primeTracker(remoteHostDisk0Histogram, routerConfig.routerOperationTrackerMinDataPointsRequired,
        new Pair<>(150L, 180L));
    primeTracker(remoteHostDisk1Histogram, routerConfig.routerOperationTrackerMinDataPointsRequired,
        new Pair<>(150L, 180L));
    double localHostDisk0Cutoff = localHostDisk0Histogram.getSnapshot().getValue(QUANTILE);
    double localHostDisk1Cutoff = localHostDisk1Histogram.getSnapshot().getValue(QUANTILE);
    double remoteHostDisk0Cutoff = remoteHostDisk0Histogram.getSnapshot().getValue(QUANTILE);
    OperationTracker tracker1 = getOperationTracker(routerConfig, mockPartition1);
    OperationTracker tracker2 = getOperationTracker(routerConfig, mockPartition2);
    // issue first request for both partitions in local DC
    sendRequests(tracker2, 1);
    sendRequests(tracker1, 1);
    // partition1: 0-1-0-0, partition2: 0-1-0-0
    time.sleep((long) localHostDisk0Cutoff + 1);
    // partition1 should send 2nd request to RemoteNode1, partition2 won't because its 1st request isn't past due.
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 0);
    // partition1: 0-1-0-0(local), 1-1-0-0(remote); partition2: 0-1-0-0(local), 2-0-0-0(remote)

    time.sleep((long) (localHostDisk1Cutoff - localHostDisk0Cutoff) + 2);
    // partition2 Replica1 on localhost Disk1 is past due, so the request should be sent to remote host
    sendRequests(tracker1, 0);
    sendRequests(tracker2, 1);
    // partition1: 0-1-0-0(local), 1-1-0-0(remote); partition2: 0-1-0-0(local), 2-0-0-0(remote)

    time.sleep((long) remoteHostDisk0Cutoff + 1);
    // both requests are past due (Note that they have same latency histogram)
    sendRequests(tracker1, 1);
    sendRequests(tracker2, 1);

    assertFalse("Operation should not be done", tracker1.isDone() || tracker2.isDone());
    // make local requests successful
    tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.SUCCESS);
    tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.SUCCESS);
    // make remote requests failed
    tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.FAILURE);
    tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.TIMED_OUT);
    tracker1.onResponse(partitionAndInflightReplicas.get(mockPartition1).poll(), TrackedRequestFinalState.TIMED_OUT);
    tracker2.onResponse(partitionAndInflightReplicas.get(mockPartition2).poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should have succeeded", tracker1.hasSucceeded() && tracker2.hasSucceeded());
    // past due count should be 4 because for each partition there were one local and one remote request that didn't get
    // response within threshold. In total, it should be 2 * (1 + 1) = 4
    assertEquals("Past due counter is not expected", 4, pastDueCount.getCount());
    // number of data points in local colo histogram should be 2 because both requests finally succeeded
    assertEquals("Mismatch in number of data points in local colo histogram", 2,
        routerMetrics.getBlobLocalDcLatencyMs.getCount());
    // number of data points in cross colo histogram should be 2 because two timed-out requests should be counted
    assertEquals("Mismatch in number of data points in cross colo histogram", 2,
        routerMetrics.getBlobCrossDcLatencyMs.getCount());

    // additional test: dynamically add 1 new partition and 2 new nodes. Each new node hosts a replica from new partition
    MockDataNodeId newNode1 = clusterMap.createNewDataNodes(1, "dc-0").get(0);
    MockDataNodeId newNode2 = clusterMap.createNewDataNodes(1, "dc-1").get(0);
    MockPartitionId mockPartition3 = new MockPartitionId(3L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    mockPartition3.replicaIds.add(new MockReplicaId(PORT, mockPartition3, newNode1, 0));
    mockPartition3.replicaIds.add(new MockReplicaId(PORT, mockPartition3, newNode2, 1));
    OperationTracker tracker3 = getOperationTracker(routerConfig, mockPartition3);
    // send 1st request
    sendRequests(tracker3, 1);
    // attempt to send 2nd one. This will trigger router metrics to create a histogram that associated with new disk
    // However, there is no 2nd request out because new created histogram doesn't of enough data points.
    sendRequests(tracker3, 0);
    // make the 1st request fail
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.FAILURE);
    // 2nd request is sent
    sendRequests(tracker3, 1);
    // make the 2nd request succeed
    tracker3.onResponse(partitionAndInflightReplicas.get(mockPartition3).poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", tracker3.hasSucceeded());

    // restore the tracer scope and routerMetrics
    trackerScope = OperationTrackerScope.Datacenter;
    routerMetrics = originalMetrics;
  }

  /**
   * Tests that the {@link Histogram} instances used by {@link AdaptiveOperationTracker} are updated correctly on
   * successful requests.
   * @throws InterruptedException
   */
  @Test
  public void trackerUpdateOnSuccessTest() throws InterruptedException {
    doTrackerUpdateTest(true);
  }

  /**
   * Tests that the {@link Histogram} instances used by {@link AdaptiveOperationTracker} are updated correctly on failed
   * requests.
   * @throws InterruptedException
   */
  @Test
  public void trackerUpdateOnFailureTest() throws InterruptedException {
    doTrackerUpdateTest(false);
  }

  /**
   * Tests the case where there are no unexpired requests because the only unexpired request returned a failure. In
   * that case, the tracker must allow sending more requests.
   * @throws InterruptedException
   */
  @Test
  public void noUnexpiredRequestsTest() throws InterruptedException {
    primeTracker(localColoTracker, MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(QUANTILE);

    OperationTracker ot = getOperationTracker(createRouterConfig(false, 1, 1, 6, null, true), mockPartition);
    // 3-0-0-0
    sendRequests(ot, 1);
    // 2-1-0-0
    // sleep for a time greater than cutoff
    time.sleep((long) localColoCutoff + 2);
    sendRequests(ot, 1);
    // 1-2-0-0
    // provide a response to the second request that is not a success
    ot.onResponse(partitionAndInflightReplicas.get(mockPartition).pollLast(), TrackedRequestFinalState.FAILURE);
    // 1-1-0-1
    assertFalse("Operation should not be done", ot.isDone());
    // should now be able to send one more request
    sendRequests(ot, 1);
    // 0-2-0-1
    ot.onResponse(partitionAndInflightReplicas.get(mockPartition).pollLast(), TrackedRequestFinalState.SUCCESS);
    // 0-1-1-1
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be 1
    assertEquals("Past due counter is inconsistent", 1, pastDueCounter.getCount());
  }

  /**
   * Tests the case where the tracker is updated b/w the {@link Iterator#hasNext()} and {@link Iterator#next()} calls.
   * @throws InterruptedException
   */
  @Test
  public void trackerUpdateBetweenHasNextAndNextTest() throws InterruptedException {
    primeTracker(localColoTracker, MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(1);

    OperationTracker ot = getOperationTracker(createRouterConfig(false, 1, 1, 6, null, true), mockPartition);
    // 3-0-0-0
    sendRequests(ot, 1);
    // 2-1-0-0
    // sleep for a time greater than cutoff
    time.sleep((long) localColoCutoff + 2);

    // now get an iterator and call hasNext() on it
    Iterator<ReplicaId> replicaIterator = ot.getReplicaIterator();
    assertTrue("There should be a replica to send to", replicaIterator.hasNext());
    // now insert a value in the tracker such that it is the max value. However, the return value of hasNext() must
    // not change even though the tracker has changed its return value for getSnapshot().getValue(1).
    long valueToInsert = 2 * (long) localColoCutoff;
    localColoTracker.update(valueToInsert);
    assertEquals("Tracker's snapshot should return the max value", valueToInsert,
        (long) localColoTracker.getSnapshot().getValue(1));
    // hasNext() should not change it's return value
    assertTrue("There should be a replica to send to", replicaIterator.hasNext());

    sendRequests(ot, 1);
    // 1-2-0-0
    ot.onResponse(partitionAndInflightReplicas.get(mockPartition).pollLast(), TrackedRequestFinalState.SUCCESS);
    // 1-1-1-0
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be 1
    assertEquals("Past due counter is inconsistent", 1, pastDueCounter.getCount());
  }

  /**
   * Test that {@link NonBlockingRouterMetrics} can correctly register custom percentiles. An example of metric name is:
   * "com.github.ambry.router.GetOperation.LocalColoLatencyMs.91.0.thPercentile"
   */
  @Test
  public void customPercentilesMetricsRegistryTest() {
    // test that if custom percentile is not set, no corresponding metrics would be generated.
    MetricRegistry metricRegistry = routerMetrics.getMetricRegistry();
    MetricFilter filter = new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.endsWith("thPercentile");
      }
    };
    SortedMap<String, Gauge> gauges = metricRegistry.getGauges(filter);
    assertTrue("No gauges should be created because custom percentile is not set", gauges.isEmpty());
    // test that dedicated gauges are correctly created for custom percentiles.
    String customPercentiles = "0.91,0.97";
    RouterConfig routerConfig = createRouterConfig(false, 1, 1, 6, customPercentiles, true);
    String[] percentileArray = customPercentiles.split(",");
    Arrays.sort(percentileArray);
    List<String> sortedPercentiles = Arrays.stream(percentileArray)
        .map(p -> String.valueOf(Double.parseDouble(p) * 100))
        .collect(Collectors.toList());
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    gauges = routerMetrics.getMetricRegistry().getGauges(filter);
    // Note that each percentile creates 4 metrics (GetBlobInfo/GetBlob joins LocalColo/CrossColo). So, the total number of
    // metrics should equal to 4 * (# of given custom percentiles)
    assertEquals("The number of custom percentile gauge doesn't match", sortedPercentiles.size() * 4, gauges.size());
    Iterator mapItor = gauges.keySet().iterator();
    Iterator<String> listItor = sortedPercentiles.iterator();
    while (listItor.hasNext()) {
      String gaugeName = (String) mapItor.next();
      String percentileStr = listItor.next();
      assertTrue("The gauge name doesn't match", gaugeName.endsWith(percentileStr + ".thPercentile"));
    }
    // reset router metrics to clean up registered custom percentile metrics
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, defaultRouterConfig);
  }

  /**
   * Test that if metric scope in router config is invalid, the {@link IllegalArgumentException} is thrown explicitly.
   */
  @Test
  public void invalidOperationTrackerScopeTest() {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", localDcName);
    props.setProperty("router.operation.tracker.metric.scope", "Invalid Scope");
    props.setProperty("router.get.success.target", Integer.toString(1));
    props.setProperty("router.get.request.parallelism", Integer.toString(1));
    RouterConfig routerConfig = null;
    try {
      routerConfig = new RouterConfig(new VerifiableProperties(props));
    } catch (IllegalArgumentException e) {
      //exception is expected and set valid metric scope to instantiate routerConfig for subsequent test.
      props.setProperty("router.operation.tracker.metric.scope", "Datacenter");
      routerConfig = new RouterConfig(new VerifiableProperties(props));
    }
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    AdaptiveOperationTracker tracker =
        new AdaptiveOperationTracker(routerConfig, routerMetrics, RouterOperation.GetBlobInfoOperation, mockPartition,
            null, time);
    // test that operation tracker works correctly with default Datacenter scope
    sendRequests(tracker, 1);
    assertFalse("Operation should not be done", tracker.isDone());
    tracker.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", tracker.hasSucceeded());
    // test that no other resource-level metrics have been instantiated.
    assertTrue("Partition histogram in RouterMetrics should be empty",
        routerMetrics.getBlobInfoLocalDcResourceToLatency.isEmpty());
    assertTrue("Partition histogram in RouterMetrics should be empty",
        routerMetrics.getBlobInfoCrossDcResourceToLatency.isEmpty());
    assertTrue("Partition histogram in OperationTracker should be empty",
        tracker.getResourceToLatencyMap(RouterOperation.GetBlobInfoOperation, true).isEmpty());
    assertTrue("Partition histogram in OperationTracker should be empty",
        tracker.getResourceToLatencyMap(RouterOperation.GetBlobInfoOperation, false).isEmpty());
    // extra test: invalid router operation
    try {
      tracker.getResourceToLatencyMap(RouterOperation.PutOperation, true);
      fail("should fail due to invalid router operation");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  // helpers

  // general

  /**
   * Instantiate an adaptive operation tracker.
   * @param routerConfig the {@link RouterConfig} to use in adaptive tracker.
   * @param partitionId the {@link PartitionId} to use in adaptive tracker.
   * @return an instance of {@link AdaptiveOperationTracker} with the given parameters.
   */
  private OperationTracker getOperationTracker(RouterConfig routerConfig, PartitionId partitionId) {
    return new AdaptiveOperationTracker(routerConfig, routerMetrics, RouterOperation.GetBlobOperation, partitionId,
        null, time);
  }

  /**
   * Generate an instance of {@link RouterConfig} based on input parameters.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTarget the number of successful responses required for the operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param maxInflightNum the maximum number of inflight requests for adaptive tracker.
   * @param customPercentiles the custom percentiles to be reported. Percentiles are specified in a comma-separated
   *                          string, i.e "0.94,0.96,0.97".
   * @param excludeTimeout whether to exclude timed out requests in Histogram.
   * @return an instance of {@link RouterConfig}
   */
  private RouterConfig createRouterConfig(boolean crossColoEnabled, int successTarget, int parallelism,
      int maxInflightNum, String customPercentiles, boolean excludeTimeout) {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", localDcName);
    props.setProperty("router.get.cross.dc.enabled", Boolean.toString(crossColoEnabled));
    props.setProperty("router.get.success.target", Integer.toString(successTarget));
    props.setProperty("router.get.request.parallelism", Integer.toString(parallelism));
    props.setProperty("router.get.include.non.originating.dc.replicas", "true");
    props.setProperty("router.get.replicas.required", Integer.toString(Integer.MAX_VALUE));
    props.setProperty("router.latency.tolerance.quantile", Double.toString(QUANTILE));
    props.setProperty("router.operation.tracker.metric.scope", trackerScope.toString());
    props.setProperty("router.operation.tracker.max.inflight.requests", Integer.toString(maxInflightNum));
    props.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(excludeTimeout));
    if (customPercentiles != null) {
      props.setProperty("router.operation.tracker.custom.percentiles", customPercentiles);
    }
    return new RouterConfig(new VerifiableProperties(props));
  }

  /**
   * Updates the {@code tracker} to mimic {@code numRequests} each taking {@code latency} ms.
   * @param tracker the {@link Histogram} to update
   * @param numRequests the number of requests (data points)
   * @param latencyRange the range of latencies (in ms) to generate and record.
   */
  private void primeTracker(Histogram tracker, long numRequests, Pair<Long, Long> latencyRange) {
    for (long i = 0; i < numRequests; i++) {
      // Given latencyRange specifies boundaries of latency: low = latencyRange.getFirst(), high = latencyRange.getSecond().
      // Any randomly generated latency should fall in the range [low, high).
      long latency = Utils.getRandomLong(TestUtils.RANDOM, latencyRange.getSecond() - latencyRange.getFirst())
          + latencyRange.getFirst();
      tracker.update(latency);
    }
  }

  /**
   * Send requests to all replicas provided by the {@link OperationTracker#getReplicaIterator()}
   * @param operationTracker the {@link OperationTracker} that provides replicas.
   * @param numRequestsExpected the number of requests expected to be sent out.
   */
  private void sendRequests(OperationTracker operationTracker, int numRequestsExpected) {
    int sent = 0;
    Iterator<ReplicaId> replicaIdIterator = operationTracker.getReplicaIterator();
    while (replicaIdIterator.hasNext()) {
      ReplicaId nextReplica = replicaIdIterator.next();
      assertNotNull("There should be a replica to send a request to", nextReplica);
      assertFalse("Replica that was used for a request returned by iterator again",
          repetitionTracker.contains(nextReplica));
      LinkedList<ReplicaId> infightReplicas =
          partitionAndInflightReplicas.getOrDefault(nextReplica.getPartitionId(), new LinkedList<>());
      infightReplicas.offer(nextReplica);
      partitionAndInflightReplicas.put(nextReplica.getPartitionId(), infightReplicas);
      repetitionTracker.add(nextReplica);
      replicaIdIterator.remove();
      sent++;
    }
    assertEquals("Did not send expected number of requests", numRequestsExpected, sent);
  }

  // trackerUpdateTest() helpers

  /**
   * Tests that the {@link Histogram} instances used by {@link AdaptiveOperationTracker} are updated correctly.
   * @param succeedRequests {@code true} if the requests should receive successful responses. {@code false} otherwise.
   * @throws InterruptedException
   */
  private void doTrackerUpdateTest(boolean succeedRequests) throws InterruptedException {
    long timeIncrement = 10;
    OperationTracker ot =
        getOperationTracker(createRouterConfig(true, REPLICA_COUNT, REPLICA_COUNT, 6, null, true), mockPartition);
    // 3-0-0-0; 3-0-0-0
    sendRequests(ot, REPLICA_COUNT);
    // 0-3-0-0; 0-3-0-0
    Double[] localColoExpectedAverages =
        {(double) timeIncrement, (double) timeIncrement + timeIncrement / 2, 2 * (double) timeIncrement};
    verifyHistogramRecording(ot, succeedRequests, timeIncrement, localColoExpectedAverages, localColoTracker);
    Double[] crossColoExpectedAverages =
        {4 * (double) timeIncrement, 4 * (double) timeIncrement + timeIncrement / 2, 5 * (double) timeIncrement};
    verifyHistogramRecording(ot, succeedRequests, timeIncrement, crossColoExpectedAverages, crossColoTracker);
    assertEquals("Operation success state is unexpected", succeedRequests, ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Verifies that the {@code tracker} is updated correctly when responses are received.
   * @param ot the {@link OperationTracker} to use.
   * @param succeedRequests {@code true} if the requests should receive successful responses. {@code false} otherwise.
   * @param timeIncrement increment of time (in ms) before responses are recorded for each request.
   * @param expectedAverages the expected averages after every response that is recorded.
   * @param tracker the {@link Histogram} that should be updated.
   * @throws InterruptedException
   */
  private void verifyHistogramRecording(OperationTracker ot, boolean succeedRequests, long timeIncrement,
      Double[] expectedAverages, Histogram tracker) throws InterruptedException {
    for (double expectedAverage : expectedAverages) {
      time.sleep(timeIncrement);
      ot.onResponse(partitionAndInflightReplicas.get(mockPartition).poll(),
          succeedRequests ? TrackedRequestFinalState.SUCCESS : TrackedRequestFinalState.FAILURE);
      assertEquals("Average does not match. Histogram recording may be incorrect", expectedAverage,
          tracker.getSnapshot().getMean(), 0.001);
    }
  }
}
