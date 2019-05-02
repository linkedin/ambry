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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
 * local unsent count-local inflight count-local succeeded count-local failed count;
 * remote unsent count-remote inflight count-remote succeeded count-remote failed count
 *
 * For example: 3-0-0-0; 9-0-0-0
 */
public class AdaptiveOperationTrackerTest {
  private static final int REPLICA_COUNT = 6;
  private static final int PORT = 6666;
  private static final double QUANTILE = 0.9;
  private static final Pair<Long, Long> LOCAL_COLO_LATENCY_RANGE = new Pair<>(0L, 100L);
  private static final Pair<Long, Long> CROSS_COLO_LATENCY_RANGE = new Pair<>(120L, 220L);

  private final List<MockDataNodeId> datanodes;
  private final MockPartitionId mockPartition;
  private final String localDcName;
  private final LinkedList<ReplicaId> inflightReplicas = new LinkedList<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();
  private final Time time = new MockTime();
  private final MetricRegistry registry = new MetricRegistry();
  private final Histogram localColoTracker = registry.histogram("LocalColoTracker");
  private final Histogram crossColoTracker = registry.histogram("CrossColoTracker");
  private final Counter pastDueCounter = registry.counter("PastDueCounter");
  private NonBlockingRouterMetrics routerMetrics;

  /**
   * Constructor that sets up state.
   */
  public AdaptiveOperationTrackerTest() throws Exception {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1")));
    localDcName = datanodes.get(0).getDatacenterName();
    mockPartition = new MockPartitionId();
    for (int i = 0; i < REPLICA_COUNT; i++) {
      mockPartition.replicaIds.add(new MockReplicaId(PORT, mockPartition, datanodes.get(i % datanodes.size()), 0));
    }
    routerMetrics = new NonBlockingRouterMetrics(new MockClusterMap());
  }

  /**
   * Tests that requests are discounted from the parallelism count once they move beyond the tolerance quantile.
   * @throws InterruptedException
   */
  @Test
  public void adaptationTest() throws InterruptedException {
    primeTracker(localColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(QUANTILE);
    double crossColoCutoff = crossColoTracker.getSnapshot().getValue(QUANTILE);

    OperationTracker ot = getOperationTracker(true, REPLICA_COUNT, 2, null);
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
    // no requests should be sent
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
      ot.onResponse(inflightReplicas.poll(), true);
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be REPLICA_COUNT - 2
    assertEquals("Past due counter is inconsistent", REPLICA_COUNT - 2, pastDueCounter.getCount());
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
    primeTracker(localColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(QUANTILE);

    OperationTracker ot = getOperationTracker(false, 1, 1, null);
    // 3-0-0-0
    sendRequests(ot, 1);
    // 2-1-0-0
    // sleep for a time greater than cutoff
    time.sleep((long) localColoCutoff + 2);
    sendRequests(ot, 1);
    // 1-2-0-0
    // provide a response to the second request that is not a success
    ot.onResponse(inflightReplicas.pollLast(), false);
    // 1-1-0-1
    assertFalse("Operation should not be done", ot.isDone());
    // should now be able to send one more request
    sendRequests(ot, 1);
    // 0-2-0-1
    ot.onResponse(inflightReplicas.pollLast(), true);
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
    primeTracker(localColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, LOCAL_COLO_LATENCY_RANGE);
    primeTracker(crossColoTracker, AdaptiveOperationTracker.MIN_DATA_POINTS_REQUIRED, CROSS_COLO_LATENCY_RANGE);
    double localColoCutoff = localColoTracker.getSnapshot().getValue(1);

    OperationTracker ot = getOperationTracker(false, 1, 1, null);
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
    ot.onResponse(inflightReplicas.pollLast(), true);
    // 1-1-1-0
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    // past due counter should be 1
    assertEquals("Past due counter is inconsistent", 1, pastDueCounter.getCount());
  }

  /**
   * Test that adaptive operation track can correctly register custom percentiles. An example of metric name is:
   * "com.github.ambry.router.GetOperation.LocalColoLatencyMs.91.0.thPercentile"
   * @throws Exception
   */
  @Test
  public void customPercentilesMetricsRegistryTest() throws Exception {
    // test that if custom percentile is not set, no corresponding metrics would be generated.
    getOperationTracker(true, 1, 1, null);
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
    String[] percentileArray = customPercentiles.split(",");
    Arrays.sort(percentileArray);
    List<String> sortedPercentiles =
        Arrays.stream(percentileArray).map(p -> String.valueOf(Double.valueOf(p) * 100)).collect(Collectors.toList());
    getOperationTracker(false, 1, 1, customPercentiles);
    gauges = metricRegistry.getGauges(filter);
    // Note that crossColoEnabled is false, the number of gauges should equal to number of given percentiles
    assertEquals("The number of custom percentile gauge doesn't match", sortedPercentiles.size(), gauges.size());
    Iterator mapItor = gauges.keySet().iterator();
    Iterator<String> listItor = sortedPercentiles.iterator();
    while (listItor.hasNext()) {
      String gaugeName = (String) mapItor.next();
      String percentileStr = listItor.next();
      assertTrue("The gauge name doesn't match", gaugeName.endsWith(percentileStr + ".thPercentile"));
    }
    // reset router metrics to clean up registered metrics
    routerMetrics = new NonBlockingRouterMetrics(new MockClusterMap());
    metricRegistry = routerMetrics.getMetricRegistry();
    getOperationTracker(true, 1, 1, customPercentiles);
    gauges = metricRegistry.getGauges(filter);
    assertEquals("The number of custom percentile gauge doesn't match", sortedPercentiles.size() * 2, gauges.size());
  }

  // helpers

  // general

  /**
   * Returns an instance of {@link AdaptiveOperationTracker}.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTarget the number of successful responses required for the operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param customPercentiles the custom percentiles to be reported. Percentiles are specified in a comma-separated
   *                          string, i.e "0.94,0.96,0.97".
   * @return an instance of {@link AdaptiveOperationTracker} with the given parameters.
   */
  private OperationTracker getOperationTracker(boolean crossColoEnabled, int successTarget, int parallelism,
      String customPercentiles) {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", localDcName);
    props.setProperty("router.get.cross.dc.enabled", Boolean.toString(crossColoEnabled));
    props.setProperty("router.get.success.target", Integer.toString(successTarget));
    props.setProperty("router.get.request.parallelism", Integer.toString(parallelism));
    props.setProperty("router.get.include.non.originating.dc.replicas", "true");
    props.setProperty("router.get.replicas.required", Integer.toString(Integer.MAX_VALUE));
    props.setProperty("router.latency.tolerance.quantile", Double.toString(QUANTILE));
    if (customPercentiles != null) {
      props.setProperty("router.operation.tracker.custom.percentiles", customPercentiles);
    }
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    return new AdaptiveOperationTracker(routerConfig, RouterOperation.GetBlobOperation, mockPartition, null,
        localColoTracker, crossColoEnabled ? crossColoTracker : null, pastDueCounter, routerMetrics, time);
  }

  /**
   * Updates the {@code tracker} to mimic {@code numRequests} each taking {@code latency} ms.
   * @param tracker the {@link Histogram} to update
   * @param numRequests the number of requests (data points)
   * @param latencyRange the range of latencies (in ms) to generate and record.
   */
  private void primeTracker(Histogram tracker, long numRequests, Pair<Long, Long> latencyRange) {
    for (long i = 0; i < numRequests; i++) {
      long latency = Utils.getRandomLong(TestUtils.RANDOM, latencyRange.getSecond()) + latencyRange.getFirst();
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
      inflightReplicas.offer(nextReplica);
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
    OperationTracker ot = getOperationTracker(true, REPLICA_COUNT, REPLICA_COUNT, null);
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
      ot.onResponse(inflightReplicas.poll(), succeedRequests);
      assertEquals("Average does not match. Histogram recording may be incorrect", expectedAverage,
          tracker.getSnapshot().getMean(), 0.001);
    }
  }
}
