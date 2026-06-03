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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.UpdateReplicationPriorityAdminRequest;
import com.github.ambry.replication.PriorityEntry;
import com.github.ambry.server.ServerErrorCode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for the pure (no-I/O) logic of {@link ReplicationPriorityAdminHelper}'s replication-priority
 * operations: action resolution + the clear table, fabric-required host resolution with optional
 * {@code --servers} narrowing
 * (whole-request rejection on a bad server), the {@code maxFanoutTotal} admission guard, per-host fan-out
 * failure isolation, the set/clear JSON response shape, and the list response shape (one raw row per
 * server-returned entry, with the {@code interColo} flag from {@link PriorityEntry#isInterColo()}).
 *
 * <p>The send/parse round-trip against a live server is exercised by the server-side admin integration suite
 * (see {@code AmbryServerRequests} handling of {@code UpdateReplicationPriority}/{@code ListReplicationPriority});
 * it is intentionally not duplicated here because this test deliberately avoids standing up an embedded server.
 */
public class ReplicationPriorityAdminHelperTest {
  private MockClusterMap clusterMap;
  private List<PartitionId> partitions;

  @Before
  public void setUp() throws Exception {
    // Default ctor: 9 nodes across DC1/DC2/DC3 (3 per dc). Every default partition has a replica on every node;
    // additionally a "special" partition is hosted on only a SUBSET of nodes (3 in the local dc, 2 elsewhere),
    // which lets us exercise the per-host partition-subset fan-out below.
    clusterMap = new MockClusterMap();
    // MockClusterMap names every node "localhost" (distinguishing only by port), but --servers and the JSON
    // output key on unique hostnames (as in prod). Give each node a unique hostname so the
    // hostname-based server resolution and host-keyed JSON are exercised realistically.
    int hostIndex = 0;
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      ((MockDataNodeId) node).setHostname(node.getDatacenterName() + "-host" + hostIndex++ + ".test");
    }
    partitions = new ArrayList<>(clusterMap.getAllPartitionIds(null));
    assertFalse("Expected the mock cluster map to have partitions", partitions.isEmpty());
  }

  @After
  public void tearDown() throws Exception {
    if (clusterMap != null) {
      clusterMap.cleanup();
    }
  }

  /**
   * Asserts that {@code runnable} throws {@link IllegalArgumentException}. JUnit 4.12 (this repo's version)
   * has no {@code Assert.assertThrows}, so this provides the same affordance.
   * @param runnable the code expected to throw.
   */
  private static void assertThrowsIae(Runnable runnable) {
    try {
      runnable.run();
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  // ---- resolveAction ----

  @Test
  public void testResolveActionSet() {
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.SET,
        ReplicationPriorityAdminHelper.resolveAction(false, Collections.singletonList(partitions.get(0)), 8));
  }

  @Test
  public void testResolveActionSetEmptyPartitionsRejected() {
    // clear=false requires non-empty partitions.
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveAction(false, Collections.emptyList(), 8));
  }

  @Test
  public void testResolveActionSetBoostTooLowRejected() {
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveAction(false, Collections.singletonList(partitions.get(0)), 0));
  }

  @Test
  public void testResolveActionSetBoostTooHighRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveAction(false,
        Collections.singletonList(partitions.get(0)), UpdateReplicationPriorityAdminRequest.MAX_PRIORITY_BOOST + 1));
  }

  @Test
  public void testResolveActionSetBoostAtCapAccepted() {
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.SET, ReplicationPriorityAdminHelper.resolveAction(false,
        Collections.singletonList(partitions.get(0)), UpdateReplicationPriorityAdminRequest.MAX_PRIORITY_BOOST));
  }

  @Test
  public void testResolveActionUnset() {
    // clear=true, non-empty partitions -> UNSET.
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.UNSET,
        ReplicationPriorityAdminHelper.resolveAction(true, Collections.singletonList(partitions.get(0)), 1));
  }

  @Test
  public void testResolveActionUnsetAll() {
    // clear=true, empty partitions -> UNSET_ALL (servers requirement enforced in host resolution).
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL,
        ReplicationPriorityAdminHelper.resolveAction(true, Collections.emptyList(), 1));
  }

  @Test
  public void testResolveActionTooManyPartitionsRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveAction(false, tooManyPartitions(), 8));
  }

  @Test
  public void testResolveActionUnsetTooManyPartitionsRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveAction(true, tooManyPartitions(), 1));
  }

  // ---- resolveTargetHostPartitions: fabric REQUIRED ----

  @Test
  public void testResolveTargetHostPartitionsFabricRequired() {
    // fabric REQUIRED for SetReplicationPriority -> missing fabric is a hard error.
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.emptyList(),
            Collections.emptyList(), Collections.singletonList(partitions.get(0)),
            UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  @Test
  public void testResolveTargetHostPartitionsUnknownFabricRejected() {
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap,
            Collections.singletonList("DC-nope"), Collections.emptyList(), Collections.singletonList(partitions.get(0)),
            UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  // ---- resolveTargetHostPartitions: SET/UNSET fabric fan-out, per-host subset ----

  @Test
  public void testResolveTargetHostPartitionsFabricSetSinglePartition() {
    // SET on one default partition scoped to DC1: every DC1 host hosting it gets exactly [that partition].
    PartitionId partition = partitions.get(0);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.emptyList(), Collections.singletonList(partition),
            UpdateReplicationPriorityAdminRequest.Action.SET);
    Set<DataNodeId> expectedHosts = nodesOfPartitionInDcs(partition, Collections.singleton("DC1"));
    assertEquals(expectedHosts, hostPartitions.keySet());
    for (Map.Entry<DataNodeId, List<PartitionId>> entry : hostPartitions.entrySet()) {
      assertEquals("DC1", entry.getKey().getDatacenterName());
      assertEquals(Collections.singletonList(partition), entry.getValue());
    }
  }

  @Test
  public void testResolveTargetHostPartitionsFabricSetSendsOnlyHostedSubset() {
    // The crux of the all-or-nothing bug fix: when a partition is NOT on every node, each host must receive only
    // the subset of the requested partitions it actually hosts.
    PartitionId defaultPartition = firstDefaultPartition();
    PartitionId specialPartition = clusterMap.getSpecialPartition();
    assertNotNull("Mock cluster map must have a special partition for this test", specialPartition);

    Set<DataNodeId> defaultHostsDc2 = nodesOfPartitionInDcs(defaultPartition, Collections.singleton("DC2"));
    Set<DataNodeId> specialHostsDc2 = nodesOfPartitionInDcs(specialPartition, Collections.singleton("DC2"));
    assertTrue("Expected the special partition to be on fewer DC2 nodes than the default partition",
        specialHostsDc2.size() < defaultHostsDc2.size());

    List<PartitionId> requested = Arrays.asList(defaultPartition, specialPartition);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC2"),
            Collections.emptyList(), requested, UpdateReplicationPriorityAdminRequest.Action.SET);

    assertEquals(defaultHostsDc2, hostPartitions.keySet());
    for (Map.Entry<DataNodeId, List<PartitionId>> entry : hostPartitions.entrySet()) {
      DataNodeId host = entry.getKey();
      List<PartitionId> forHost = entry.getValue();
      if (specialHostsDc2.contains(host)) {
        assertEquals("Host carrying both partitions must receive both, in request order",
            Arrays.asList(defaultPartition, specialPartition), forHost);
      } else {
        assertEquals("Host carrying only the default partition must receive only it (not the special one)",
            Collections.singletonList(defaultPartition), forHost);
      }
    }
  }

  @Test
  public void testResolveTargetHostPartitionsFabricSetScopedToOneFabricExcludesOtherDcs() {
    // SET scoped to DC3 only: hosts in OTHER datacenters (DC1/DC2) must be excluded even though they host the
    // same partition. This is the single-fabric scoping / blast-radius guarantee.
    PartitionId partition = partitions.get(0);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC3"),
            Collections.emptyList(), Collections.singletonList(partition),
            UpdateReplicationPriorityAdminRequest.Action.SET);
    Set<DataNodeId> expectedHosts = nodesOfPartitionInDcs(partition, Collections.singleton("DC3"));
    assertEquals(expectedHosts, hostPartitions.keySet());
    for (Map.Entry<DataNodeId, List<PartitionId>> entry : hostPartitions.entrySet()) {
      assertEquals("Only DC3 hosts may be targeted", "DC3", entry.getKey().getDatacenterName());
      assertEquals(Collections.singletonList(partition), entry.getValue());
    }
  }

  @Test
  public void testResolveTargetHostPartitionsFabricUnsetSendsOnlyHostedSubset() {
    PartitionId defaultPartition = firstDefaultPartition();
    PartitionId specialPartition = clusterMap.getSpecialPartition();
    assertNotNull(specialPartition);
    Set<DataNodeId> specialHostsDc2 = nodesOfPartitionInDcs(specialPartition, Collections.singleton("DC2"));

    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC2"),
            Collections.emptyList(), Arrays.asList(defaultPartition, specialPartition),
            UpdateReplicationPriorityAdminRequest.Action.UNSET);
    for (Map.Entry<DataNodeId, List<PartitionId>> entry : hostPartitions.entrySet()) {
      if (specialHostsDc2.contains(entry.getKey())) {
        assertTrue(entry.getValue().contains(specialPartition));
      } else {
        assertFalse("A host not hosting the special partition must not be sent it",
            entry.getValue().contains(specialPartition));
      }
    }
  }

  @Test
  public void testResolveTargetHostPartitionsSetNoHostsInFabricRejected() throws Exception {
    // SET (no --servers) scoped to a fabric where NONE of the requested partitions has a replica must be rejected,
    // not silently resolve to zero hosts (a requested=0 no-op that's easy to mistake for success, e.g. a mistyped
    // --fabric). Symmetric to the --servers "hosts none of the requested partitions" rejection. A freshly added DC
    // hosts no existing partition, giving us a known-but-empty fabric for the requested partition.
    clusterMap.createNewDataNodes(1, "DC-empty");
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap,
            Collections.singletonList("DC-empty"), Collections.emptyList(),
            Collections.singletonList(partitions.get(0)), UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  // ---- clear semantics: servers narrowing + validation ----

  @Test
  public void testResolveTargetHostPartitionsServersNarrowsSet() {
    // clear=false, partitions non-empty, servers given -> restrict SET to those servers.
    PartitionId partition = partitions.get(0);
    List<DataNodeId> dc1Hosts = new ArrayList<>(nodesOfPartitionInDcs(partition, Collections.singleton("DC1")));
    DataNodeId chosen = dc1Hosts.get(0);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.singletonList(chosen.getHostname()), Collections.singletonList(partition),
            UpdateReplicationPriorityAdminRequest.Action.SET);
    assertEquals(Collections.singleton(chosen), hostPartitions.keySet());
    assertEquals(Collections.singletonList(partition), hostPartitions.get(chosen));
  }

  @Test
  public void testResolveTargetHostPartitionsServerOutOfFabricRejectsWholeRequest() {
    // a server NOT in the requested fabric rejects the WHOLE request.
    PartitionId partition = partitions.get(0);
    DataNodeId dc2Host = new ArrayList<>(nodesOfPartitionInDcs(partition, Collections.singleton("DC2"))).get(0);
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.singletonList(dc2Host.getHostname()), Collections.singletonList(partition),
            UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  @Test
  public void testResolveTargetHostPartitionsServerNotHostingPartitionRejectsWholeRequest() {
    // a server in-fabric but hosting NONE of the requested partitions rejects the WHOLE request.
    PartitionId specialPartition = clusterMap.getSpecialPartition();
    assertNotNull(specialPartition);
    Set<DataNodeId> specialHostsDc2 = nodesOfPartitionInDcs(specialPartition, Collections.singleton("DC2"));
    // Find a DC2 node that does NOT host the special partition.
    DataNodeId nonHostingDc2 = null;
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      if ("DC2".equals(node.getDatacenterName()) && !specialHostsDc2.contains(node)) {
        nonHostingDc2 = node;
        break;
      }
    }
    assertNotNull("Expected a DC2 node not hosting the special partition", nonHostingDc2);
    final DataNodeId badServer = nonHostingDc2;
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC2"),
            Collections.singletonList(badServer.getHostname()), Collections.singletonList(specialPartition),
            UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  @Test
  public void testResolveTargetHostPartitionsUnknownServerRejected() {
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.singletonList("no-such-host"), Collections.singletonList(partitions.get(0)),
            UpdateReplicationPriorityAdminRequest.Action.SET));
  }

  @Test
  public void testResolveTargetHostPartitionsUnsetAllServersRequiredEmptyLists() {
    // clear=true, partitions empty, servers non-empty -> UNSET_ALL on those servers (empty lists).
    List<DataNodeId> dc1Nodes = new ArrayList<>(allNodesInDcs(Collections.singleton("DC1")));
    DataNodeId a = dc1Nodes.get(0);
    DataNodeId b = dc1Nodes.get(1);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Arrays.asList(a.getHostname(), b.getHostname()), Collections.emptyList(),
            UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL);
    assertEquals(new HashSet<>(Arrays.asList(a, b)), hostPartitions.keySet());
    for (List<PartitionId> forHost : hostPartitions.values()) {
      assertTrue("UNSET_ALL sends an empty partition list to each host", forHost.isEmpty());
    }
  }

  @Test
  public void testResolveTargetHostPartitionsUnsetAllNoServersRejected() {
    // clear=true, partitions empty, servers empty -> REJECT.
    assertThrowsIae(
        () -> ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.emptyList(), Collections.emptyList(),
            UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL));
  }

  @Test
  public void testResolveTargetHostPartitionsClearPartitionsAndServersIntersection() {
    // clear=true, partitions non-empty, servers non-empty -> intersection (UNSET on those servers).
    PartitionId partition = partitions.get(0);
    List<DataNodeId> dc1Hosts = new ArrayList<>(nodesOfPartitionInDcs(partition, Collections.singleton("DC1")));
    DataNodeId chosen = dc1Hosts.get(0);
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        ReplicationPriorityAdminHelper.resolveTargetHostPartitions(clusterMap, Collections.singletonList("DC1"),
            Collections.singletonList(chosen.getHostname()), Collections.singletonList(partition),
            UpdateReplicationPriorityAdminRequest.Action.UNSET);
    assertEquals(Collections.singleton(chosen), hostPartitions.keySet());
    assertEquals(Collections.singletonList(partition), hostPartitions.get(chosen));
  }

  // ---- resolveTargetHosts (List path: partition-agnostic, fabric NOT required) ----

  @Test
  public void testResolveTargetHostsListNoFabricNoServersWholeCluster() {
    // list with neither fabric nor servers -> every host in the cluster.
    List<DataNodeId> hosts =
        ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap, Collections.emptyList(), Collections.emptyList());
    assertEquals(new HashSet<>(clusterMap.getDataNodeIds()), new HashSet<>(hosts));
  }

  @Test
  public void testResolveTargetHostsListFabricScoped() {
    // List scoped to a single fabric (DC1): exactly the DC1 hosts, no DC2/DC3 hosts.
    List<DataNodeId> hosts = ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap,
        Collections.singletonList("DC1"), Collections.emptyList());
    assertEquals(allNodesInDcs(Collections.singleton("DC1")), new HashSet<>(hosts));
  }

  @Test
  public void testResolveTargetHostsListServersScoped() {
    List<DataNodeId> nodes = clusterMap.getDataNodeIds();
    DataNodeId a = nodes.get(0);
    DataNodeId b = nodes.get(1);
    List<DataNodeId> hosts = ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap, Collections.emptyList(),
        Arrays.asList(a.getHostname(), b.getHostname()));
    assertEquals(Arrays.asList(a, b), hosts);
  }

  @Test
  public void testResolveTargetHostsListUnknownFabricRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap,
        Collections.singletonList("DC-nope"), Collections.emptyList()));
  }

  @Test
  public void testResolveTargetHostsListServerOutOfFabricScopeRejected() {
    DataNodeId dc2Host = null;
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      if ("DC2".equals(node.getDatacenterName())) {
        dc2Host = node;
        break;
      }
    }
    assertNotNull(dc2Host);
    final DataNodeId server = dc2Host;
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap,
        Collections.singletonList("DC1"), Collections.singletonList(server.getHostname())));
  }

  @Test
  public void testResolveTargetHostsListUnknownServerRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.resolveTargetHosts(clusterMap, Collections.emptyList(),
        Collections.singletonList("no-such-host")));
  }

  // ---- request construction: buildUpdateRequest + resolveReplicaPartition ----

  @Test
  public void testBuildUpdateRequestWrapsWithNullPartitionAndCarriesTypedBody() {
    PartitionId p0 = partitions.get(0);
    PartitionId p1 = partitions.get(1);
    List<PartitionId> ids = Arrays.asList(p0, p1);
    UpdateReplicationPriorityAdminRequest request =
        ReplicationPriorityAdminHelper.buildUpdateRequest(ids, 8, UpdateReplicationPriorityAdminRequest.Action.SET, 42);
    assertNull("Wrapping AdminRequest partition must be null", request.getPartitionId());
    assertEquals(AdminRequestOrResponseType.UpdateReplicationPriority, request.getType());
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.SET, request.getAction());
    assertEquals(8, request.getBoost());
    assertEquals(ids, request.getPartitionIds());
  }

  @Test
  public void testBuildUpdateRequestUnsetAllAction() {
    UpdateReplicationPriorityAdminRequest request =
        ReplicationPriorityAdminHelper.buildUpdateRequest(Collections.emptyList(), 1,
            UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL, 7);
    assertNull(request.getPartitionId());
    assertEquals(UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL, request.getAction());
    assertTrue(request.getPartitionIds().isEmpty());
  }

  @Test
  public void testResolveReplicaPartitionSingleReturnsThatPartition() {
    PartitionId p0 = partitions.get(0);
    assertSame(p0, ReplicationPriorityAdminHelper.resolveReplicaPartition(Collections.singletonList(p0)));
  }

  @Test
  public void testResolveReplicaPartitionMultiReturnsNull() {
    assertNull(
        ReplicationPriorityAdminHelper.resolveReplicaPartition(Arrays.asList(partitions.get(0), partitions.get(1))));
  }

  @Test
  public void testResolveReplicaPartitionEmptyReturnsNull() {
    assertNull(ReplicationPriorityAdminHelper.resolveReplicaPartition(Collections.emptyList()));
  }

  // ---- parsePartitionIds ----

  @Test
  public void testParsePartitionIdsValid() {
    String s0 = partitions.get(0).toPathString();
    String s1 = partitions.get(1).toPathString();
    List<PartitionId> parsed = ReplicationPriorityAdminHelper.parsePartitionIds(s0 + "," + s1, clusterMap);
    assertEquals(Arrays.asList(partitions.get(0), partitions.get(1)), parsed);
  }

  @Test
  public void testParsePartitionIdsDefaultEmptyConfigYieldsEmptyList() {
    assertTrue(ReplicationPriorityAdminHelper.parsePartitionIds("", clusterMap).isEmpty());
  }

  @Test
  public void testParsePartitionIdsTrimsAndSkipsBlanks() {
    String s0 = partitions.get(0).toPathString();
    // Surrounding spaces and stray/empty tokens (leading/trailing/repeated commas) must be ignored.
    List<PartitionId> parsed = ReplicationPriorityAdminHelper.parsePartitionIds(" " + s0 + " ,,  ", clusterMap);
    assertEquals(Collections.singletonList(partitions.get(0)), parsed);
  }

  @Test
  public void testParsePartitionIdsUnknownThrows() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.parsePartitionIds("999999999", clusterMap));
  }

  // ---- toFabricList ----

  @Test
  public void testToFabricListSingleFabric() {
    assertEquals(Collections.singletonList("DC1"), ReplicationPriorityAdminHelper.toFabricList("DC1"));
  }

  @Test
  public void testToFabricListDefaultEmptyConfigYieldsEmptyList() {
    assertTrue(ReplicationPriorityAdminHelper.toFabricList("").isEmpty());
  }

  @Test
  public void testToFabricListTrimsWhitespace() {
    assertEquals(Collections.singletonList("DC1"), ReplicationPriorityAdminHelper.toFabricList("  DC1 "));
  }

  @Test
  public void testToFabricListBlankAfterTrimYieldsEmptyList() {
    assertTrue(ReplicationPriorityAdminHelper.toFabricList("   ").isEmpty());
  }

  @Test
  public void testToFabricListCommaSeparatedRejected() {
    // Only one fabric may be specified per invocation; a comma-separated value is a hard error.
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.toFabricList("DC1,DC2"));
  }

  // ---- checkMaxFanoutTotal ----

  @Test
  public void testCheckMaxFanoutTotalUnderCapAccepted() {
    ReplicationPriorityAdminHelper.checkMaxFanoutTotal(10, 100, 1000);  // exactly at cap is fine
    ReplicationPriorityAdminHelper.checkMaxFanoutTotal(1, 1, 1000);
  }

  @Test
  public void testCheckMaxFanoutTotalOverCapRejected() {
    assertThrowsIae(() -> ReplicationPriorityAdminHelper.checkMaxFanoutTotal(11, 100, 1000));  // 1100 > 1000
  }

  @Test
  public void testRunListReplicationPriorityOverCapUnscopedListRejected() {
    // An unscoped (no fabric, no servers) list resolves to EVERY host in the cluster. With a cap below the
    // cluster size it must be rejected (hard error) before any host is contacted -- list is partition-agnostic,
    // so the fan-out cost is one unit per host.
    int clusterSize = clusterMap.getDataNodeIds().size();
    assertTrue("Need a multi-host cluster for this test", clusterSize > 1);
    // sender that fails the test if it is ever invoked -- the cap must short-circuit before any send.
    ReplicationPriorityAdminHelper.Sender sender = (node, partition, request) -> {
      throw new AssertionError("No host should be contacted when the fan-out cap is exceeded");
    };
    ReplicationPriorityAdminHelper helper = new ReplicationPriorityAdminHelper(clusterMap, sender);
    assertThrowsIae(() -> helper.runListReplicationPriority("", "", "", clusterSize - 1));
  }

  // ---- fanOut: per-host failure isolation + failure recording ----

  @Test
  public void testFanOutIsolatesPerHostFailuresAndRecordsErrors() {
    List<DataNodeId> hosts = new ArrayList<>(clusterMap.getDataNodeIds());
    assertTrue("Need at least 3 hosts for this test", hosts.size() >= 3);
    DataNodeId errorCodeHost = hosts.get(0);
    DataNodeId throwingHost = hosts.get(1);
    List<DataNodeId> visited = new ArrayList<>();
    // The set path's work lambda turns a non-NoError ServerErrorCode into an IOException; model that here so a
    // returned error and a thrown transport failure both funnel through fanOut's per-host isolation.
    ReplicationPriorityAdminHelper.HostWork work = host -> {
      visited.add(host);
      if (host.equals(errorCodeHost)) {
        throw new java.io.IOException("ServerErrorCode: " + ServerErrorCode.UnknownError);
      }
      if (host.equals(throwingHost)) {
        throw new java.util.concurrent.TimeoutException("simulated transport timeout");
      }
    };
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    ReplicationPriorityAdminHelper.FanOutSummary summary =
        ReplicationPriorityAdminHelper.fanOut(hosts, "SET", work, failures);
    assertEquals(hosts, visited);
    assertEquals(hosts.size(), summary.requested);
    assertEquals(hosts.size() - 2, summary.succeeded);
    assertEquals(2, summary.failed);
    // The two failing hosts are recorded with their error text; the rest are not.
    assertEquals(new HashSet<>(Arrays.asList(errorCodeHost, throwingHost)), failures.keySet());
    assertTrue(failures.get(errorCodeHost).contains("UnknownError"));
    assertTrue(failures.get(throwingHost).contains("simulated transport timeout"));
  }

  @Test
  public void testFanOutIsolatesUncheckedTransportFailure() {
    // The transport (ServerAdminTool.sendRequestGetResponse) throws an unchecked IllegalStateException for an
    // unreachable/errored host. That must be isolated to the offending host like any other per-host failure:
    // the rest of the fan-out continues and succeeds, and the down host is counted as failed with its message.
    List<DataNodeId> hosts = new ArrayList<>(clusterMap.getDataNodeIds());
    assertTrue("Need at least 3 hosts for this test", hosts.size() >= 3);
    DataNodeId downHost = hosts.get(1);
    List<DataNodeId> visited = new ArrayList<>();
    ReplicationPriorityAdminHelper.HostWork work = host -> {
      visited.add(host);
      if (host.equals(downHost)) {
        // Mirror the transport's unchecked failure for a host that is down / returned a transport error.
        throw new IllegalStateException(host.getHostname() + ": Encountered error while trying to send request");
      }
    };
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    ReplicationPriorityAdminHelper.FanOutSummary summary =
        ReplicationPriorityAdminHelper.fanOut(hosts, "SET", work, failures);
    // (a) every remaining host was still attempted (and the down host did not abort the loop).
    assertEquals(hosts, visited);
    assertEquals(hosts.size(), summary.requested);
    assertEquals(hosts.size() - 1, summary.succeeded);
    assertEquals(1, summary.failed);
    // (b) only the down host is recorded as failed, with its IllegalStateException message captured.
    assertEquals(Collections.singleton(downHost), failures.keySet());
    assertTrue(failures.get(downHost).contains("IllegalStateException"));
    assertTrue(failures.get(downHost).contains("Encountered error while trying to send request"));
  }

  @Test
  public void testFanOutVisitsEveryHostInOrderOnAllSuccess() {
    List<DataNodeId> hosts = new ArrayList<>(clusterMap.getDataNodeIds());
    assertTrue("Need at least 2 hosts for this test", hosts.size() >= 2);
    List<DataNodeId> visited = new ArrayList<>();
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    ReplicationPriorityAdminHelper.FanOutSummary summary =
        ReplicationPriorityAdminHelper.fanOut(hosts, "ListReplicationPriority", visited::add, failures);
    assertEquals(hosts, visited);
    assertEquals(hosts.size(), summary.requested);
    assertEquals(hosts.size(), summary.succeeded);
    assertEquals(0, summary.failed);
    assertTrue(failures.isEmpty());
  }

  // ---- set/clear JSON response shape ----

  @Test
  public void testBuildSetResultJsonShape() {
    List<DataNodeId> nodes = clusterMap.getDataNodeIds();
    DataNodeId ok1 = nodes.get(0);
    DataNodeId ok2 = nodes.get(1);
    DataNodeId bad = nodes.get(2);
    Map<DataNodeId, List<PartitionId>> hostPartitions = new LinkedHashMap<>();
    hostPartitions.put(ok1, Collections.singletonList(partitions.get(0)));
    hostPartitions.put(ok2, Collections.singletonList(partitions.get(0)));
    hostPartitions.put(bad, Collections.singletonList(partitions.get(0)));
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    failures.put(bad, "ServerErrorCode: BadRequest");
    ReplicationPriorityAdminHelper.FanOutSummary summary = new ReplicationPriorityAdminHelper.FanOutSummary(3, 2);

    JSONObject json = ReplicationPriorityAdminHelper.buildSetResultJson(hostPartitions.keySet(), failures, summary);
    JSONObject summaryJson = json.getJSONObject("summary");
    assertEquals(3, summaryJson.getInt("requested"));
    assertEquals(2, summaryJson.getInt("succeeded"));
    assertEquals(1, summaryJson.getInt("failed"));

    JSONArray results = json.getJSONArray("results");
    assertEquals(3, results.length());
    Map<String, JSONObject> byHost = indexByHost(results);
    assertEquals("ok", byHost.get(ok1.getHostname()).getString("status"));
    assertEquals("ok", byHost.get(ok2.getHostname()).getString("status"));
    JSONObject badJson = byHost.get(bad.getHostname());
    assertEquals("error", badJson.getString("status"));
    assertEquals("ServerErrorCode: BadRequest", badJson.getString("error"));
    assertFalse("ok hosts must not carry an error field", byHost.get(ok1.getHostname()).has("error"));
  }

  // ---- list JSON response shape (raw per-entry interColo) ----

  @Test
  public void testBuildListResultJsonInterColoFalseOnly() {
    PartitionId p = partitions.get(0);
    DataNodeId host = clusterMap.getDataNodeIds().get(0);
    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    hostEntries.put(host, Collections.singletonList(new PriorityEntry(p, 4, /*isInterColo*/ false)));
    JSONObject json = ReplicationPriorityAdminHelper.buildListResultJson(Collections.singletonList(host), hostEntries,
        Collections.emptyMap(), Collections.emptySet(), new ReplicationPriorityAdminHelper.FanOutSummary(1, 1));
    JSONArray priorities = indexByHost(json.getJSONArray("results")).get(host.getHostname()).getJSONArray("priorities");
    assertEquals(1, priorities.length());
    assertEquals(p.getId(), priorities.getJSONObject(0).getLong("partitionId"));
    assertEquals(4, priorities.getJSONObject(0).getInt("boost"));
    assertFalse(priorities.getJSONObject(0).getBoolean("interColo"));
    assertEquals(1, json.getJSONObject("summary").getInt("uniquePartitions"));
  }

  @Test
  public void testBuildListResultJsonInterColoTrueOnly() {
    PartitionId p = partitions.get(0);
    DataNodeId host = clusterMap.getDataNodeIds().get(0);
    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    hostEntries.put(host, Collections.singletonList(new PriorityEntry(p, 7, /*isInterColo*/ true)));
    JSONObject json = ReplicationPriorityAdminHelper.buildListResultJson(Collections.singletonList(host), hostEntries,
        Collections.emptyMap(), Collections.emptySet(), new ReplicationPriorityAdminHelper.FanOutSummary(1, 1));
    JSONArray priorities = indexByHost(json.getJSONArray("results")).get(host.getHostname()).getJSONArray("priorities");
    assertEquals(1, priorities.length());
    assertEquals(7, priorities.getJSONObject(0).getInt("boost"));
    assertTrue(priorities.getJSONObject(0).getBoolean("interColo"));
  }

  @Test
  public void testBuildListResultJsonPartitionWithBothIntraAndInterEntriesYieldsTwoRows() {
    // The SAME partition is returned twice by the server (one intra-colo entry, one inter-colo entry). These
    // must NOT be collapsed: both appear as their own row, and the partition counts once in uniquePartitions.
    PartitionId p = partitions.get(0);
    DataNodeId host = clusterMap.getDataNodeIds().get(0);
    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    hostEntries.put(host, Arrays.asList(
        new PriorityEntry(p, 5, /*isInterColo*/ false),
        new PriorityEntry(p, 5, /*isInterColo*/ true)));
    JSONObject json = ReplicationPriorityAdminHelper.buildListResultJson(Collections.singletonList(host), hostEntries,
        Collections.emptyMap(), Collections.emptySet(), new ReplicationPriorityAdminHelper.FanOutSummary(1, 1));
    JSONArray priorities = indexByHost(json.getJSONArray("results")).get(host.getHostname()).getJSONArray("priorities");
    assertEquals("both entries are kept as separate rows", 2, priorities.length());
    Set<Boolean> interColoSeen = new HashSet<>();
    for (int i = 0; i < priorities.length(); i++) {
      assertEquals(p.getId(), priorities.getJSONObject(i).getLong("partitionId"));
      interColoSeen.add(priorities.getJSONObject(i).getBoolean("interColo"));
    }
    assertEquals("both an interColo=false and interColo=true row present",
        new HashSet<>(Arrays.asList(false, true)), interColoSeen);
    assertEquals("partition counts once despite two rows", 1,
        json.getJSONObject("summary").getInt("uniquePartitions"));
  }

  @Test
  public void testBuildListResultJsonShapeSummaryPartitionsServersResults() {
    PartitionId p0 = partitions.get(0);
    PartitionId p1 = partitions.get(1);
    List<DataNodeId> nodes = clusterMap.getDataNodeIds();
    DataNodeId hostWithPriorities = nodes.get(0);
    DataNodeId hostEmpty = nodes.get(1);  // queried, no priorities
    DataNodeId hostError = nodes.get(2);  // errored
    List<DataNodeId> hosts = Arrays.asList(hostWithPriorities, hostEmpty, hostError);

    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    hostEntries.put(hostWithPriorities, Arrays.asList(
        new PriorityEntry(p0, 4, false),
        new PriorityEntry(p0, 4, true),
        new PriorityEntry(p1, 9, true)));   // p1 inter-only
    hostEntries.put(hostEmpty, Collections.emptyList());
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    failures.put(hostError, "IOException: timeout");
    ReplicationPriorityAdminHelper.FanOutSummary summary = new ReplicationPriorityAdminHelper.FanOutSummary(3, 2);

    JSONObject json = ReplicationPriorityAdminHelper.buildListResultJson(hosts, hostEntries, failures,
        Collections.emptySet(), summary);

    JSONObject summaryJson = json.getJSONObject("summary");
    assertEquals(3, summaryJson.getInt("queriedHosts"));
    assertEquals(1, summaryJson.getInt("hostsWithPriorities"));
    assertEquals("two distinct partition ids despite three entries", 2, summaryJson.getInt("uniquePartitions"));

    // partitions[] = dedup of DISTINCT partition ids with at least one entry.
    Set<Long> partitionIds = new HashSet<>();
    JSONArray partitionsArr = json.getJSONArray("partitions");
    for (int i = 0; i < partitionsArr.length(); i++) {
      partitionIds.add(partitionsArr.getLong(i));
    }
    assertEquals(new HashSet<>(Arrays.asList(p0.getId(), p1.getId())), partitionIds);

    // servers[] = hosts with non-empty priorities (excludes empty + errored).
    JSONArray serversArr = json.getJSONArray("servers");
    assertEquals(1, serversArr.length());
    assertEquals(hostWithPriorities.getHostname(), serversArr.getString(0));

    // results[]: per-host raw view, one row per server-returned entry.
    Map<String, JSONObject> byHost = indexByHost(json.getJSONArray("results"));
    assertEquals(3, byHost.size());
    JSONObject hostJson = byHost.get(hostWithPriorities.getHostname());
    assertEquals("ok", hostJson.getString("status"));
    JSONArray priorities = hostJson.getJSONArray("priorities");
    assertEquals("all three entries are emitted as rows", 3, priorities.length());
    // p0 contributes both an interColo=false and an interColo=true row.
    Set<Boolean> p0InterColo = new HashSet<>();
    JSONObject p1Row = null;
    for (int i = 0; i < priorities.length(); i++) {
      JSONObject row = priorities.getJSONObject(i);
      if (row.getLong("partitionId") == p0.getId()) {
        assertEquals(4, row.getInt("boost"));
        p0InterColo.add(row.getBoolean("interColo"));
      } else {
        p1Row = row;
      }
    }
    assertEquals(new HashSet<>(Arrays.asList(false, true)), p0InterColo);
    assertNotNull(p1Row);
    assertEquals(9, p1Row.getInt("boost"));
    assertTrue(p1Row.getBoolean("interColo"));

    JSONObject emptyHostJson = byHost.get(hostEmpty.getHostname());
    assertEquals("ok", emptyHostJson.getString("status"));
    assertEquals(0, emptyHostJson.getJSONArray("priorities").length());

    JSONObject errorHostJson = byHost.get(hostError.getHostname());
    assertEquals("error", errorHostJson.getString("status"));
    assertEquals("IOException: timeout", errorHostJson.getString("error"));
    assertFalse("errored host has no priorities array", errorHostJson.has("priorities"));
  }

  @Test
  public void testBuildListResultJsonPartitionFilterApplied() {
    PartitionId kept = partitions.get(0);
    PartitionId dropped = partitions.get(1);
    DataNodeId host = clusterMap.getDataNodeIds().get(0);
    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    hostEntries.put(host, Arrays.asList(new PriorityEntry(kept, 2, false), new PriorityEntry(dropped, 3, false)));
    JSONObject json = ReplicationPriorityAdminHelper.buildListResultJson(Collections.singletonList(host), hostEntries,
        Collections.emptyMap(), Collections.singleton(kept), new ReplicationPriorityAdminHelper.FanOutSummary(1, 1));
    assertEquals(1, json.getJSONObject("summary").getInt("uniquePartitions"));
    JSONArray partitionsArr = json.getJSONArray("partitions");
    assertEquals(1, partitionsArr.length());
    assertEquals(kept.getId(), partitionsArr.getLong(0));
    // The dropped partition's row must not survive the filter.
    JSONArray priorities = indexByHost(json.getJSONArray("results")).get(host.getHostname()).getJSONArray("priorities");
    assertEquals(1, priorities.length());
    assertEquals(kept.getId(), priorities.getJSONObject(0).getLong("partitionId"));
  }

  // ---- run* paths: null-response handling + replica-less node short-circuit ----

  @Test
  public void testRunSetReplicationPriorityNullResponseReportedAsActionableError() {
    // sendRequestGetResponse may return null on a network-layer failure. The operator must see an actionable
    // error for that host, NOT a bare NullPointerException (dereferencing null + a second NPE on release()).
    ReplicationPriorityAdminHelper.Sender nullSender = (node, partition, request) -> null;
    ReplicationPriorityAdminHelper helper = new ReplicationPriorityAdminHelper(clusterMap, nullSender);
    JSONObject json = captureJson(
        () -> helper.runSetReplicationPriority(partitions.get(0).toPathString(), 4, false, "DC1", "", 1000));
    assertEquals(0, json.getJSONObject("summary").getInt("succeeded"));
    JSONArray results = json.getJSONArray("results");
    assertTrue("expected at least one targeted host", results.length() > 0);
    for (int i = 0; i < results.length(); i++) {
      assertHostErrorIsNullResponseNotNpe(results.getJSONObject(i));
    }
  }

  @Test
  public void testRunListReplicationPriorityNullResponseReportedAsActionableError() {
    ReplicationPriorityAdminHelper.Sender nullSender = (node, partition, request) -> null;
    ReplicationPriorityAdminHelper helper = new ReplicationPriorityAdminHelper(clusterMap, nullSender);
    JSONObject json = captureJson(() -> helper.runListReplicationPriority("", "DC1", "", 1000));
    JSONArray results = json.getJSONArray("results");
    assertTrue("expected at least one queried host", results.length() > 0);
    for (int i = 0; i < results.length(); i++) {
      assertHostErrorIsNullResponseNotNpe(results.getJSONObject(i));
    }
  }

  @Test
  public void testRunListReplicationPriorityReplicaLessNodeReportedOkEmptyNotError() throws Exception {
    // A fresh / spare / draining node hosts no replicas, so it can hold no priorities. It must be reported as an
    // empty-but-OK result, never contacted over the network (which would route through getReplicaFromNode with no
    // replica to pick and surface a healthy node as status:"error").
    MockDataNodeId spare = clusterMap.createNewDataNodes(1, "DC-spare").get(0);
    spare.setHostname("DC-spare-host.test");
    ReplicationPriorityAdminHelper.Sender sender = (node, partition, request) -> {
      throw new AssertionError("a replica-less node must not be contacted over the network");
    };
    ReplicationPriorityAdminHelper helper = new ReplicationPriorityAdminHelper(clusterMap, sender);
    JSONObject json =
        captureJson(() -> helper.runListReplicationPriority("", "", spare.getHostname(), 1000));
    assertEquals(1, json.getJSONObject("summary").getInt("queriedHosts"));
    assertEquals(0, json.getJSONObject("summary").getInt("hostsWithPriorities"));
    JSONArray results = json.getJSONArray("results");
    assertEquals(1, results.length());
    JSONObject hostJson = results.getJSONObject(0);
    assertEquals(spare.getHostname(), hostJson.getString("host"));
    assertEquals("ok", hostJson.getString("status"));
    assertEquals(0, hostJson.getJSONArray("priorities").length());
  }

  // ---- helpers ----

  /** Asserts a results[] entry is an error whose message is the actionable null-response IOException, not an NPE. */
  private static void assertHostErrorIsNullResponseNotNpe(JSONObject hostJson) {
    assertEquals("error", hostJson.getString("status"));
    String error = hostJson.getString("error");
    assertTrue("error should surface the null-response IOException, got: " + error, error.contains("Null response"));
    assertFalse("must not surface a NullPointerException, got: " + error, error.contains("NullPointerException"));
  }

  /** Runs {@code runnable}, capturing what it writes to {@code System.out}, and parses it as a {@link JSONObject}. */
  private static JSONObject captureJson(Runnable runnable) {
    PrintStream original = System.out;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(buffer, true, "UTF-8"));
      runnable.run();
      return new JSONObject(buffer.toString("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    } finally {
      System.setOut(original);
    }
  }

  private List<PartitionId> tooManyPartitions() {
    List<PartitionId> many = new ArrayList<>();
    PartitionId p = partitions.get(0);
    for (int i = 0; i < UpdateReplicationPriorityAdminRequest.MAX_PARTITIONS_PER_REQUEST + 1; i++) {
      many.add(p);
    }
    return many;
  }

  private PartitionId firstDefaultPartition() {
    MockPartitionId special = clusterMap.getSpecialPartition();
    for (PartitionId p : partitions) {
      if (special == null || !p.equals(special)) {
        return p;
      }
    }
    throw new IllegalStateException("No default partition found");
  }

  private Set<DataNodeId> nodesOfPartitionInDcs(PartitionId partition, Set<String> dcs) {
    Set<DataNodeId> nodes = new HashSet<>();
    for (ReplicaId r : partition.getReplicaIds()) {
      DataNodeId node = r.getDataNodeId();
      if (dcs.contains(node.getDatacenterName())) {
        nodes.add(node);
      }
    }
    return nodes;
  }

  private Set<DataNodeId> allNodesInDcs(Set<String> dcs) {
    Set<DataNodeId> nodes = new HashSet<>();
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      if (dcs.contains(node.getDatacenterName())) {
        nodes.add(node);
      }
    }
    return nodes;
  }

  private static Map<String, JSONObject> indexByHost(JSONArray results) {
    Map<String, JSONObject> byHost = new LinkedHashMap<>();
    for (int i = 0; i < results.length(); i++) {
      JSONObject o = results.getJSONObject(i);
      byHost.put(o.getString("host"), o);
    }
    return byHost;
  }
}
