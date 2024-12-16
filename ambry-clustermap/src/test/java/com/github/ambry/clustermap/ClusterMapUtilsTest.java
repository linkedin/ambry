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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.ReservedMetadataIdMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class ClusterMapUtilsTest {

  /**
   * Tests for {@link ClusterMapUtils#areAllReplicasForPartitionUp(PartitionId)}.
   */
  @Test
  public void areAllReplicasForPartitionUpTest() {
    MockDataNodeId dn1 = getDataNodeId("dn1", "DC1");
    MockDataNodeId dn2 = getDataNodeId("dn2", "DC2");
    MockPartitionId partitionId = new MockPartitionId(1, "default", Arrays.asList(dn1, dn2), 0);
    MockReplicaId replicaId1 = (MockReplicaId) partitionId.getReplicaIds().get(0);
    MockReplicaId replicaId2 = (MockReplicaId) partitionId.getReplicaIds().get(1);
    assertTrue("All replicas should be up", ClusterMapUtils.areAllReplicasForPartitionUp(partitionId));
    replicaId1.markReplicaDownStatus(true);
    assertFalse("Not all replicas should be up", ClusterMapUtils.areAllReplicasForPartitionUp(partitionId));
    replicaId2.markReplicaDownStatus(true);
    assertFalse("Not all replicas should be up", ClusterMapUtils.areAllReplicasForPartitionUp(partitionId));
    replicaId1.markReplicaDownStatus(false);
    assertFalse("Not all replicas should be up", ClusterMapUtils.areAllReplicasForPartitionUp(partitionId));
    replicaId2.markReplicaDownStatus(false);
    assertTrue("All replicas should be up", ClusterMapUtils.areAllReplicasForPartitionUp(partitionId));
  }

  /**
   * Tests for all functions in {@link ClusterMapUtils.PartitionSelectionHelper}
   */
  @Test
  public void partitionSelectionHelperTest() {
    // set up partitions for tests
    // 2 partitions with 3 replicas in two datacenters "DC1" and "DC2" (class "max-replicas-all-sites")
    // 2 partitions with 3 replicas in "DC1" and 1 replica in "DC2" (class "max-local-one-remote")
    // 2 partitions with 3 replicas in "DC2" and 1 replica in "DC1" (class "max-local-one-remote")
    // minimum number of replicas required for choosing writable partition is 3.
    final String dc1 = "DC1";
    final String dc2 = "DC2";
    final String maxReplicasAllSites = "max-replicas-all-sites";
    final String maxLocalOneRemote = "max-local-one-remote";
    final int minimumLocalReplicaCount = 3;

    MockDataNodeId dc1Dn1 = getDataNodeId("dc1dn1", dc1);
    MockDataNodeId dc1Dn2 = getDataNodeId("dc1dn2", dc1);
    MockDataNodeId dc1Dn3 = getDataNodeId("dc1dn3", dc1);
    MockDataNodeId dc2Dn1 = getDataNodeId("dc2dn1", dc2);
    MockDataNodeId dc2Dn2 = getDataNodeId("dc2dn2", dc2);
    MockDataNodeId dc2Dn3 = getDataNodeId("dc2dn3", dc2);
    List<MockDataNodeId> allDataNodes = Arrays.asList(dc1Dn1, dc1Dn2, dc1Dn3, dc2Dn1, dc2Dn2, dc2Dn3);
    MockPartitionId everywhere1 = new MockPartitionId(1, maxReplicasAllSites, allDataNodes, 0);
    MockPartitionId everywhere2 = new MockPartitionId(2, maxReplicasAllSites, allDataNodes, 0);
    MockPartitionId majorDc11 =
        new MockPartitionId(3, maxLocalOneRemote, Arrays.asList(dc1Dn1, dc1Dn2, dc1Dn3, dc2Dn1), 0);
    MockPartitionId majorDc12 =
        new MockPartitionId(4, maxLocalOneRemote, Arrays.asList(dc1Dn1, dc1Dn2, dc1Dn3, dc2Dn2), 0);
    MockPartitionId majorDc21 =
        new MockPartitionId(5, maxLocalOneRemote, Arrays.asList(dc2Dn1, dc2Dn2, dc2Dn3, dc1Dn1), 0);
    MockPartitionId majorDc22 =
        new MockPartitionId(6, maxLocalOneRemote, Arrays.asList(dc2Dn1, dc2Dn2, dc2Dn3, dc1Dn2), 0);

    Collection<MockPartitionId> allPartitionIdsMain = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(everywhere1, everywhere2, majorDc11, majorDc12, majorDc21, majorDc22)));
    ClusterManagerQueryHelper mockClusterManagerQueryHelper = Mockito.mock(ClusterManagerQueryHelper.class);
    doReturn(allPartitionIdsMain).when(mockClusterManagerQueryHelper).getPartitions();
    ClusterMapUtils.PartitionSelectionHelper psh =
        new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, null, minimumLocalReplicaCount,
            maxReplicasAllSites, null);

    String[] dcsToTry = {null, "", dc1, dc2};
    for (String dc : dcsToTry) {
      Set<MockPartitionId> allPartitionIds = new HashSet<>(allPartitionIdsMain);
      resetPartitions(allPartitionIds);
      psh.updatePartitions(allPartitionIds, dc);

      // getPartitions()
      assertCollectionEquals("Partitions returned not as expected", allPartitionIds, psh.getPartitions(null));
      assertCollectionEquals("Partitions returned for " + maxReplicasAllSites + " not as expected",
          Arrays.asList(everywhere1, everywhere2), psh.getPartitions(maxReplicasAllSites));
      assertCollectionEquals("Partitions returned for " + maxLocalOneRemote + " not as expected",
          Arrays.asList(majorDc11, majorDc12, majorDc21, majorDc22), psh.getPartitions(maxLocalOneRemote));
      checkCaseInsensitivityForPartitionSelectionHelper(psh, true, maxReplicasAllSites,
          Arrays.asList(everywhere1, everywhere2));
      checkCaseInsensitivityForPartitionSelectionHelper(psh, true, maxLocalOneRemote,
          Arrays.asList(majorDc11, majorDc12, majorDc21, majorDc22));

      assertCollectionEquals("Default partition class should be used if partition class is unrecognized",
          Arrays.asList(everywhere1, everywhere2), psh.getPartitions(getRandomString(3)));

      // getWritablePartitions()
      Set<MockPartitionId> expectedWritableForMaxLocalOneRemote = null;
      MockPartitionId candidate1 = null;
      MockPartitionId candidate2 = null;
      if (dc != null) {
        switch (dc) {
          case dc1:
            candidate1 = majorDc11;
            candidate2 = majorDc12;
            expectedWritableForMaxLocalOneRemote = new HashSet<>(Arrays.asList(majorDc11, majorDc12));
            break;
          case dc2:
            candidate1 = majorDc21;
            candidate2 = majorDc22;
            expectedWritableForMaxLocalOneRemote = new HashSet<>(Arrays.asList(majorDc21, majorDc22));
            break;
        }
      }
      // invalid class should fall back to default partition class
      verifyGetWritablePartition(psh, allPartitionIds, getRandomString(3),
          new HashSet<>(Arrays.asList(everywhere1, everywhere2)), dc);
      verifyGetRandomWritablePartition(psh, allPartitionIds, getRandomString(3),
          new HashSet<>(Arrays.asList(everywhere1, everywhere2)), dc);

      verifyWritablePartitionsReturned(psh, allPartitionIds, maxReplicasAllSites, everywhere1, everywhere2,
          maxLocalOneRemote, expectedWritableForMaxLocalOneRemote, dc);
      if (candidate1 != null && candidate2 != null) {
        verifyWritablePartitionsReturned(psh, allPartitionIds, maxLocalOneRemote, candidate1, candidate2,
            maxReplicasAllSites, new HashSet<>(Arrays.asList(everywhere1, everywhere2)), dc);
      }
    }
    // additional test: ensure getRandomWritablePartition now honors replica state for PUT request
    psh = new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, dc1, minimumLocalReplicaCount,
        maxReplicasAllSites, null);
    ReplicaId replicaId = everywhere1.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals(dc1))
        .findFirst()
        .get();
    // make one of local replicas OFFLINE for everywhere1
    everywhere1.setReplicaState(replicaId, ReplicaState.OFFLINE);
    // remove some partitions that have 3 eligible replicas in DC1
    assertEquals("The partition selection helper should choose everywhere2", everywhere2,
        psh.getRandomWritablePartition(maxReplicasAllSites, null));
  }

  /**
   * Test partition with different number of replicas in local datacenter.
   */
  @Test
  public void partitionWithDifferentReplicaCntTest() {
    // set up partitions in local dc:
    // partition1 has 2 replicas; partition2 has 3 replicas; partition3 has 4 replicas
    final String dc1 = "DC1";
    final String partitionClass = "default-partition-class";
    List<MockDataNodeId> dataNodeIdList = new ArrayList<>();
    for (int i = 1; i <= 4; ++i) {
      dataNodeIdList.add(getDataNodeId("node" + i, dc1));
    }
    MockPartitionId partition1 = new MockPartitionId(1, partitionClass, dataNodeIdList.subList(0, 2), 0);
    MockPartitionId partition2 = new MockPartitionId(2, partitionClass, dataNodeIdList.subList(0, 3), 0);
    MockPartitionId partition3 = new MockPartitionId(3, partitionClass, dataNodeIdList.subList(0, 4), 0);
    List<MockPartitionId> allPartitions = Arrays.asList(partition1, partition2, partition3);
    ClusterManagerQueryHelper mockClusterManagerQueryHelper = Mockito.mock(ClusterManagerQueryHelper.class);
    doReturn(allPartitions).when(mockClusterManagerQueryHelper).getPartitions();
    int minimumLocalReplicaCount = 3;
    ClusterMapUtils.PartitionSelectionHelper psh =
        new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, dc1, minimumLocalReplicaCount,
            partitionClass, null);
    // verify get all partitions return correct result
    assertEquals("Returned partitions are not expected", allPartitions, psh.getPartitions(null));
    // verify get writable partitions return partition2 and partition3 only
    assertEquals("Returned writable partitions are not expected", Arrays.asList(partition2, partition3),
        psh.getWritablePartitions(partitionClass));
    assertNotSame("Get random writable partition shouldn't return partition1", partition1,
        psh.getRandomWritablePartition(partitionClass, null));

    // create another partition selection helper with minimumLocalReplicaCount = 4
    minimumLocalReplicaCount = 4;
    psh = new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, dc1, minimumLocalReplicaCount,
        partitionClass, null);
    assertEquals("Returned writable partitions are not expected", Arrays.asList(partition3),
        psh.getWritablePartitions(partitionClass));
    assertEquals("Get random writable partition should return partition3 only", partition3,
        psh.getRandomWritablePartition(partitionClass, null));
  }

  /**
   * Test {@link ClusterMapUtils#partitionStateToStr(PartitionState)}.
   */
  @Test
  public void testPartitionStateToStr() {
    assertEquals(ClusterMapUtils.READ_ONLY_STR, ClusterMapUtils.partitionStateToStr(PartitionState.READ_ONLY));
    assertEquals(ClusterMapUtils.READ_WRITE_STR, ClusterMapUtils.partitionStateToStr(PartitionState.READ_WRITE));
    assertEquals(ClusterMapUtils.PARTIAL_READ_WRITE_STR,
        ClusterMapUtils.partitionStateToStr(PartitionState.PARTIAL_READ_WRITE));
  }

  /**
   * Test {@link ClusterMapUtils#partitionStateStrToReplicaSealStatus(String)}.
   */
  @Test
  public void testPartitionStateStrToReplicaSealStatus() {
    assertEquals(ReplicaSealStatus.NOT_SEALED,
        ClusterMapUtils.partitionStateStrToReplicaSealStatus(ClusterMapUtils.READ_WRITE_STR));
    assertEquals(ReplicaSealStatus.PARTIALLY_SEALED,
        ClusterMapUtils.partitionStateStrToReplicaSealStatus(ClusterMapUtils.PARTIAL_READ_WRITE_STR));
    assertEquals(ReplicaSealStatus.SEALED,
        ClusterMapUtils.partitionStateStrToReplicaSealStatus(ClusterMapUtils.READ_ONLY_STR));
    try {
      ClusterMapUtils.partitionStateStrToReplicaSealStatus("garbage");
    } catch (IllegalArgumentException ex) {
      return;
    }
    fail("partitionStateStrToReplicaSealStatus with invalid argument should have failed.");
  }

  /**
   * Tests for {@link ClusterMapUtils#reserveMetadataBlobId}.
   * @throws Exception in case of any errors.
   */
  @Test
  public void testReserveMetadataBlobId() throws Exception {
    String partitionClass = MockClusterMap.SPECIAL_PARTITION_CLASS;
    String invalidPartitionClass = "test";
    List<PartitionId> partitionsToExclude = null;
    ReservedMetadataIdMetrics reservedMetadataIdMetrics =
        ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(new MetricRegistry());
    ClusterMap clusterMap = new MockClusterMap();
    short accountId = 1;
    short containerId = 1;
    boolean isEncrypted = false;
    Properties properties = new Properties();
    properties.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    properties.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, "DEV");
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(properties));

    // test a simple success case.
    BlobId blobId =
        ClusterMapUtils.reserveMetadataBlobId(partitionClass, partitionsToExclude, reservedMetadataIdMetrics,
            clusterMap, accountId, containerId, isEncrypted, routerConfig);
    assertNotNull(blobId);
    assertEquals(partitionClass, blobId.getPartition().getPartitionClass());
    assertEquals(0, reservedMetadataIdMetrics.numFailedPartitionReserveAttempts.getCount());
    assertEquals(0, reservedMetadataIdMetrics.numUnexpectedReservedPartitionClassCount.getCount());

    // test for non-existent partition class.
    blobId =
        ClusterMapUtils.reserveMetadataBlobId(invalidPartitionClass, partitionsToExclude, reservedMetadataIdMetrics,
            clusterMap, accountId, containerId, isEncrypted, routerConfig);
    assertNotNull(blobId);
    assertNotEquals(partitionClass, blobId.getPartition().getPartitionClass());
    assertEquals(MockClusterMap.DEFAULT_PARTITION_CLASS, blobId.getPartition().getPartitionClass());
    assertEquals(0, reservedMetadataIdMetrics.numFailedPartitionReserveAttempts.getCount());
    assertEquals(1, reservedMetadataIdMetrics.numUnexpectedReservedPartitionClassCount.getCount());

    // test when no partitions were selected from cluster map
    List<PartitionId> partitionIdList = clusterMap.getAllPartitionIds(null).stream().map(p -> ((PartitionId) p)).collect(
        Collectors.toList());
    assertNull(ClusterMapUtils.reserveMetadataBlobId(partitionClass, partitionIdList, reservedMetadataIdMetrics,
        clusterMap, accountId, containerId, isEncrypted, routerConfig));
    assertEquals(1, reservedMetadataIdMetrics.numFailedPartitionReserveAttempts.getCount());
    assertEquals(1, reservedMetadataIdMetrics.numUnexpectedReservedPartitionClassCount.getCount());
  }

  /**
   * Tests for {@link ClusterMapUtils#getPartitionClass}.
   */
  @Test
  public void testGetPartitionClass() {
    Container container = Mockito.mock(Container.class);
    Account account = Mockito.mock(Account.class);

    // test when account is null but container is not.
    assertEquals("test", ClusterMapUtils.getPartitionClass(null, container, "test"));

    // test when account is null and container is null.
    assertEquals("test", ClusterMapUtils.getPartitionClass(null, null, "test"));

    // test when container has empty replication policy.
    when(container.getReplicationPolicy()).thenReturn("");
    assertEquals("test", ClusterMapUtils.getPartitionClass(account, container, "test"));

    // test when container has a valid replication policy.
    when(container.getReplicationPolicy()).thenReturn("test123");
    assertEquals("test123", ClusterMapUtils.getPartitionClass(account, container, "test"));
  }


    /**
     * @param hostname the host name of the {@link MockDataNodeId}.
     * @param dc the name of the dc of the {@link MockDataNodeId}.
     * @return a {@link MockDataNodeId} based on {@code hostname} and {@code dc}.
     */
  private MockDataNodeId getDataNodeId(String hostname, String dc) {
    return new MockDataNodeId(hostname, Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
        Collections.singletonList("/tmp"), dc);
  }

  /**
   * Resets all partitions by marking them {@link PartitionState#READ_WRITE} and marking all replicas as up.
   * @param toReset all the partition ids to reset.
   */
  private void resetPartitions(Collection<MockPartitionId> toReset) {
    for (MockPartitionId partitionId : toReset) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        ((MockReplicaId) replicaId).markReplicaDownStatus(false);
        ((MockReplicaId) replicaId).setReplicaSealStatus(ReplicaSealStatus.NOT_SEALED);
      }
    }
  }

  /**
   * Asserts that elements in collection {@code actual} is equal to the ones in {@code expected} irrespective of
   * ordering.
   * @param message the message to print if they are not equal.
   * @param expected the expected elements
   * @param actual the actual elements
   */
  private void assertCollectionEquals(String message, Collection<? extends PartitionId> expected,
      Collection<? extends PartitionId> actual) {
    assertEquals(message, new HashSet<>(expected), new HashSet<>(actual));
  }

  /**
   * Asserts that element {@code actual} is equal to the one of the elements in {@code expected} collection.
   * @param message the message to print if not found.
   * @param expected the expected elements
   * @param actual the actual element
   */
  private void assertInCollection(String message, Collection<? extends PartitionId> expected, PartitionId actual) {
    if (expected != null) {
      assertTrue(message, expected.contains(actual));
    } else {
      assertEquals(message, null, expected);
    }
  }

  /**
   * Checks that the {@code partitionClass} is treated in a case insensitive way.
   * @param psh the {@link ClusterMapUtils.PartitionSelectionHelper} to use.
   * @param allPartitions if {@code true}, calls {@link ClusterMapUtils.PartitionSelectionHelper#getPartitions(String)}.
   *                      If {@code false}, calls
   *                      calls {@link ClusterMapUtils.PartitionSelectionHelper#getWritablePartitions(String)}.
   * @param partitionClass the partition class to test against
   * @param expected the expected partitions to be returned for the partition class
   */
  private void checkCaseInsensitivityForPartitionSelectionHelper(ClusterMapUtils.PartitionSelectionHelper psh,
      boolean allPartitions, String partitionClass, Collection<? extends PartitionId> expected) {
    // case insensitivity check
    int halfwayPoint = partitionClass.length() / 2;
    String mixedCaseName =
        partitionClass.substring(0, halfwayPoint).toLowerCase() + partitionClass.substring(halfwayPoint).toUpperCase();
    String[] classesToTry = {partitionClass, partitionClass.toLowerCase(), partitionClass.toUpperCase(), mixedCaseName};
    for (String classToTry : classesToTry) {
      assertCollectionEquals("Partitions returned for " + classToTry + " not as expected", expected,
          allPartitions ? psh.getPartitions(classToTry) : psh.getWritablePartitions(classToTry));
    }
  }

  /**
   * Verifies that the values returned for
   * {@link ClusterMapUtils.PartitionSelectionHelper#getWritablePartitions(String)} is correct.
   * @param psh the {@link ClusterMapUtils.PartitionSelectionHelper} instance to use.
   * @param allPartitionIds all the partitions that are in clustermap
   * @param classBeingTested the partition class being tested
   * @param expectedReturnForClassBeingTested the list of partitions that can expected to be returned for
   *                                             {@code classBeingTested}.
   * @param localDc the local dc name.
   */
  private void verifyGetWritablePartition(ClusterMapUtils.PartitionSelectionHelper psh,
      Set<MockPartitionId> allPartitionIds, String classBeingTested,
      Set<MockPartitionId> expectedReturnForClassBeingTested, String localDc) {
    assertCollectionEquals("Partitions returned not as expected", allPartitionIds, psh.getWritablePartitions(null));
    if (localDc == null || localDc.isEmpty()) {
      assertCollectionEquals("Partitions returned not as expected", Collections.emptyList(),
          psh.getWritablePartitions(classBeingTested));
    } else {
      assertCollectionEquals("Partitions returned not as expected", expectedReturnForClassBeingTested,
          psh.getWritablePartitions(classBeingTested));
    }
  }

  /**
   * Verifies that the values returned for
   * {@link ClusterMapUtils.PartitionSelectionHelper#getRandomWritablePartition(String, List)} is correct.
   * @param psh the {@link ClusterMapUtils.PartitionSelectionHelper} instance to use.
   * @param allPartitionIds all the partitions that are in clustermap
   * @param classBeingTested the partition class being tested
   * @param expectedReturnForClassBeingTested the list of partitions that can expected to be returned for
   *                                             {@code classBeingTested}.
   * @param localDc the local dc name.
   */
  private void verifyGetRandomWritablePartition(ClusterMapUtils.PartitionSelectionHelper psh,
      Set<MockPartitionId> allPartitionIds, String classBeingTested,
      Set<MockPartitionId> expectedReturnForClassBeingTested, String localDc) {
    assertInCollection("Random partition returned not as expected", allPartitionIds,
        psh.getRandomWritablePartition(null, null));
    if (localDc == null || localDc.isEmpty()) {
      assertNull("Partitions returned not as expected", psh.getRandomWritablePartition(classBeingTested, null));
    } else {
      assertInCollection("Random partition returned not as expected", expectedReturnForClassBeingTested,
          psh.getRandomWritablePartition(classBeingTested, null));
    }
  }

  /**
   * Verifies that the values returned {@link ClusterMapUtils.PartitionSelectionHelper#getWritablePartitions(String)} and
   *  {@link ClusterMapUtils.PartitionSelectionHelper#getRandomWritablePartition(String, List)}  is correct.
   * @param psh the {@link ClusterMapUtils.PartitionSelectionHelper} instance to use.
   * @param allPartitionIds all the partitions that are in clustermap
   * @param classBeingTested the partition class being tested
   * @param testedPart1 a partition in {@code classBeingTested}
   * @param testedPart2 another partition in {@code classBeingTested}
   * @param classNotBeingTested a partition class is not being tested (to check that changes to partitions in
   * {@code classBeingTested} aren't affected).
   * @param expectedReturnForClassNotBeingTested the list of partitions that can expected to be returned for
   *                                             {@code classNotBeingTested}.
   * @param localDc the local dc name.
   */
  private void verifyWritablePartitionsReturned(ClusterMapUtils.PartitionSelectionHelper psh,
      Set<MockPartitionId> allPartitionIds, String classBeingTested, MockPartitionId testedPart1,
      MockPartitionId testedPart2, String classNotBeingTested,
      Set<MockPartitionId> expectedReturnForClassNotBeingTested, String localDc) {
    Set<MockPartitionId> expectedReturnForClassBeingTested = new HashSet<>(Arrays.asList(testedPart1, testedPart2));
    // no problematic scenarios
    verifyGetWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested, localDc);
    verifyGetRandomWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested,
        localDc);

    //verify excluded partitions behavior in getRandomPartition
    assertNull(psh.getRandomWritablePartition(null, new ArrayList<>(allPartitionIds)));

    if (localDc != null && !localDc.isEmpty()) {
      checkCaseInsensitivityForPartitionSelectionHelper(psh, false, classBeingTested,
          expectedReturnForClassBeingTested);
    }

    // one replica of one partition of "classBeingTested" down
    ((MockReplicaId) testedPart1.getReplicaIds().get(0)).markReplicaDownStatus(true);
    allPartitionIds.remove(testedPart1);
    expectedReturnForClassBeingTested.remove(testedPart1);
    verifyGetWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested, localDc);
    //for getRandomWritablePartition one replica being down doesnt change anything unless its a local replica
    expectedReturnForClassBeingTested.add(testedPart1);
    verifyGetRandomWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested,
        localDc);

    // one replica of other partition of "classBeingTested" down too
    ((MockReplicaId) testedPart2.getReplicaIds().get(0)).markReplicaDownStatus(true);
    allPartitionIds.remove(testedPart2);
    // if both have a replica down, then even though both are unhealthy, they are both returned.
    expectedReturnForClassBeingTested.add(testedPart1);
    verifyGetWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested, localDc);
    verifyGetRandomWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested,
        localDc);

    if (expectedReturnForClassNotBeingTested != null) {
      assertCollectionEquals("Partitions returned not as expected", expectedReturnForClassNotBeingTested,
          psh.getWritablePartitions(classNotBeingTested));
    }

    ((MockReplicaId) testedPart1.getReplicaIds().get(0)).markReplicaDownStatus(false);
    ((MockReplicaId) testedPart2.getReplicaIds().get(0)).markReplicaDownStatus(false);
    allPartitionIds.add(testedPart1);
    allPartitionIds.add(testedPart2);

    // one partition of "classBeingTested" is READ_ONLY
    ((MockReplicaId) testedPart1.getReplicaIds().get(0)).setReplicaSealStatus(ReplicaSealStatus.SEALED);
    allPartitionIds.remove(testedPart1);
    expectedReturnForClassBeingTested.remove(testedPart1);
    verifyGetWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested, localDc);
    verifyGetRandomWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested,
        localDc);

    // all READ_ONLY
    ((MockReplicaId) testedPart2.getReplicaIds().get(0)).setReplicaSealStatus(ReplicaSealStatus.SEALED);
    allPartitionIds.remove(testedPart2);
    expectedReturnForClassBeingTested.remove(testedPart2);
    verifyGetWritablePartition(psh, allPartitionIds, classBeingTested, expectedReturnForClassBeingTested, localDc);
    verifyGetRandomWritablePartition(psh, allPartitionIds, classBeingTested, null, localDc);

    if (expectedReturnForClassNotBeingTested != null) {
      assertCollectionEquals("Partitions returned not as expected", expectedReturnForClassNotBeingTested,
          psh.getWritablePartitions(classNotBeingTested));
    }

    //cleanup the cluster map
    ((MockReplicaId) testedPart1.getReplicaIds().get(0)).setReplicaSealStatus(ReplicaSealStatus.NOT_SEALED);
    ((MockReplicaId) testedPart2.getReplicaIds().get(0)).setReplicaSealStatus(ReplicaSealStatus.NOT_SEALED);
    allPartitionIds.add(testedPart1);
    allPartitionIds.add(testedPart2);
    expectedReturnForClassBeingTested.add(testedPart1);
    expectedReturnForClassBeingTested.remove(testedPart2);
  }
}
