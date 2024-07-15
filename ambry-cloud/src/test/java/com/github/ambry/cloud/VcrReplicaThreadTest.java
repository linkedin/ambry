/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class VcrReplicaThreadTest {
  protected final VerifiableProperties properties;
  private static final Logger logger = LoggerFactory.getLogger(VcrReplicaThreadTest.class);
  private final MetricRegistry metrics;
  private final AzureMetrics azureMetrics;
  private final AzuriteUtils azuriteUtils;
  private final AzureCloudConfig azureCloudConfig;
  protected AzureCloudDestinationSync azureClient;
  protected MockClusterMap clustermap;
  public static final int NUM_NODES = 5; // Also num_replicas
  public static final int NUM_PARTITIONS = 10;
  protected ClusterMap clusterMap;
  public VcrReplicaThreadTest() throws IOException, ReflectiveOperationException {
    clusterMap = new MockClusterMap();
    azuriteUtils = new AzuriteUtils();
    Properties props = azuriteUtils.getAzuriteConnectionProperties();
    props.setProperty(CloudConfig.CLOUD_RECENT_BLOB_CACHE_LIMIT, String.valueOf(1));
    metrics = new MetricRegistry();
    azureMetrics = new AzureMetrics(metrics);
    azureClient = azuriteUtils.getAzuriteClient(props, metrics, clusterMap, null);
    properties = new VerifiableProperties(props);
    azureCloudConfig = new AzureCloudConfig(properties);
    // Create test cluster MAP
    clustermap = new MockClusterMap(false, false, NUM_NODES,
        1, NUM_PARTITIONS, true, false,
        "localhost");
  }

  HashMap<BlobId, CloudBlobMetadata> createBlob(String data, int numBlobs) {
    HashMap<BlobId, CloudBlobMetadata> blobs = new HashMap<>();
    PartitionId partitionId = clusterMap.getWritablePartitionIds(null).get(0);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    IntStream.range(0, numBlobs).forEach(i -> {
      BlobId blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID,
          accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
      HashMap<String, String> map = new HashMap<>();
      long now = System.currentTimeMillis();
      // Required fields
      map.put(CloudBlobMetadata.FIELD_ID, blobId.getID());
      map.put(CloudBlobMetadata.FIELD_PARTITION_ID, String.valueOf(blobId.getPartition().getId()));
      map.put(CloudBlobMetadata.FIELD_ACCOUNT_ID, String.valueOf(blobId.getAccountId()));
      map.put(CloudBlobMetadata.FIELD_CONTAINER_ID, String.valueOf(blobId.getContainerId()));
      map.put(CloudBlobMetadata.FIELD_SIZE, String.valueOf(data.length()));
      map.put(CloudBlobMetadata.FIELD_CREATION_TIME, String.valueOf(now));
      map.put(CloudBlobMetadata.FIELD_EXPIRATION_TIME, String.valueOf(now));
      blobs.put(blobId, CloudBlobMetadata.fromMap(map));
    });
    return blobs;
  }

  CloudBlobMetadata getBlobMetadata(BlobId blob) {
    AtomicReference<CloudBlobMetadata> md = new AtomicReference<>();
    IntStream.range(0,5).forEach(i -> {
      try {
        md.set(azureClient.getBlobMetadata(Collections.singletonList(blob)).get(blob.getID()));
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });
    return md.get();
  }

  boolean verifyConstantMetadataFields(CloudBlobMetadata original, CloudBlobMetadata received) {
    assertEquals(original.getId(), received.getId());
    assertEquals(original.getPartitionId(), received.getPartitionId());
    assertEquals(original.getAccountId(), received.getAccountId());
    assertEquals(original.getContainerId(), received.getContainerId());
    assertEquals(original.getSize(), received.getSize());
    assertEquals(original.getCreationTime(), received.getCreationTime());
    assertEquals(original.getNameSchemeVersion(), azureCloudConfig.azureNameSchemeVersion);
    return true;
  }

  /**
   * Tests thread-local metadata cache by fetching blob metadata from azure
   * @throws CloudStorageException
   */
  @Test
  public void testThreadLocalMetadataCache() throws CloudStorageException {
    String data = "hello world!";
    HashMap<BlobId, CloudBlobMetadata> blobs = createBlob(data, 5);
    int iter = 0;
    for (BlobId blob : blobs.keySet()) {
      CloudBlobMetadata metadata = blobs.get(blob);
      short version = metadata.getLifeVersion();
      // put blob
      azureClient.uploadBlob(blob, data.length(), metadata,
          new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
      assertEquals(metadata, getBlobMetadata(blob)); // same content
      assertFalse(metadata == getBlobMetadata(blob)); // but different objects
      assertFalse(getBlobMetadata(blob).isTtlUpdated());
      assertFalse(getBlobMetadata(blob).isDeleted());
      assertFalse(getBlobMetadata(blob).isUndeleted());
      CloudBlobMetadata fetchedMetadata = blobs.get(blob);

      // remove TTL
      azureClient.updateBlobExpiration(blob, Utils.Infinite_Time, null);
      verifyConstantMetadataFields(metadata, getBlobMetadata(blob));
      assertFalse(fetchedMetadata == getBlobMetadata(blob)); // cache hit
      assertTrue(getBlobMetadata(blob).isTtlUpdated());
      assertFalse(getBlobMetadata(blob).isDeleted());
      assertFalse(getBlobMetadata(blob).isUndeleted());

      // delete blob
      long now = System.currentTimeMillis();
      azureClient.deleteBlob(blob, now, (short) (version + 1), null);
      verifyConstantMetadataFields(metadata, getBlobMetadata(blob));
      assertFalse(fetchedMetadata == getBlobMetadata(blob)); // cache hit
      assertTrue(getBlobMetadata(blob).isTtlUpdated());
      assertTrue(getBlobMetadata(blob).isDeleted());
      assertFalse(getBlobMetadata(blob).isUndeleted());
      assertEquals(now, getBlobMetadata(blob).getDeletionTime());

      // undelete blob
      azureClient.undeleteBlob(blob, (short) (version + 2), null);
      verifyConstantMetadataFields(metadata, getBlobMetadata(blob));
      assertFalse(fetchedMetadata == getBlobMetadata(blob)); // cache hit
      assertTrue(getBlobMetadata(blob).isTtlUpdated());
      assertFalse(getBlobMetadata(blob).isDeleted());
      assertTrue(getBlobMetadata(blob).isUndeleted());

      // fetch metadata per blob _exactly_ once, all other calls should hit cache
      assertEquals(iter+1, azureMetrics.blobGetPropertiesSuccessRate.getCount());
      iter += 1;
    }
  }

  @Test
  public void testSelectReplicas() {
    // Give hosts a name
    AtomicInteger ai = new AtomicInteger(0);
    int Z = 'Z';
    clustermap.getDataNodes().forEach(d -> d.setHostname(String.valueOf((char)(Z - (ai.getAndIncrement() % 26)))));

    // Create a test-thread
    VcrReplicaThread rthread =
        new VcrReplicaThread("vcrReplicaThreadTest", null, clustermap,
            new AtomicInteger(0), clustermap.getDataNodes().get(0), null, null,
            null, null, false,
            clustermap.getDataNodes().get(0).getDatacenterName(), null, null,
            null, null, null, null, null,
            properties);

    // Assign replicas to test-thread
    List<PartitionId> partitions = clustermap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    Map<DataNodeId, List<RemoteReplicaInfo>> nodes = new HashMap<>();
    partitions.forEach(partition -> partition.getReplicaIds().forEach(replica -> {
      RemoteReplicaInfo rinfo =
          new RemoteReplicaInfo(replica, null, null, null, 0,
              SystemTime.getInstance(), null);
      rthread.addRemoteReplicaInfo(rinfo);
      // Group by datanode
      DataNodeId dnode = replica.getDataNodeId();
      List rlist = nodes.getOrDefault(dnode, new ArrayList<>());
      rlist.add(rinfo);
      nodes.putIfAbsent(dnode, rlist);
    }));

    // Call custom-filter. Each time its called, it picks one replica per partition per node.
    // If we call NUM_NODES, then all replicas across all nodes are covered.
    HashMap<Long, List<String>> replicas = new HashMap<>();
    IntStream.rangeClosed(1,NUM_NODES).forEach(i -> rthread.selectReplicas(nodes).forEach((dnode, rlist) -> rlist.forEach(r -> {
      long pid = r.getReplicaId().getPartitionId().getId();
      List dlist = replicas.getOrDefault(pid, new ArrayList<>());
      dlist.add(dnode.getHostname());
      replicas.putIfAbsent(pid, dlist);
    })));

    // Check that all replicas are covered, the replicas are picked in lexicographical order
    replicas.keySet().forEach(pid -> {
      List<String> dlist = replicas.get(pid);
      List<String> slist = dlist.stream().sorted().collect(Collectors.toList());
      if (dlist.size() != NUM_NODES) {
        logger.error("Insufficient replicas for partition {}, expected {} replicas, but found only {} which are {}",
            pid, NUM_NODES, dlist.size(), String.join(", ", dlist));
        Assert.assertTrue(false);
      }
      if (!slist.equals(dlist)) {
        logger.error("Replicas are not sorted for partition {}, original list = [{}], sorted list = [{}]",
            pid, String.join(", ", dlist), String.join(", ", slist));
        Assert.assertTrue(false);
      }
    });
  }

  /**
   * Tests that expected number of repl threads are created based on cpu scaling factor
   * @throws ReplicationException
   */
  @Test
  public void testNumReplThreads() throws ReplicationException {
    VcrReplicationManager manager =
        new VcrReplicationManager(properties, null, null, clustermap,
            mock(VcrClusterParticipant.class), mock(AzureCloudDestinationSync.class), null,
            mock(NetworkClientFactory.class), null, null);
    assertEquals(0, manager.getNumReplThreads(0));
    assertEquals(2, manager.getNumReplThreads(-2.5));
    assertEquals((int) (Double.valueOf(Runtime.getRuntime().availableProcessors()) * 2.5),
        manager.getNumReplThreads(2.5));
  }
}
