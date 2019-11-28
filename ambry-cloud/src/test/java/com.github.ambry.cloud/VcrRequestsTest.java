/**
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.MockFindTokenHelper;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for {@link VcrRequests}
 */
public class VcrRequestsTest {
  private final VcrRequests vcrRequests;
  private final NetworkRequest request;
  private final PartitionId availablePartitionId;
  private final Store store;

  public VcrRequestsTest() throws ReflectiveOperationException, IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    properties.setProperty("replication.no.of.intra.dc.replica.threads", "1");
    properties.setProperty("replication.no.of.inter.dc.replica.threads", "1");
    properties.setProperty("cloud.blob.crypto.agent.factory.class",
        "com.github.ambry.cloud.TestCloudBlobCryptoAgentFactory");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    DataNodeId dataNodeId = clusterMap.getDataNodeIds().get(0);
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    FindTokenHelper findTokenHelper = new MockFindTokenHelper(storeKeyFactory, replicationConfig);
    StoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    ServerMetrics serverMetrics =
        new ServerMetrics(clusterMap.getMetricRegistry(), AmbryRequests.class, this.getClass());

    CloudDestination cloudDestination = new LatchBasedInMemoryCloudDestination(Collections.emptyList());
    StoreManager storeManager =
        new CloudStorageManager(verifiableProperties, new VcrMetrics(new MetricRegistry()), cloudDestination,
            clusterMap);
    ReplicaId mockReplicaId = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    storeManager.addBlobStore(mockReplicaId);
    availablePartitionId = mockReplicaId.getPartitionId();

    vcrRequests =
        new VcrRequests(storeManager, null, clusterMap, dataNodeId, clusterMap.getMetricRegistry(), serverMetrics,
            findTokenHelper, null, null, null, false, storeKeyConverterFactory);
    store = new CloudBlobStore(verifiableProperties, availablePartitionId, cloudDestination, clusterMap,
        new VcrMetrics(new MetricRegistry()));
    request = new NetworkRequest() {
      @Override
      public InputStream getInputStream() {
        return null;
      }

      @Override
      public long getStartTimeInMs() {
        return 0;
      }
    };
  }

  /**
   * Test for {@code VcrRequests#handlePutRequest}
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void handlePutRequestTest() throws IOException, InterruptedException {
    try {
      vcrRequests.handlePutRequest(request);
      Assert.fail("handlePutRequest should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException ex) {
    }
  }

  /**
   * Test for {@code VcrRequests#handleDeleteRequest}
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void handleDeleteRequest() throws IOException, InterruptedException {
    try {
      vcrRequests.handleDeleteRequest(request);
      Assert.fail("handleDeleteRequest should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException ex) {
    }
  }

  /**
   * Test for {@code VcrRequests#handleDeleteRequest}
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void handleTtlUpdateRequest() throws IOException, InterruptedException {
    try {
      vcrRequests.handleTtlUpdateRequest(request);
      Assert.fail("handleTtlUpdateRequest should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException ex) {
    }
  }

  /**
   * Test for {@code VcrRequests#validateRequest}
   */
  @Test
  public void validateRequestTest() {
    //test for null partitionid
    Assert.assertEquals(vcrRequests.validateRequest(null, null, false), ServerErrorCode.Bad_Request);
    Assert.assertEquals(vcrRequests.validateRequest(null, null, true), ServerErrorCode.Bad_Request);

    //test for unavailable partitionid
    MockPartitionId unavailablePartitionId = new MockPartitionId();
    Assert.assertEquals(vcrRequests.validateRequest(unavailablePartitionId, null, false), ServerErrorCode.No_Error);
    Assert.assertEquals(vcrRequests.validateRequest(unavailablePartitionId, null, true), ServerErrorCode.No_Error);

    //test for available partitionid
    Assert.assertEquals(vcrRequests.validateRequest(availablePartitionId, null, false), ServerErrorCode.No_Error);
    Assert.assertEquals(vcrRequests.validateRequest(availablePartitionId, null, true), ServerErrorCode.No_Error);
  }

  /**
   * Test for {@code VcrRequests#getRemoteReplicaLag}
   */
  @Test
  public void getRemoteReplicaLagTest() {
    Assert.assertEquals(vcrRequests.getRemoteReplicaLag(null, 0), -1);
    Assert.assertEquals(vcrRequests.getRemoteReplicaLag(null, 100), -1);
    Assert.assertEquals(vcrRequests.getRemoteReplicaLag(store, 0), -1);
    Assert.assertEquals(vcrRequests.getRemoteReplicaLag(store, 100), -1);
  }
}
