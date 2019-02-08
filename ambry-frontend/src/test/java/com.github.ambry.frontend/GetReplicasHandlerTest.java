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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link GetReplicasHandler}
 */
public class GetReplicasHandlerTest {
  private static final ClusterMap CLUSTER_MAP;
  private static final GetReplicasHandler getReplicasHandler;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      getReplicasHandler = new GetReplicasHandler(new FrontendMetrics(new MetricRegistry()), CLUSTER_MAP);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Tests {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)}
   * <p/>
   * For the each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created. The replica list returned
   * from {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)}is checked for equality against a locally
   * obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasTest() throws Exception {
    List<? extends PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
          partitionId, false, BlobId.BlobDataType.DATACHUNK);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = getReplicasHandler.getReplicas(blobId.getID(), restResponseChannel);
      assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
      assertEquals("Unexpected Content-Type", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      String returnedReplicasStr = RestTestUtils.getJsonizedResponseBody(channel)
          .get(GetReplicasHandler.REPLICAS_KEY)
          .toString()
          .replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests reactions of the {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)} operation to bad input -
   * specifically if we do not include required parameters.
   */
  @Test
  public void getReplicasWithBadInputTest() {
    // bad input - invalid blob id.
    try {
      getReplicasHandler.getReplicas("12345", new MockRestResponseChannel());
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }

    // bad input - invalid blob id for this cluster map.
    String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
    try {
      getReplicasHandler.getReplicas(blobId, new MockRestResponseChannel());
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }
  }
}
