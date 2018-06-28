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
package com.github.ambry.server;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;


/**
 *
 */
class DirectSender implements Runnable {

  BlockingChannel channel;
  List<BlobId> blobIds;
  byte[] data;
  byte[] usermetadata;
  byte[] encryptionKey;
  BlobProperties blobProperties;
  CountDownLatch endLatch;

  public DirectSender(MockCluster cluster, BlockingChannel channel, int totalBlobsToPut, byte[] data,
      byte[] usermetadata, BlobProperties blobProperties, byte[] encryptionKey, CountDownLatch endLatch) {
    MockClusterMap clusterMap = cluster.getClusterMap();
    this.channel = channel;
    blobIds = new ArrayList<>(totalBlobsToPut);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    for (int i = 0; i < totalBlobsToPut; i++) {
      int partitionIndex = new Random().nextInt(partitionIds.size());
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
          partitionIds.get(partitionIndex), false, BlobId.BlobDataType.DATACHUNK);
      blobIds.add(blobId);
    }
    this.data = data;
    this.usermetadata = usermetadata;
    this.blobProperties = blobProperties;
    this.encryptionKey = encryptionKey;
    this.endLatch = endLatch;
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < blobIds.size(); i++) {
        PutRequest putRequest =
            new PutRequest(1, "client1", blobIds.get(i), blobProperties, ByteBuffer.wrap(usermetadata),
                ByteBuffer.wrap(data), blobProperties.getBlobSize(), BlobType.DataBlob,
                encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);

        channel.send(putRequest);
        InputStream putResponseStream = channel.receive().getInputStream();
        PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
        Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    } finally {
      endLatch.countDown();
    }
  }

  List<BlobId> getBlobIds() {
    return blobIds;
  }
}

