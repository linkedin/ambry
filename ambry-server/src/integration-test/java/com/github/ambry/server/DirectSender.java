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
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;


/**
 * Sender for put messages directly to server nodes.
 */
class DirectSender implements Runnable {

  ConnectedChannel channel;
  List<BlobId> blobIds;
  byte[] data;
  byte[] usermetadata;
  byte[] encryptionKey;
  BlobProperties blobProperties;
  CountDownLatch endLatch;

  /**
   * Constructor for {@link DirectSender} object.
   * @param cluster {@link MockCluster} object to obtain {@link MockClusterMap} from.
   * @param channel {@link BlockingChannel} object to connect to destination.
   * @param totalBlobsToPut number of blobs to put.
   * @param data data to put in each blob.
   * @param usermetadata user metadata to put in each blob.
   * @param blobProperties {@link BlobProperties} object to add to each blob.
   * @param encryptionKey encryption key to encrypt blobs.
   * @param endLatch {@link CountDownLatch} object to signal all blobs have been uploaded.
   */
  public DirectSender(MockCluster cluster, ConnectedChannel channel, int totalBlobsToPut, byte[] data,
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

  /**
   * Constructor for {@link DirectSender} object.
   * @param channel {@link BlockingChannel} object to connect to destination.
   * @param blobIds {@link List} of {@link BlobId}s to send.
   * @param data data data to put in each blob.
   * @param usermetadata usermetadata user metadata to put in each blob.
   * @param blobProperties {@link BlobProperties} object to add to each blob.
   * @param encryptionKey encryption key to encrypt blobs.
   * @param endLatch {@link CountDownLatch} object to signal all blobs have been uploaded.
   */
  public DirectSender(ConnectedChannel channel, List<BlobId> blobIds, byte[] data, byte[] usermetadata,
      BlobProperties blobProperties, byte[] encryptionKey, CountDownLatch endLatch) {
    this.channel = channel;
    this.data = data;
    this.usermetadata = usermetadata;
    this.blobProperties = blobProperties;
    this.encryptionKey = encryptionKey;
    this.endLatch = endLatch;
    this.blobIds = blobIds;
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < blobIds.size(); i++) {
        PutRequest putRequest =
            new PutRequest(1, "client1", blobIds.get(i), blobProperties, ByteBuffer.wrap(usermetadata),
                Unpooled.wrappedBuffer(data), blobProperties.getBlobSize(), BlobType.DataBlob,
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

  /**
   * Get {@link List} of {@link BlobId}s.
   * @return {@link List} of {@link BlobId}s.
   */
  List<BlobId> getBlobIds() {
    return blobIds;
  }
}
