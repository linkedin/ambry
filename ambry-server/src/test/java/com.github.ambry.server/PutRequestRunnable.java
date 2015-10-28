package com.github.ambry.server;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.ByteBufferInputStream;
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
class PutRequestRunnable implements Runnable {

  BlockingChannel channel;
  List<BlobId> blobIds;
  byte[] data;
  byte[] usermetadata;
  BlobProperties blobProperties;
  CountDownLatch latch;

  public PutRequestRunnable(MockCluster cluster, BlockingChannel channel, int totalBlobsToPut, byte[] data,
      byte[] usermetadata, BlobProperties blobProperties, CountDownLatch latch) {
    MockClusterMap clusterMap = cluster.getClusterMap();
    this.channel = channel;
    blobIds = new ArrayList<BlobId>(totalBlobsToPut);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (int i = 0; i < totalBlobsToPut; i++) {
      int partitionIndex = new Random().nextInt(partitionIds.size());
      BlobId blobId = new BlobId(partitionIds.get(partitionIndex));
      blobIds.add(blobId);
    }
    this.data = data;
    this.usermetadata = usermetadata;
    this.blobProperties = blobProperties;
    this.latch = latch;
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < blobIds.size(); i++) {
        PutRequest putRequest =
            new PutRequest(1, "client1", blobIds.get(i), blobProperties, ByteBuffer.wrap(usermetadata),
                new ByteBufferInputStream(ByteBuffer.wrap(data)));

        channel.send(putRequest);
        InputStream putResponseStream = channel.receive().getInputStream();
        PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
        Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    } finally {
      latch.countDown();
    }
  }

  List<BlobId> getBlobIds() {
    return blobIds;
  }
}

