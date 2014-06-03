package com.github.ambry.server;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


public class ServerTest {

  private MockCluster cluster;

  public ServerTest()
      throws InterruptedException, IOException, StoreException, InstantiationException {
    cluster = new MockCluster();
  }

  @After
  public void cleanup() {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("About to invoke cluster.cleanup()");
    cluster.cleanup();
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void startStopTest() {
    // do nothing
  }

  @Test
  public void endToEndTest()
      throws InterruptedException, IOException {

    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      BlobProperties properties = new BlobProperties(31870, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      BlobId blobId1 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId2 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId3 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      // put blob 1
      PutRequest putRequest = new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel.send(putRequest2);
      putResponseStream = channel.receive();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel.send(putRequest3);
      putResponseStream = channel.receive();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIdAt(0);
      ids.add(blobId1);
      GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest1);
      InputStream stream = channel.receive();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get user metadata
      GetRequest getRequest2 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partition, ids);
      channel.send(getRequest2);
      stream = channel.receive();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(getCoordinatorProperties(), clusterMap);
        coordinator.start();
        BlobOutput output = coordinator.getBlob(blobId1.toString());
        Assert.assertEquals(output.getSize(), 31870);
        byte[] dataOutputStream = new byte[(int) output.getSize()];
        output.getStream().read(dataOutputStream);
        Assert.assertArrayEquals(dataOutputStream, data);
        coordinator.shutdown();
      } catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIdAt(0);
      ids.add(new BlobId(partition));
      GetRequest getRequest4 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest4);
      stream = channel.receive();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getError(), ServerErrorCode.Blob_Not_Found);
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  @Test
  public void endToEndReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException {

    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[1000];
      BlobProperties properties = new BlobProperties(1000, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      BlobId blobId1 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId2 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId3 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId4 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId5 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId6 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId7 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId8 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId9 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId10 = new BlobId(clusterMap.getWritablePartitionIdAt(0));
      BlobId blobId11 = new BlobId(clusterMap.getWritablePartitionIdAt(0));

      // put blob 1
      PutRequest putRequest = new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      BlockingChannel channel1 = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      BlockingChannel channel2 = new BlockingChannel("localhost", 6668, 10000, 10000, 10000);
      BlockingChannel channel3 = new BlockingChannel("localhost", 6669, 10000, 10000, 10000);

      channel1.connect();
      channel2.connect();
      channel3.connect();
      channel1.send(putRequest);
      InputStream putResponseStream = channel1.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 4
      putRequest = new PutRequest(1, "client1", blobId4, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel1.send(putRequest);
      putResponseStream = channel1.receive();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 5
      putRequest2 = new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 6
      putRequest3 = new PutRequest(1, "client1", blobId6, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // wait till replication can complete
      Thread.sleep(4000);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIdAt(0);
      ids.add(blobId3);
      GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel2.send(getRequest1);
      InputStream stream = channel2.receive();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp1.getError(), ServerErrorCode.No_Error);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 1000);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get user metadata
      ids.clear();
      ids.add(blobId2);
      GetRequest getRequest2 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partition, ids);
      channel1.send(getRequest2);
      stream = channel1.receive();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp2.getError(), ServerErrorCode.No_Error);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get blob
      ids.clear();
      ids.add(blobId1);
      GetRequest getRequest3 = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partition, ids);
      channel3.send(getRequest3);
      stream = channel3.receive();
      GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      //System.out.println("response from get " + resp3.getError());
      try {
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp3.getInputStream());
        byte[] blobout = new byte[(int) blobOutput.getSize()];
        int readsize = 0;
        while (readsize < blobOutput.getSize()) {
          readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
        }
        Assert.assertArrayEquals(blobout, data);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(getCoordinatorProperties(), clusterMap);
        coordinator.start();
        checkBlobId(coordinator, blobId1, data);
        checkBlobId(coordinator, blobId2, data);
        checkBlobId(coordinator, blobId3, data);
        checkBlobId(coordinator, blobId4, data);
        checkBlobId(coordinator, blobId5, data);
        checkBlobId(coordinator, blobId6, data);

        coordinator.shutdown();
      } catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIdAt(0);
      ids.add(new BlobId(partition));
      GetRequest getRequest4 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel3.send(getRequest4);
      stream = channel3.receive();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getError(), ServerErrorCode.Blob_Not_Found);

      // delete a blob and ensure it is propagated
      DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobId1);
      channel1.send(deleteRequest);
      InputStream deleteResponseStream = channel1.receive();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
      Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

      Thread.sleep(1000);

      ids = new ArrayList<BlobId>();
      ids.add(blobId1);
      GetRequest getRequest5 = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partition, ids);
      channel3.send(getRequest5);
      stream = channel3.receive();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp5.getError(), ServerErrorCode.Blob_Deleted);

      // persist and restore to check state

      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();

      // read the replica file and check correctness
      DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", 6667);
      List<String> mountPaths = ((MockDataNodeId) dataNodeId).getMountPaths();
      Set<String> setToCheck = new HashSet<String>();

      // we should have an entry for each partition - remote replica pair
      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      for (ReplicaId replicaId : replicaIds) {
        List<ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        for (ReplicaId peerReplica : peerReplicas) {
          setToCheck.add(replicaId.getPartitionId().toString() +
              peerReplica.getDataNodeId().getHostname() +
              peerReplica.getDataNodeId().getPort());
        }
      }
      for (String mountPath : mountPaths) {
        File replicaTokenFile = new File(mountPath, "replicaTokens");
        if (replicaTokenFile.exists()) {
          CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
          DataInputStream dataInputStream = new DataInputStream(crcStream);
          try {
            short version = dataInputStream.readShort();
            Assert.assertEquals(version, 0);
            StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.shared.BlobIdFactory", clusterMap);
            FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

            System.out.println("setToCheck" + setToCheck.size());
            while (dataInputStream.available() > 8) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(dataInputStream);
              // read remote node host name
              String hostname = Utils.readIntString(dataInputStream);
              // read remote port
              int port = dataInputStream.readInt();
              Assert.assertTrue(setToCheck.contains(partitionId.toString() + hostname + port));
              setToCheck.remove(partitionId.toString() + hostname + port);
              // read replica token
              FindToken token = factory.getFindToken(dataInputStream);
              System.out.println(
                  "partitionId " + partitionId + " hostname " + hostname + " port " + port + " token " + token);
              ByteBuffer bytebufferToken = ByteBuffer.wrap(token.toBytes());
              Assert.assertEquals(bytebufferToken.getShort(), 0);
              int size = bytebufferToken.getInt();
              bytebufferToken.position(bytebufferToken.position() + size);
              long parsedToken = bytebufferToken.getLong();
              System.out.println("The parsed token is " + parsedToken);
              Assert.assertTrue(parsedToken == -1 || parsedToken == 13062);
            }
            long crc = crcStream.getValue();
            Assert.assertEquals(crc, dataInputStream.readLong());
          } catch (IOException e) {
            Assert.assertTrue(false);
          } finally {
            dataInputStream.close();
          }
        } else {
          Assert.assertTrue(false);
        }
      }

      // Add more data to server 2 and server 3. Recover server 1 and ensure it is completely replicated
      // put blob 7
      putRequest2 = new PutRequest(1, "client1", blobId7, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 8
      putRequest3 = new PutRequest(1, "client1", blobId8, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 9
      putRequest2 = new PutRequest(1, "client1", blobId9, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 10
      putRequest3 = new PutRequest(1, "client1", blobId10, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 11
      putRequest2 = new PutRequest(1, "client1", blobId11, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      cluster.getServers().get(0).startup();
      // wait for server to recover
      Thread.sleep(2000);
      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(blobId2, channel1, data);
        checkBlobContent(blobId3, channel1, data);
        checkBlobContent(blobId4, channel1, data);
        checkBlobContent(blobId5, channel1, data);
        checkBlobContent(blobId6, channel1, data);
        checkBlobContent(blobId7, channel1, data);
        checkBlobContent(blobId8, channel1, data);
        checkBlobContent(blobId9, channel1, data);
        checkBlobContent(blobId10, channel1, data);
        checkBlobContent(blobId11, channel1, data);
      } catch (MessageFormatException e) {
        Assert.assertFalse(true);
      }

      // Shutdown server 1. Remove all its data from all mount path. Recover server 1 and ensure node is built
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();

      File mountFile = new File(clusterMap.getReplicaIds(dataNodeId).get(0).getMountPath());
      for (File toDelete : mountFile.listFiles()) {
        deleteFolderContent(toDelete, true);
      }

      cluster.getServers().get(0).startup();
      Thread.sleep(2000);
      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(blobId2, channel1, data);
        checkBlobContent(blobId3, channel1, data);
        checkBlobContent(blobId4, channel1, data);
        checkBlobContent(blobId5, channel1, data);
        checkBlobContent(blobId6, channel1, data);
        checkBlobContent(blobId7, channel1, data);
        checkBlobContent(blobId8, channel1, data);
        checkBlobContent(blobId9, channel1, data);
        checkBlobContent(blobId10, channel1, data);
        checkBlobContent(blobId11, channel1, data);
      } catch (MessageFormatException e) {
        Assert.assertFalse(true);
      }

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  class PutRequestRunnable implements Runnable {

    BlockingChannel channel;
    List<BlobId> blobIds;
    byte[] data;
    byte[] usermetadata;
    BlobProperties blobProperties;
    CountDownLatch latch;

    private PutRequestRunnable(BlockingChannel channel, int totalBlobsToPut, byte[] data, byte[] usermetadata,
        BlobProperties blobProperties, CountDownLatch latch) {
      MockClusterMap clusterMap = cluster.getClusterMap();
      this.channel = channel;
      blobIds = new ArrayList<BlobId>(totalBlobsToPut);
      for (int i = 0; i < totalBlobsToPut; i++) {
        int partitionId = new Random().nextInt((int) clusterMap.getWritablePartitionIdsCount());
        BlobId blobId = new BlobId(clusterMap.getWritablePartitionIdAt(partitionId));
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
          InputStream putResponseStream = channel.receive();
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

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionTest()
      throws InterruptedException, IOException {

    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      List<AmbryServer> serverList = cluster.getServers();
      byte[] usermetadata = new byte[100];
      byte[] data = new byte[100];
      BlobProperties properties = new BlobProperties(100, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);

      // connect to all the servers
      BlockingChannel channel1 = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      BlockingChannel channel2 = new BlockingChannel("localhost", 6668, 10000, 10000, 10000);
      BlockingChannel channel3 = new BlockingChannel("localhost", 6669, 10000, 10000, 10000);

      // put all the blobs to random servers

      channel1.connect();
      channel2.connect();
      channel3.connect();

      int noOfParallelThreads = 3;
      CountDownLatch latch = new CountDownLatch(noOfParallelThreads);
      List<PutRequestRunnable> runnables = new ArrayList<PutRequestRunnable>(noOfParallelThreads);
      BlockingChannel channel = null;
      for (int i = 0; i < noOfParallelThreads; i++) {
        if (i % noOfParallelThreads == 0) {
          channel = channel1;
        } else if (i % noOfParallelThreads == 1) {
          channel = channel2;
        } else if (i % noOfParallelThreads == 2) {
          channel = channel3;
        }
        PutRequestRunnable runnable = new PutRequestRunnable(channel, 50, data, usermetadata, properties, latch);
        runnables.add(runnable);
        Thread threadToRun = new Thread(runnable);
        threadToRun.start();
      }
      latch.await();

      // wait till replication can complete
      Thread.sleep(4000);
      List<BlobId> blobIds = new ArrayList<BlobId>();
      for (int i = 0; i < runnables.size(); i++) {
        blobIds.addAll(runnables.get(i).getBlobIds());
      }

      // verify blob properties, metadata and blob across all nodes
      for (int i = 0; i < 3; i++) {
        channel = null;
        if (i == 0) {
          channel = channel1;
        } else if (i == 1) {
          channel = channel2;
        } else if (i == 2) {
          channel = channel3;
        }

        for (int j = 0; j < blobIds.size(); j++) {
          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(blobIds.get(j));
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, blobIds.get(j).getPartition(), ids);
          channel.send(getRequest);
          InputStream stream = channel.receive();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }

          // get user metadata
          ids.clear();
          ids.add(blobIds.get(j));
          getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, blobIds.get(j).getPartition(), ids);
          channel.send(getRequest);
          stream = channel.receive();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.assertEquals(false, true);
          }

          // get blob
          ids.clear();
          ids.add(blobIds.get(j));
          getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, blobIds.get(j).getPartition(), ids);
          channel.send(getRequest);
          stream = channel.receive();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          //System.out.println("response from get " + resp.getError());
          try {
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
            }
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.assertEquals(false, true);
          }
        }
      }

      // delete random blobs, wait for replication and ensure it is deleted in all nodes

      Set<BlobId> blobsDeleted = new HashSet<BlobId>();
      Set<BlobId> blobsChecked = new HashSet<BlobId>();
      for (int i = 0; i < blobIds.size(); i++) {
        int j = new Random().nextInt(3);
        if (j == 0) {
          j = new Random().nextInt(3);
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }
          DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobIds.get(i));
          channel.send(deleteRequest);
          InputStream deleteResponseStream = channel.receive();
          DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
          Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);
          blobsDeleted.add(blobIds.get(i));
        }
      }

      // wait for deleted state to replicate
      Thread.sleep(4000);

      Iterator<BlobId> iterator = blobsDeleted.iterator();
      while (iterator.hasNext()) {
        BlobId deletedId = iterator.next();
        for (int j = 0; j < 3; j++) {
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }

          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(deletedId);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, deletedId.getPartition(), ids);
          channel.send(getRequest);
          InputStream stream = channel.receive();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          Assert.assertEquals(resp.getError(), ServerErrorCode.Blob_Deleted);
        }
      }

      // take a server down, clean up a mount path, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      MockDataNodeId dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", 6667);
      System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(0));
      for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
        if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
          System.out.println("Cleaning partition " + replicaId.getPartitionId());
        }
      }
      deleteFolderContent(new File(dataNode.getMountPaths().get(0)), false);
      serverList.get(0).startup();

      Thread.sleep(4000);
      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        InputStream stream = channel1.receive();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        stream = channel1.receive();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        stream = channel1.receive();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        //System.out.println("response from get " + resp.getError());
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
          blobsDeleted.remove(blobIds.get(j));
          blobsChecked.add(blobIds.get(j));
        } else {
          try {
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
            }
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }
      }
      Assert.assertEquals(blobsDeleted.size(), 0);
      // take a server down, clean all contents, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", 6667);
      for (int i = 0; i < dataNode.getMountPaths().size(); i++) {
        System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(i));
        for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(i)) == 0) {
            System.out.println("Cleaning partition " + replicaId.getPartitionId());
          }
        }
        deleteFolderContent(new File(dataNode.getMountPaths().get(i)), false);
      }

      serverList.get(0).startup();

      Thread.sleep(4000);
      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        InputStream stream = channel1.receive();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        stream = channel1.receive();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, blobIds.get(j).getPartition(), ids);
        channel1.send(getRequest);
        stream = channel1.receive();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        //System.out.println("response from get " + resp.getError());
        if (resp.getError() == ServerErrorCode.Blob_Deleted || resp.getError() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
          blobsChecked.remove(blobIds.get(j));
        } else {
          try {
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
            }
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }
      }
      Assert.assertEquals(blobsChecked.size(), 0);

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  class Payload {
    public byte[] blob;
    public byte[] metadata;
    public BlobProperties blobProperties;
    public String blobId;

    public Payload(BlobProperties blobProperties, byte[] metadata, byte[] blob, String blobId) {
      this.blobProperties = blobProperties;
      this.metadata = metadata;
      this.blob = blob;
      this.blobId = blobId;
    }
  }

  class Sender implements Runnable {

    BlockingQueue<Payload> blockingQueue;
    CountDownLatch completedLatch;
    int numberOfRequests;
    Coordinator coordinator;

    public Sender(LinkedBlockingQueue<Payload> blockingQueue, CountDownLatch completedLatch, int numberOfRequests,
        Coordinator coordinator) {
      this.blockingQueue = blockingQueue;
      this.completedLatch = completedLatch;
      this.numberOfRequests = numberOfRequests;
      this.coordinator = coordinator;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < numberOfRequests; i++) {
          int size = new Random().nextInt(5000);
          BlobProperties properties = new BlobProperties(size, "service1", "owner id check", "image/jpeg", false);
          byte[] metadata = new byte[new Random().nextInt(1000)];
          byte[] blob = new byte[size];
          new Random().nextBytes(metadata);
          new Random().nextBytes(blob);
          String blobId = coordinator.putBlob(properties, ByteBuffer.wrap(metadata), new ByteArrayInputStream(blob));
          blockingQueue.put(new Payload(properties, metadata, blob, blobId));
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        completedLatch.countDown();
      }
    }
  }

  class Verifier implements Runnable {

    BlockingQueue<Payload> blockingQueue;
    CountDownLatch completedLatch;
    AtomicInteger totalRequests;
    AtomicInteger requestsVerified;
    MockClusterMap clusterMap;
    AtomicBoolean cancelTest;

    public Verifier(BlockingQueue<Payload> blockingQueue, CountDownLatch completedLatch, AtomicInteger totalRequests,
        AtomicInteger requestsVerified, MockClusterMap clusterMap, AtomicBoolean cancelTest) {
      this.blockingQueue = blockingQueue;
      this.completedLatch = completedLatch;
      this.totalRequests = totalRequests;
      this.requestsVerified = requestsVerified;
      this.clusterMap = clusterMap;
      this.cancelTest = cancelTest;
    }

    @Override
    public void run() {
      try {
        while (requestsVerified.get() != totalRequests.get() && !cancelTest.get()) {
          Payload payload = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (payload != null) {
            for (MockDataNodeId dataNodeId : clusterMap.getDataNodes()) {
              BlockingChannel channel1 =
                  new BlockingChannel(dataNodeId.getHostname(), dataNodeId.getPort(), 10000, 10000, 10000);
              channel1.connect();
              ArrayList<BlobId> ids = new ArrayList<BlobId>();
              ids.add(new BlobId(payload.blobId, clusterMap));
              GetRequest getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, ids.get(0).getPartition(), ids);
              channel1.send(getRequest);
              InputStream stream = channel1.receive();
              GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println(dataNodeId.getHostname() + " " + dataNodeId.getPort() + " " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
                  if (propertyOutput.getBlobSize() != payload.blobProperties.getBlobSize()) {
                    System.out.println("blob size not matching " + " expected " +
                        payload.blobProperties.getBlobSize() + " actual " + propertyOutput.getBlobSize());
                    throw new IllegalStateException();
                  }
                  if (!propertyOutput.getServiceId().equals(payload.blobProperties.getServiceId())) {
                    System.out.println("service id not matching " + " expected " +
                        payload.blobProperties.getServiceId() + " actual " + propertyOutput.getBlobSize());
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get user metadata
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
              getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, ids.get(0).getPartition(), ids);
              channel1.send(getRequest);
              stream = channel1.receive();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get user metadata " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
                  if (userMetadataOutput.compareTo(ByteBuffer.wrap(payload.metadata)) != 0 ) {
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get blob
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
              getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, ids.get(0).getPartition(), ids);
              channel1.send(getRequest);
              stream = channel1.receive();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              //System.out.println("response from get " + resp.getError());
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get blob " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
                  byte[] blobout = new byte[(int) blobOutput.getSize()];
                  int readsize = 0;
                  while (readsize < blobOutput.getSize()) {
                    readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              channel1.disconnect();
            }

            requestsVerified.incrementAndGet();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        cancelTest.set(true);
      } finally {
        completedLatch.countDown();
      }
    }
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest()
      throws InterruptedException, IOException {
    Properties props = new Properties();
    props.setProperty("coordinator.hostname", "localhost");
    props.setProperty("coordinator.datacenter.name", "DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    Coordinator coordinator = new AmbryCoordinator(verifiableProperties, cluster.getClusterMap());
    coordinator.start();
    Thread[] senderThreads = new Thread[3];
    LinkedBlockingQueue<Payload> blockingQueue = new LinkedBlockingQueue<Payload>();
    int numberOfSenderThreads = 3;
    int numberOfVerifierThreads = 3;
    CountDownLatch senderLatch = new CountDownLatch(numberOfSenderThreads);
    int numberOfRequestsToSendPerThread = 10;
    for (int i = 0; i < numberOfSenderThreads; i++) {
      senderThreads[i] = new Thread(new Sender(blockingQueue, senderLatch, numberOfRequestsToSendPerThread, coordinator));
      senderThreads[i].start();
    }
    senderLatch.await();

    if (blockingQueue.size() != numberOfRequestsToSendPerThread * numberOfSenderThreads) {
      // Failed during putBlob
      throw new IllegalStateException();
    }

    // Let replication complete before starting verifiers.
    Thread.sleep(4000);

    CountDownLatch verifierLatch = new CountDownLatch(numberOfVerifierThreads);
    AtomicInteger totalRequests = new AtomicInteger(numberOfRequestsToSendPerThread * numberOfSenderThreads);
    AtomicInteger verifiedRequests = new AtomicInteger(0);
    AtomicBoolean cancelTest = new AtomicBoolean(false);
    for (int i = 0; i < numberOfVerifierThreads; i++) {
      Thread thread = new Thread(
          new Verifier(blockingQueue, verifierLatch, totalRequests, verifiedRequests, cluster.getClusterMap(), cancelTest));
      thread.start();
    }
    verifierLatch.await();

    Assert.assertEquals(totalRequests.get(), verifiedRequests.get());
    coordinator.shutdown();
  }

  private void checkBlobId(Coordinator coordinator, BlobId blobId, byte[] data)
      throws CoordinatorException, IOException {
    BlobOutput output = coordinator.getBlob(blobId.toString());
    Assert.assertEquals(output.getSize(), 1000);
    byte[] dataOutputStream = new byte[(int) output.getSize()];
    output.getStream().read(dataOutputStream);
    Assert.assertArrayEquals(dataOutputStream, data);
  }

  private void checkBlobContent(BlobId blobId, BlockingChannel channel, byte[] dataToCheck)
      throws IOException, MessageFormatException {
    ArrayList<BlobId> listIds = new ArrayList<BlobId>();
    listIds.add(blobId);
    GetRequest getRequest3 = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, blobId.getPartition(), listIds);
    channel.send(getRequest3);
    InputStream stream = channel.receive();
    GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), cluster.getClusterMap());
    BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = new byte[(int) blobOutput.getSize()];
    int readsize = 0;
    while (readsize < blobOutput.getSize()) {
      readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
    }
    Assert.assertArrayEquals(blobout, dataToCheck);
  }

  private VerifiableProperties getCoordinatorProperties() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "Datacenter");
    return new VerifiableProperties(properties);
  }

  public static void deleteFolderContent(File folder, boolean deleteParentFolder) {
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteFolderContent(f, true);
        } else {
          f.delete();
        }
      }
    }
    if (deleteParentFolder) {
      folder.delete();
    }
  }
}
