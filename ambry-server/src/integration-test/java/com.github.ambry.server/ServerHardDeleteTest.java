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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.PersistentIndex;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ServerHardDeleteTest {
  private MockNotificationSystem notificationSystem;
  private MockTime time;
  private AmbryServer server;
  private MockClusterMap mockClusterMap;

  @Before
  public void initialize()
      throws Exception {
    notificationSystem = new MockNotificationSystem(1);
    mockClusterMap = new MockClusterMap(false, 1, 1, 1);
    time = new MockTime(SystemTime.getInstance().milliseconds());
    Properties props = new Properties();
    props.setProperty("host.name", mockClusterMap.getDataNodes().get(0).getHostname());
    props.setProperty("port", Integer.toString(mockClusterMap.getDataNodes().get(0).getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.enable.hard.delete", "true");
    props.setProperty("store.deleted.message.retention.days", "1");
    VerifiableProperties propverify = new VerifiableProperties(props);
    server = new AmbryServer(propverify, mockClusterMap, notificationSystem, time);
    server.startup();
  }

  @After
  public void cleanup()
      throws IOException {
    server.shutdown();
    mockClusterMap.cleanup();
  }

  /**
   * Waits and ensures that the hard delete cleanup token catches up to the expected token value.
   * @param path the path to the cleanup token.
   * @param mockClusterMap the {@link MockClusterMap} being used for the cluster.
   * @param expectedTokenValue the expected value that the cleanup token should contain. Until this value is reached,
   *                           the method will keep reopening the file and read the value or until a predefined
   *                           timeout is reached.
   * @throws Exception if there were any I/O errors or the sleep gets interrupted.
   */
  void ensureCleanupTokenCatchesUp(String path, MockClusterMap mockClusterMap, long expectedTokenValue)
      throws Exception {
    final int TIMEOUT = 10000;
    File cleanupTokenFile = new File(path, "cleanuptoken");
    FindToken endToken;
    long parsedTokenValue = -1;

    long endTime = SystemTime.getInstance().milliseconds() + TIMEOUT;
    do {
      if (cleanupTokenFile.exists()) {
        /* The cleanup token format is as follows:
           --
           token_version
           startTokenForRecovery
           endTokenForRecovery
           numBlobsInRange
           --
           blob1_blobReadOptions {version, offset, sz, ttl, key}
           blob2_blobReadOptions
           ....
           blobN_blobReadOptions
           --
           length_of_blob1_messageStoreRecoveryInfo
           blob1_messageStoreRecoveryInfo {headerVersion, userMetadataVersion, userMetadataSize, blobRecordVersion,
            blobType, blobStreamSize}
           length_of_blob2_messageStoreRecoveryInfo
           blob2_messageStoreRecoveryInfo
           ....
           length_of_blobN_messageStoreRecoveryInfo
           blobN_messageStoreRecoveryInfo
           crc
           ---
         */

        CrcInputStream crcStream = new CrcInputStream(new FileInputStream(cleanupTokenFile));
        DataInputStream stream = new DataInputStream(crcStream);
        try {
          short version = stream.readShort();
          Assert.assertEquals(version, PersistentIndex.Cleanup_Token_Version_V1);
          StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", mockClusterMap);
          FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

          factory.getFindToken(stream);
          endToken = factory.getFindToken(stream);

          ByteBuffer bytebufferToken = ByteBuffer.wrap(endToken.toBytes());
          Assert.assertEquals(bytebufferToken.getShort(), 0);
          int size = bytebufferToken.getInt();
          bytebufferToken.position(bytebufferToken.position() + size);
          parsedTokenValue = bytebufferToken.getLong();

          int num = stream.readInt();
          List<StoreKey> storeKeyList = new ArrayList<StoreKey>(num);
          for (int i = 0; i < num; i++) {
            // Read BlobReadOptions
            short blobReadOptionsVersion = stream.readShort();
            switch (blobReadOptionsVersion) {
              case 0:
                long offset = stream.readLong();
                long sz = stream.readLong();
                long ttl = stream.readLong();
                StoreKey key = storeKeyFactory.getStoreKey(stream);
                storeKeyList.add(key);
                break;
              default:
                Assert.assertFalse(true);
            }
          }

          for (int i = 0; i < num; i++) {
            int length = stream.readInt();
            short headerVersion = stream.readShort();
            short userMetadataVersion = stream.readShort();
            int userMetadataSize = stream.readInt();
            short blobRecordVersion = stream.readShort();
            if (blobRecordVersion == MessageFormatRecord.Blob_Version_V2) {
              short blobType = stream.readShort();
            }
            long blobStreamSize = stream.readLong();
            StoreKey key = storeKeyFactory.getStoreKey(stream);
            Assert.assertTrue(storeKeyList.get(i).equals(key));
          }

          long crc = crcStream.getValue();
          Assert.assertEquals(crc, stream.readLong());
          Thread.sleep(1000);
        } finally {
          stream.close();
        }
      }
    } while (SystemTime.getInstance().milliseconds() < endTime && parsedTokenValue < expectedTokenValue);
    Assert.assertEquals(expectedTokenValue, parsedTokenValue);
  }

  /**
   * Tests the hard delete functionality.
   * <p>
   * This test does the following:
   * 1. Makes 6 puts, waits for notification.
   * 2. Makes 2 deletes, waits for notification.
   * 3. Waits for hard deletes to catch up to the expected token value.
   * 4. Verifies that the two records that are deleted are zeroed out by hard deletes.
   * 5. Makes 3 more puts, waits for notification.
   * 6. Makes 3 deletes - 2 of records from the initial set of puts, and 1 from the new set.
   * 7. Waits for hard deletes to catch up again to the expected token value.
   * 8. Verifies that the three records that are deleted are zeroed out by hard deletes.
   *
   * @throws Exception
   */
  @Test
  public void endToEndTestHardDeletes()
      throws Exception {
    DataNodeId dataNodeId = mockClusterMap.getDataNodeIds().get(0);
    ArrayList<byte[]> usermetadata = new ArrayList<byte[]>(9);
    ArrayList<byte[]> data = new ArrayList<byte[]>(9);
    Random random = new Random();
    for (int i = 0; i < 9; i++) {
      usermetadata.add(new byte[1000 + i]);
      data.add(new byte[31870 + i]);
      random.nextBytes(usermetadata.get(i));
      random.nextBytes(data.get(i));
    }

    ArrayList<BlobProperties> properties = new ArrayList<BlobProperties>(9);
    properties.add(new BlobProperties(31870, "serviceid1"));
    properties.add(new BlobProperties(31871, "serviceid1"));
    properties.add(new BlobProperties(31872, "serviceid1"));
    properties.add(new BlobProperties(31873, "serviceid1", "ownerid", "jpeg", false, 0));
    properties.add(new BlobProperties(31874, "serviceid1"));
    properties.add(new BlobProperties(31875, "serviceid1", "ownerid", "jpeg", false, 0));
    properties.add(new BlobProperties(31876, "serviceid1"));
    properties.add(new BlobProperties(31877, "serviceid1"));
    properties.add(new BlobProperties(31878, "serviceid1"));

    List<PartitionId> partitionIds = mockClusterMap.getWritablePartitionIds();
    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>(9);
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));

    // put blob 0
    PutRequest putRequest0 =
        new PutRequest(1, "client1", blobIdList.get(0), properties.get(0), ByteBuffer.wrap(usermetadata.get(0)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(0))), properties.get(0).getBlobSize(),
            BlobType.DataBlob);
    BlockingChannel channel = ServerTestUtil
        .getBlockingChannelBasedOnPortType(new Port(dataNodeId.getPort(), PortType.PLAINTEXT), "localhost", null, null);
    channel.connect();
    channel.send(putRequest0);
    InputStream putResponseStream = channel.receive().getInputStream();
    PutResponse response0 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response0.getError(), ServerErrorCode.No_Error);

    // put blob 1
    PutRequest putRequest1 =
        new PutRequest(1, "client1", blobIdList.get(1), properties.get(1), ByteBuffer.wrap(usermetadata.get(1)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(1))), properties.get(1).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest1);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response1 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response1.getError(), ServerErrorCode.No_Error);

    // put blob 2
    PutRequest putRequest2 =
        new PutRequest(1, "client1", blobIdList.get(2), properties.get(2), ByteBuffer.wrap(usermetadata.get(2)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(2))), properties.get(2).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest2);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

    // put blob 3 that is expired
    PutRequest putRequest3 =
        new PutRequest(1, "client1", blobIdList.get(3), properties.get(3), ByteBuffer.wrap(usermetadata.get(3)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(3))), properties.get(3).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest3);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

    // put blob 4
    PutRequest putRequest4 =
        new PutRequest(1, "client1", blobIdList.get(4), properties.get(4), ByteBuffer.wrap(usermetadata.get(4)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(4))), properties.get(4).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest4);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response4.getError(), ServerErrorCode.No_Error);

    // put blob 5 that is expired
    PutRequest putRequest5 =
        new PutRequest(1, "client1", blobIdList.get(5), properties.get(5), ByteBuffer.wrap(usermetadata.get(5)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(5))), properties.get(5).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest5);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response5 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response5.getError(), ServerErrorCode.No_Error);

    notificationSystem.awaitBlobCreations(blobIdList.get(0).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());

    // delete blob 1
    DeleteRequest deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(1));
    channel.send(deleteRequest);
    InputStream deleteResponseStream = channel.receive().getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    byte[] zeroedMetadata = new byte[usermetadata.get(1).length];
    usermetadata.set(1, zeroedMetadata);
    byte[] zeroedData = new byte[data.get(1).length];
    data.set(1, zeroedData);

    // delete blob 4
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(4));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(4).length];
    usermetadata.set(4, zeroedMetadata);
    zeroedData = new byte[data.get(4).length];
    data.set(4, zeroedData);

    notificationSystem.awaitBlobDeletions(blobIdList.get(1).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(4).getID());

    time.currentMilliseconds = time.currentMilliseconds + Time.SecsPerDay * Time.MsPerSec;
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(0).getReplicaPath(), mockClusterMap, 198443);

    MockPartitionId partition = (MockPartitionId) mockClusterMap.getWritablePartitionIds().get(0);

    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    for (int i = 0; i < 6; i++) {
      ids.add(blobIdList.get(i));
    }

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
    partitionRequestInfoList.add(partitionRequestInfo);

    try {
      GetRequest getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
              GetOptions.Include_All);
      channel.send(getRequest);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 6; i++) {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), properties.get(i).getBlobSize());
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }

      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
          GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 6; i++) {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata.get(i));
      }

      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 6; i++) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
        Assert.assertEquals(properties.get(i).getBlobSize(), blobData.getSize());
        byte[] dataOutput = new byte[(int) blobData.getSize()];
        blobData.getStream().read(dataOutput);
        Assert.assertArrayEquals(dataOutput, data.get(i));
      }
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    }

    // put blob 6
    PutRequest putRequest6 =
        new PutRequest(1, "client1", blobIdList.get(6), properties.get(6), ByteBuffer.wrap(usermetadata.get(6)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(6))), properties.get(6).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest6);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response6 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response6.getError(), ServerErrorCode.No_Error);

    // put blob 7
    PutRequest putRequest7 =
        new PutRequest(1, "client1", blobIdList.get(7), properties.get(7), ByteBuffer.wrap(usermetadata.get(7)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(7))), properties.get(7).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest7);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response7 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response7.getError(), ServerErrorCode.No_Error);

    // put blob 8
    PutRequest putRequest8 =
        new PutRequest(1, "client1", blobIdList.get(8), properties.get(8), ByteBuffer.wrap(usermetadata.get(8)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(8))), properties.get(8).getBlobSize(),
            BlobType.DataBlob);
    channel.send(putRequest8);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response9 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response9.getError(), ServerErrorCode.No_Error);

    notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
    // Do more deletes

    // delete blob 3 that is expired.
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(3));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(3).length];
    usermetadata.set(3, zeroedMetadata);
    zeroedData = new byte[data.get(3).length];
    data.set(3, zeroedData);

    // delete blob 0
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(0));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(0).length];
    usermetadata.set(0, zeroedMetadata);
    zeroedData = new byte[data.get(0).length];
    data.set(0, zeroedData);

    // delete blob 6.
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(6));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(6).length];
    usermetadata.set(6, zeroedMetadata);
    zeroedData = new byte[data.get(6).length];
    data.set(6, zeroedData);

    notificationSystem.awaitBlobDeletions(blobIdList.get(0).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(6).getID());

    time.currentMilliseconds = time.currentMilliseconds + Time.SecsPerDay * Time.MsPerSec;
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(0).getReplicaPath(), mockClusterMap, 297923);

    partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfo = new PartitionRequestInfo(partition, blobIdList);
    partitionRequestInfoList.add(partitionRequestInfo);

    try {
      GetRequest getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
              GetOptions.Include_All);
      channel.send(getRequest);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 9; i++) {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), properties.get(i).getBlobSize());
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }

      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
          GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 9; i++) {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata.get(i));
      }

      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);

      for (int i = 0; i < 9; i++) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
        Assert.assertEquals(blobData.getSize(), properties.get(i).getBlobSize());
        byte[] dataOutput = new byte[(int) blobData.getSize()];
        blobData.getStream().read(dataOutput);
        Assert.assertArrayEquals(dataOutput, data.get(i));
      }
    } catch (MessageFormatException e) {
      e.printStackTrace();
      Assert.assertEquals(false, true);
    }
  }
}