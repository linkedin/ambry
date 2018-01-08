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
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.HardDeleter;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
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
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ServerHardDeleteTest {
  private MockNotificationSystem notificationSystem;
  private MockTime time;
  private AmbryServer server;
  private MockClusterAgentsFactory mockClusterAgentsFactory;
  private MockClusterMap mockClusterMap;
  private ArrayList<BlobProperties> properties;
  private ArrayList<byte[]> encryptionKey;
  private ArrayList<byte[]> usermetadata;
  private ArrayList<byte[]> data;
  private ArrayList<BlobId> blobIdList;

  @Before
  public void initialize() throws Exception {
    notificationSystem = new MockNotificationSystem(1);
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, 1);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    time = new MockTime(SystemTime.getInstance().milliseconds());
    Properties props = new Properties();
    props.setProperty("host.name", mockClusterMap.getDataNodes().get(0).getHostname());
    props.setProperty("port", Integer.toString(mockClusterMap.getDataNodes().get(0).getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.enable.hard.delete", "true");
    props.setProperty("store.deleted.message.retention.days", "1");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    VerifiableProperties propverify = new VerifiableProperties(props);
    server = new AmbryServer(propverify, mockClusterAgentsFactory, notificationSystem, time);
    server.startup();
  }

  @After
  public void cleanup() throws IOException {
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
    StoreFindToken endToken;
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
           pause flag
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
          Assert.assertEquals(version, HardDeleter.Cleanup_Token_Version_V1);
          StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", mockClusterMap);
          FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

          factory.getFindToken(stream);
          endToken = (StoreFindToken) factory.getFindToken(stream);
          Offset endTokenOffset = endToken.getOffset();
          parsedTokenValue = endTokenOffset == null ? -1 : endTokenOffset.getOffset();
          boolean pauseFlag = stream.readByte() == (byte) 1;
          int num = stream.readInt();
          List<StoreKey> storeKeyList = new ArrayList<StoreKey>(num);
          for (int i = 0; i < num; i++) {
            // Read BlobReadOptions
            short blobReadOptionsVersion = stream.readShort();
            switch (blobReadOptionsVersion) {
              case 1:
                Offset.fromBytes(stream);
                stream.readLong();
                stream.readLong();
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
  public void endToEndTestHardDeletes() throws Exception {
    DataNodeId dataNodeId = mockClusterMap.getDataNodeIds().get(0);
    encryptionKey = new ArrayList<>(9);
    usermetadata = new ArrayList<>(9);
    data = new ArrayList<>(9);
    Random random = new Random();
    for (int i = 0; i < 9; i++) {
      if (i % 2 == 0) {
        encryptionKey.add(new byte[100]);
        random.nextBytes(encryptionKey.get(i));
      } else {
        encryptionKey.add(null);
      }
      usermetadata.add(new byte[1000 + i]);
      data.add(new byte[31870 + i]);
      random.nextBytes(usermetadata.get(i));
      random.nextBytes(data.get(i));
    }

    properties = new ArrayList<>(9);
    properties.add(new BlobProperties(31870, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(new BlobProperties(31871, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), false));
    properties.add(new BlobProperties(31872, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(
        new BlobProperties(31873, "serviceid1", "ownerid", "jpeg", false, 0, Utils.getRandomShort(TestUtils.RANDOM),
            Utils.getRandomShort(TestUtils.RANDOM), false));
    properties.add(new BlobProperties(31874, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(
        new BlobProperties(31875, "serviceid1", "ownerid", "jpeg", false, 0, Utils.getRandomShort(TestUtils.RANDOM),
            Utils.getRandomShort(TestUtils.RANDOM), false));
    properties.add(new BlobProperties(31876, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(new BlobProperties(31877, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), false));
    properties.add(new BlobProperties(31878, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));

    List<PartitionId> partitionIds = mockClusterMap.getWritablePartitionIds();
    PartitionId chosenPartition = partitionIds.get(0);
    blobIdList = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      blobIdList.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          mockClusterMap.getLocalDatacenterId(), properties.get(i).getAccountId(), properties.get(i).getContainerId(),
          chosenPartition, false));
    }

    BlockingChannel channel =
        ServerTestUtil.getBlockingChannelBasedOnPortType(new Port(dataNodeId.getPort(), PortType.PLAINTEXT),
            "localhost", null, null);
    channel.connect();
    for (int i = 0; i < 6; i++) {
      // blob 3 and 5 are expired among these
      putBlob(blobIdList.get(i), properties.get(i), encryptionKey.get(i), usermetadata.get(i), data.get(i), channel);
    }

    notificationSystem.awaitBlobCreations(blobIdList.get(0).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());

    // delete blob 1
    deleteBlob(blobIdList.get(1), channel);
    zeroOutBlobContent(1);

    // delete blob 4
    deleteBlob(blobIdList.get(4), channel);
    zeroOutBlobContent(4);

    notificationSystem.awaitBlobDeletions(blobIdList.get(1).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(4).getID());

    time.sleep(TimeUnit.DAYS.toMillis(1));
    // Changes in this patch: a. New header version has 4 more bytes compared to previous b. BlobProperties increased by 1 byte
    // c. Encryption Key Record size is 114 for an encryptionKey of size 100. EncryptionKeyRecord could be null if not applicable.
    // Delta: 7 * 4 (headers for 7 records) + 1*6 (BlobProperties for 6 put records) + 114*3 = 376
    ensureCleanupTokenCatchesUp(chosenPartition.getReplicaIds().get(0).getReplicaPath(), mockClusterMap, 198896);

    getAndVerify(channel, 6);

    // put blob 6
    putBlob(blobIdList.get(6), properties.get(6), encryptionKey.get(6), usermetadata.get(6), data.get(6), channel);
    // put blob 7
    putBlob(blobIdList.get(7), properties.get(7), encryptionKey.get(7), usermetadata.get(7), data.get(7), channel);
    // put blob 8
    putBlob(blobIdList.get(8), properties.get(8), encryptionKey.get(8), usermetadata.get(8), data.get(8), channel);

    notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
    // Do more deletes

    // delete blob 3 that is expired.
    deleteBlob(blobIdList.get(3), channel);
    zeroOutBlobContent(3);

    // delete blob 0
    deleteBlob(blobIdList.get(0), channel);
    zeroOutBlobContent(0);

    // delete blob 6.
    deleteBlob(blobIdList.get(6), channel);
    zeroOutBlobContent(6);

    notificationSystem.awaitBlobDeletions(blobIdList.get(0).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(6).getID());

    time.sleep(TimeUnit.DAYS.toMillis(1));
    // changes in this patch: a. New header version has 4 more bytes compared to preivous b. BlobProperties increased by 1 byte
    // c. Encryption Key Record size is 114 for an encryptionKey of size 100. EncryptionKeyRecord could be null if not applicable.
    // Delta: 6*4 (header) + 3*1 ( blob props) + 114*2 = 225 + 376 (from previous checkpoint) = 631
    ensureCleanupTokenCatchesUp(chosenPartition.getReplicaIds().get(0).getReplicaPath(), mockClusterMap, 298712);

    getAndVerify(channel, 9);
  }

  /**
   * Uploads a single blob to ambry server node
   * @param blobId the {@link BlobId} that needs to be put
   * @param properties the {@link BlobProperties} of the blob being uploaded
   * @param usermetadata the user metadata of the blob being uploaded
   * @param data the blob content of the blob being uploaded
   * @param channel the {@link BlockingChannel} to use to send and receive data
   * @throws IOException
   */
  void putBlob(BlobId blobId, BlobProperties properties, byte[] encryptionKey, byte[] usermetadata, byte[] data,
      BlockingChannel channel) throws IOException {
    PutRequest putRequest0 =
        new PutRequest(1, "client1", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey == null ? null : ByteBuffer.wrap(encryptionKey));
    channel.send(putRequest0);
    InputStream putResponseStream = channel.receive().getInputStream();
    PutResponse response0 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response0.getError(), ServerErrorCode.No_Error);
  }

  /**
   * Deletes a single blob from ambry server node
   * @param blobId the {@link BlobId} that needs to be deleted
   * @param channel the {@link BlockingChannel} to use to send and receive data
   * @throws IOException
   */
  void deleteBlob(BlobId blobId, BlockingChannel channel) throws IOException {
    DeleteRequest deleteRequest = new DeleteRequest(1, "client1", blobId, time.milliseconds());
    channel.send(deleteRequest);
    InputStream deleteResponseStream = channel.receive().getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);
  }

  /**
   * Zeros out user metadata and blob content for the blob indexed at the given {@code index}
   * @param index the index of the blob that needs to be zeroed out
   */
  void zeroOutBlobContent(int index) {
    byte[] zeroedMetadata = new byte[usermetadata.get(index).length];
    usermetadata.set(index, zeroedMetadata);
    byte[] zeroedData = new byte[data.get(index).length];
    data.set(index, zeroedData);
  }

  /**
   * Fetches the Blob(for all MessageFormatFlags) and verifies the content
   * @param channel the {@link BlockingChannel} to use to send and receive data
   * @param blobsCount the total number of blobs that needs to be verified against
   * @throws Exception
   */
  void getAndVerify(BlockingChannel channel, int blobsCount) throws Exception {
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    ArrayList<BlobId> ids = new ArrayList<>();
    for (int i = 0; i < blobsCount; i++) {
      ids.add(blobIdList.get(i));
    }

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIdList.get(0).getPartition(), ids);
    partitionRequestInfoList.add(partitionRequestInfo);

    ArrayList<MessageFormatFlags> flags = new ArrayList<>();
    flags.add(MessageFormatFlags.BlobProperties);
    flags.add(MessageFormatFlags.BlobUserMetadata);
    flags.add(MessageFormatFlags.Blob);
    for (MessageFormatFlags flag : flags) {
      GetRequest getRequest = new GetRequest(1, "clientid2", flag, partitionRequestInfoList, GetOption.Include_All);
      channel.send(getRequest);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), mockClusterMap);
      if (flag == MessageFormatFlags.BlobProperties) {
        for (int i = 0; i < blobsCount; i++) {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          Assert.assertEquals(properties.get(i).getBlobSize(), propertyOutput.getBlobSize());
          Assert.assertEquals("serviceid1", propertyOutput.getServiceId());
          Assert.assertEquals("AccountId mismatch", properties.get(i).getAccountId(), propertyOutput.getAccountId());
          Assert.assertEquals("ContainerId mismatch", properties.get(i).getContainerId(),
              propertyOutput.getContainerId());
        }
      } else if (flag == MessageFormatFlags.BlobUserMetadata) {
        for (int i = 0; i < blobsCount; i++) {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata.get(i));
        }
      } else if (flag == MessageFormatFlags.Blob) {
        for (int i = 0; i < blobsCount; i++) {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          Assert.assertEquals(properties.get(i).getBlobSize(), blobData.getSize());
          byte[] dataOutput = new byte[(int) blobData.getSize()];
          blobData.getStream().read(dataOutput);
          Assert.assertArrayEquals(dataOutput, data.get(i));
        }
      } else {
        throw new IllegalArgumentException("Unrecognized message format flags " + flags);
      }
    }
  }
}

