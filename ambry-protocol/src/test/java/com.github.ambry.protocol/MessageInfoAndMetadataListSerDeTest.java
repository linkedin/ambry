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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.utils.TestUtils.*;


/**
 * Tests {@link MessageInfoAndMetadataListSerde}
 */
@RunWith(Parameterized.class)
public class MessageInfoAndMetadataListSerDeTest {
  private final short serDeVersion;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{MessageInfoAndMetadataListSerde.VERSION_1}, {MessageInfoAndMetadataListSerde.VERSION_2}, {MessageInfoAndMetadataListSerde.VERSION_3}, {MessageInfoAndMetadataListSerde.VERSION_4}});
  }

  public MessageInfoAndMetadataListSerDeTest(short serDeVersion) {
    this.serDeVersion = serDeVersion;
  }

  @Test
  public void testSerDe() throws Exception {
    MockClusterMap mockMap = new MockClusterMap();
    MockPartitionId partitionId = new MockPartitionId();
    short[] accountIds = {100, 101, 102, 103};
    short[] containerIds = {10, 11, 12, 13};
    StoreKey[] keys =
        {new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0,
            accountIds[0], containerIds[0], partitionId, false), new BlobId(
            TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0, accountIds[1],
            containerIds[1], partitionId, false), new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()),
            BlobId.BlobIdType.NATIVE, (byte) 0, accountIds[2], containerIds[2], partitionId, false), new BlobId(
            TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0, accountIds[3],
            containerIds[3], partitionId, false)};
    long[] blobSizes = {1024, 2048, 4096, 8192};
    long[] operationTimes = {SystemTime.getInstance().milliseconds(),
        SystemTime.getInstance().milliseconds() + 10,
        SystemTime.getInstance().milliseconds() + 20, SystemTime.getInstance().milliseconds() + 30};
    MessageMetadata[] messageMetadata = new MessageMetadata[4];
    messageMetadata[0] = new MessageMetadata(ByteBuffer.wrap(getRandomBytes(100)));
    messageMetadata[1] = new MessageMetadata(null);
    messageMetadata[2] = null;
    messageMetadata[3] = new MessageMetadata(ByteBuffer.wrap(getRandomBytes(200)));

    List<MessageInfo> messageInfoList = new ArrayList<>(4);
    List<MessageMetadata> messageMetadataList = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      messageInfoList.add(new MessageInfo(keys[i], blobSizes[i], accountIds[i], containerIds[i], operationTimes[i]));
      messageMetadataList.add(messageMetadata[i]);
    }

    // Serialize and then deserialize
    MessageInfoAndMetadataListSerde messageInfoAndMetadataListSerde =
        new MessageInfoAndMetadataListSerde(messageInfoList, messageMetadataList, serDeVersion);
    ByteBuffer buffer = ByteBuffer.allocate(messageInfoAndMetadataListSerde.getMessageInfoAndMetadataListSize());
    messageInfoAndMetadataListSerde.serializeMessageInfoAndMetadataList(buffer);
    buffer.flip();
    Pair<List<MessageInfo>, List<MessageMetadata>> messageInfoAndMetadataList =
        MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(
            new DataInputStream(new ByteBufferInputStream(buffer)), mockMap, serDeVersion);

    // Verify
    List<MessageInfo> responseMessageInfoList = messageInfoAndMetadataList.getFirst();
    List<MessageMetadata> responseMessageMetadataList = messageInfoAndMetadataList.getSecond();
    Assert.assertEquals(4, responseMessageInfoList.size());
    Assert.assertEquals(4, responseMessageMetadataList.size());
    for (int i = 0; i < 4; i++) {
      assertMessageInfoEquality(messageInfoList.get(i), responseMessageInfoList.get(i));
      assertMessageMetadataEquality(messageMetadataList.get(i), responseMessageMetadataList.get(i));
    }
  }

  private void assertMessageInfoEquality(MessageInfo a, MessageInfo b) {
    Assert.assertEquals(a.getExpirationTimeInMs(), b.getExpirationTimeInMs());
    Assert.assertEquals(a.getSize(), b.getSize());
    Assert.assertEquals(a.getStoreKey(), b.getStoreKey());
    Assert.assertEquals(a.isDeleted(), b.isDeleted());
    Assert.assertEquals(a.isExpired(), b.isExpired());
    if (serDeVersion >= MessageInfoAndMetadataListSerde.VERSION_2) {
      Assert.assertEquals(a.getCrc(), b.getCrc());
    }
    if (serDeVersion >= MessageInfoAndMetadataListSerde.VERSION_3) {
      Assert.assertEquals(a.getAccountId(), b.getAccountId());
      Assert.assertEquals(a.getContainerId(), b.getContainerId());
      Assert.assertEquals(a.getOperationTimeMs(), b.getOperationTimeMs());
    }
  }

  private void assertMessageMetadataEquality(MessageMetadata a, MessageMetadata b) {
    if (serDeVersion >= MessageInfoAndMetadataListSerde.VERSION_4) {
      if (a == null) {
        Assert.assertNull(b);
      } else {
        if (a.getEncryptionKey() == null) {
          // null encryption keys come in as empty.
          Assert.assertEquals(0, b.getEncryptionKey().remaining());
        } else {
          Assert.assertEquals(a.getEncryptionKey().rewind(), b.getEncryptionKey());
        }
        Assert.assertEquals(a.getVersion(), b.getVersion());
      }
    }
  }
}
