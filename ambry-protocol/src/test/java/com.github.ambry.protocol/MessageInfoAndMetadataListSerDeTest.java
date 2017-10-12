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
  private final short getResponseVersion;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{GetResponse.GET_RESPONSE_VERSION_V_4}, {GetResponse.GET_RESPONSE_VERSION_V_3}});
  }

  public MessageInfoAndMetadataListSerDeTest(short getResponseVersion) {
    this.getResponseVersion = getResponseVersion;
  }

  @Test
  public void testSerDe() throws Exception {
    MockClusterMap mockMap = new MockClusterMap();
    MockPartitionId partitionId = new MockPartitionId();
    short[] accountIds = {100, 101, 102, 103};
    short[] containerIds = {10, 11, 12, 13};
    StoreKey[] keys =
        {new BlobId((byte) 0, (byte) 0, accountIds[0], containerIds[0], partitionId), new BlobId((byte) 0, (byte) 0,
            accountIds[1], containerIds[1], partitionId), new BlobId((byte) 0, (byte) 0, accountIds[2], containerIds[2],
            partitionId), new BlobId((byte) 0, (byte) 0, accountIds[3], containerIds[3], partitionId)};
    long[] blobSizes = {1024, 2048, 4096, 8192};
    long[] operationTimes = {SystemTime.getInstance().milliseconds(),
        SystemTime.getInstance().milliseconds() + 10,
        SystemTime.getInstance().milliseconds() + 20, SystemTime.getInstance().milliseconds() + 30};
    MessageMetadata[] messageMetadata = new MessageMetadata[4];
    messageMetadata[0] = new MessageMetadata(ByteBuffer.wrap(randomArray(100)));
    messageMetadata[1] = new MessageMetadata(null);
    messageMetadata[2] = null;
    messageMetadata[3] = new MessageMetadata(ByteBuffer.wrap(randomArray(200)));

    List<MessageInfo> messageInfoList = new ArrayList<>(4);
    List<MessageMetadata> messageMetadataList = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      messageInfoList.add(new MessageInfo(keys[i], blobSizes[i], accountIds[i], containerIds[i], operationTimes[i]));
      messageMetadataList.add(messageMetadata[i]);
    }

    // Serialize and then deserialize
    MessageInfoAndMetadataListSerde messageInfoAndMetadataListSerde =
        new MessageInfoAndMetadataListSerde(messageInfoList, messageMetadataList, getResponseVersion);
    ByteBuffer buffer = ByteBuffer.allocate(messageInfoAndMetadataListSerde.getMessageInfoAndMetadataListSize());
    messageInfoAndMetadataListSerde.serializeMessageInfoAndMetadataList(buffer);
    buffer.flip();
    Pair<List<MessageInfo>, List<MessageMetadata>> messageInfoAndMetadataList =
        MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(
            new DataInputStream(new ByteBufferInputStream(buffer)), mockMap, getResponseVersion);

    // Verify
    List<MessageInfo> responseMessageInfoList = messageInfoAndMetadataList.getFirst();
    List<MessageMetadata> responseMessageMetadataList = messageInfoAndMetadataList.getSecond();
    Assert.assertEquals(4, responseMessageInfoList.size());
    Assert.assertEquals(4, responseMessageMetadataList.size());
  }
}
