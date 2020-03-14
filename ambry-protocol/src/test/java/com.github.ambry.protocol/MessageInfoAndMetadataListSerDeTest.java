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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
        new Object[][]{{MessageInfoAndMetadataListSerde.DETERMINE_VERSION}, {MessageInfoAndMetadataListSerde.VERSION_1},
            {MessageInfoAndMetadataListSerde.VERSION_2}, {MessageInfoAndMetadataListSerde.VERSION_3},
            {MessageInfoAndMetadataListSerde.VERSION_4}, {MessageInfoAndMetadataListSerde.VERSION_5},
            {MessageInfoAndMetadataListSerde.VERSION_6}});
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
    boolean[] isDeletedVals = {false, true, false, true};
    boolean[] isTtlUpdatedVals = {true, false, false, true};
    boolean[] isUndeletedVals = {true, false, false, true};
    short[] lifeVersionVals = {1, 2, 3, 4};
    Long[] crcs = {null, 100L, Long.MIN_VALUE, Long.MAX_VALUE};
    StoreKey[] keys =
        {new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0,
            accountIds[0], containerIds[0], partitionId, false, BlobId.BlobDataType.DATACHUNK),
            new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0,
                accountIds[1], containerIds[1], partitionId, false, BlobId.BlobDataType.DATACHUNK),
            new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0,
                accountIds[2], containerIds[2], partitionId, false, BlobId.BlobDataType.DATACHUNK),
            new BlobId(TestUtils.getRandomElement(BlobId.getAllValidVersions()), BlobId.BlobIdType.NATIVE, (byte) 0,
                accountIds[3], containerIds[3], partitionId, false, BlobId.BlobDataType.DATACHUNK)};
    long[] blobSizes = {1024, 2048, 4096, 8192};
    long[] times = {SystemTime.getInstance().milliseconds(), SystemTime.getInstance().milliseconds() - 1,
        SystemTime.getInstance().milliseconds() + TimeUnit.SECONDS.toMillis(5), Utils.Infinite_Time};
    MessageMetadata[] messageMetadata = new MessageMetadata[4];
    messageMetadata[0] = new MessageMetadata(ByteBuffer.wrap(getRandomBytes(100)));
    messageMetadata[1] = new MessageMetadata(null);
    messageMetadata[2] = null;
    messageMetadata[3] = new MessageMetadata(ByteBuffer.wrap(getRandomBytes(200)));

    List<MessageInfo> messageInfoList = new ArrayList<>(4);
    List<MessageMetadata> messageMetadataList = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      messageInfoList.add(
          new MessageInfo(keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], isUndeletedVals[i], times[i],
              crcs[i], accountIds[i], containerIds[i], times[i], lifeVersionVals[i]));
      messageMetadataList.add(messageMetadata[i]);
    }

    // Serialize and then deserialize
    MessageInfoAndMetadataListSerde messageInfoAndMetadataListSerde =
        new MessageInfoAndMetadataListSerde(messageInfoList, messageMetadataList, serDeVersion);
    ByteBuffer buffer = ByteBuffer.allocate(messageInfoAndMetadataListSerde.getMessageInfoAndMetadataListSize());
    messageInfoAndMetadataListSerde.serializeMessageInfoAndMetadataList(buffer);
    buffer.flip();
    if (serDeVersion >= MessageInfoAndMetadataListSerde.VERSION_5) {
      // should fail if the wrong version is provided
      try {
        MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(
            new DataInputStream(new ByteBufferInputStream(buffer)), mockMap, (short) (serDeVersion - 1));
        Assert.fail("Should have failed to deserialize");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do
      }
    }
    buffer.rewind();
    MessageInfoAndMetadataListSerde messageInfoAndMetadataList =
        MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(
            new DataInputStream(new ByteBufferInputStream(buffer)), mockMap, serDeVersion);

    // Verify
    List<MessageInfo> responseMessageInfoList = messageInfoAndMetadataList.getMessageInfoList();
    List<MessageMetadata> responseMessageMetadataList = messageInfoAndMetadataList.getMessageMetadataList();
    Assert.assertEquals(4, responseMessageInfoList.size());
    Assert.assertEquals(4, responseMessageMetadataList.size());
    for (int i = 0; i < 4; i++) {
      assertMessageInfoEquality(messageInfoList.get(i), responseMessageInfoList.get(i));
      assertMessageMetadataEquality(messageMetadataList.get(i), responseMessageMetadataList.get(i));
    }
  }

  private void assertMessageInfoEquality(MessageInfo exp, MessageInfo act) {
    short version =
        serDeVersion == MessageInfoAndMetadataListSerde.DETERMINE_VERSION ? MessageInfoAndMetadataListSerde.AUTO_VERSION
            : serDeVersion;
    Assert.assertEquals(exp.getExpirationTimeInMs(), act.getExpirationTimeInMs());
    Assert.assertEquals(exp.getSize(), act.getSize());
    Assert.assertEquals(exp.getStoreKey(), act.getStoreKey());
    Assert.assertEquals(exp.isDeleted(), act.isDeleted());
    Assert.assertEquals(exp.isExpired(), act.isExpired());
    if (version >= MessageInfoAndMetadataListSerde.VERSION_2) {
      Assert.assertEquals(exp.getCrc(), act.getCrc());
    } else {
      Assert.assertNull("Crc should be null for version < 2", act.getCrc());
    }
    if (version >= MessageInfoAndMetadataListSerde.VERSION_3) {
      Assert.assertEquals(exp.getAccountId(), act.getAccountId());
      Assert.assertEquals(exp.getContainerId(), act.getContainerId());
      Assert.assertEquals(exp.getOperationTimeMs(), act.getOperationTimeMs());
    } else {
      Assert.assertEquals("Account ID should be unknown", Account.UNKNOWN_ACCOUNT_ID, act.getAccountId());
      Assert.assertEquals("Container ID should be unknown", Container.UNKNOWN_CONTAINER_ID, act.getContainerId());
      Assert.assertEquals("Operation time should be unknown", Utils.Infinite_Time, act.getOperationTimeMs());
    }
    if (version >= MessageInfoAndMetadataListSerde.VERSION_5) {
      Assert.assertEquals("TtlUpdated not as expected", exp.isTtlUpdated(), act.isTtlUpdated());
    } else {
      Assert.assertFalse("TtlUpdated should be false for version < 5", act.isTtlUpdated());
    }
    if (version >= MessageInfoAndMetadataListSerde.VERSION_6) {
      Assert.assertEquals("Undelete not as expected", exp.isUndeleted(), act.isUndeleted());
      Assert.assertEquals("LifeVersion not as expected", exp.getLifeVersion(), act.getLifeVersion());
    } else {
      Assert.assertEquals("Undelete not as expected", false, act.isUndeleted());
      Assert.assertEquals("LifeVersion not as expected", (short) 0, act.getLifeVersion());
    }
  }

  private void assertMessageMetadataEquality(MessageMetadata expected, MessageMetadata actual) {
    short version =
        serDeVersion == MessageInfoAndMetadataListSerde.DETERMINE_VERSION ? MessageInfoAndMetadataListSerde.AUTO_VERSION
            : serDeVersion;
    if (version >= MessageInfoAndMetadataListSerde.VERSION_4) {
      if (expected == null) {
        Assert.assertNull(actual);
      } else {
        if (expected.getEncryptionKey() == null) {
          // null encryption keys come in as empty.
          Assert.assertEquals(0, actual.getEncryptionKey().remaining());
        } else {
          Assert.assertEquals(expected.getEncryptionKey().rewind(), actual.getEncryptionKey());
        }
        Assert.assertEquals(expected.getVersion(), actual.getVersion());
      }
    } else {
      Assert.assertNull("MessageMetadata must null for versions < 4", actual);
    }
  }
}
