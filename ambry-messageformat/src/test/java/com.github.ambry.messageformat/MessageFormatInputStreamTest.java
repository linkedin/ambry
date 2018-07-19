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
package com.github.ambry.messageformat;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.store.MockId;
import com.github.ambry.store.MockIdFactory;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class MessageFormatInputStreamTest {
  private static short messageFormatHeaderVersionSaved;

  @BeforeClass
  public static void saveMessageFormatHeaderVersionToUse() {
    messageFormatHeaderVersionSaved = MessageFormatRecord.headerVersionToUse;
  }

  @After
  public void resetMessageFormatHeaderVersionToUse() {
    MessageFormatRecord.headerVersionToUse = messageFormatHeaderVersionSaved;
  }

  /**
   * Tests for {@link PutMessageFormatInputStream} in different versions.
   */
  @Test
  public void messageFormatRecordsTest() throws IOException, MessageFormatException {
    messageFormatRecordsTest(MessageFormatRecord.Blob_Version_V1, BlobType.DataBlob, false);
    messageFormatRecordsTest(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob, false);
    messageFormatRecordsTest(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob, false);
    messageFormatRecordsTest(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob, true);
    messageFormatRecordsTest(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob, true);
  }

  private void messageFormatRecordsTest(short blobVersion, BlobType blobType, boolean useV2Header)
      throws IOException, MessageFormatException {
    StoreKey key = new MockId("id1");
    StoreKeyFactory keyFactory = new MockIdFactory();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop = new BlobProperties(10, "servid", accountId, containerId, false);
    byte[] encryptionKey = new byte[100];
    new Random().nextBytes(encryptionKey);
    byte[] usermetadata = new byte[1000];
    new Random().nextBytes(usermetadata);
    int blobContentSize = 2000;
    byte[] data = new byte[blobContentSize];
    new Random().nextBytes(data);
    long blobSize = -1;
    MessageFormatRecord.headerVersionToUse =
        useV2Header ? MessageFormatRecord.Message_Header_Version_V2 : MessageFormatRecord.Message_Header_Version_V1;
    if (blobVersion == MessageFormatRecord.Blob_Version_V1) {
      blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data = byteBufferBlob.array();
      blobContentSize = data.length;
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }

    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(data));

    MessageFormatInputStream messageFormatStream =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key,
            ByteBuffer.wrap(encryptionKey), prop, ByteBuffer.wrap(usermetadata), stream, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key, prop, ByteBuffer.wrap(usermetadata), stream, blobContentSize,
                blobType);

    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(
        useV2Header ? MessageFormatRecord.Message_Header_Version_V2 : MessageFormatRecord.Message_Header_Version_V1);

    int blobEncryptionKeySize =
        useV2Header ? MessageFormatRecord.BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(
            ByteBuffer.wrap(encryptionKey)) : 0;
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata));

    Assert.assertEquals(messageFormatStream.getSize(),
        headerSize + blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize + blobSize
            + key.sizeInBytes());

    // verify header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(
        useV2Header ? MessageFormatRecord.Message_Header_Version_V2 : MessageFormatRecord.Message_Header_Version_V1,
        headerBuf.getShort());
    Assert.assertEquals(blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize + blobSize,
        headerBuf.getLong());
    if (useV2Header) {
      Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
      Assert.assertEquals(headerSize + key.sizeInBytes() + blobEncryptionKeySize, headerBuf.getInt());
      Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      Assert.assertEquals(headerSize + key.sizeInBytes() + blobEncryptionKeySize + blobPropertiesRecordSize,
          headerBuf.getInt());
      Assert.assertEquals(
          headerSize + key.sizeInBytes() + blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize,
          headerBuf.getInt());
    } else {
      Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
      Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      Assert.assertEquals(headerSize + key.sizeInBytes() + blobPropertiesRecordSize, headerBuf.getInt());
      Assert.assertEquals(headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize,
          headerBuf.getInt());
    }
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);

    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // verify encryption key
    if (useV2Header) {
      byte[] blobEncryptionKeyOutput = new byte[blobEncryptionKeySize];
      ByteBuffer blobEncryptionKeyBuf = ByteBuffer.wrap(blobEncryptionKeyOutput);
      messageFormatStream.read(blobEncryptionKeyOutput);
      Assert.assertEquals(blobEncryptionKeyBuf.getShort(), MessageFormatRecord.Blob_Encryption_Key_V1);
      Assert.assertEquals(blobEncryptionKeyBuf.getInt(), 100);
      dest = new byte[100];
      blobEncryptionKeyBuf.get(dest);
      Assert.assertArrayEquals(dest, encryptionKey);
      crc = new Crc32();
      crc.update(blobEncryptionKeyOutput, 0, blobEncryptionKeySize - MessageFormatRecord.Crc_Size);
      Assert.assertEquals(crc.getValue(), blobEncryptionKeyBuf.getLong());
    }

    // verify blob properties
    byte[] blobPropertiesOutput = new byte[blobPropertiesRecordSize];
    ByteBuffer blobPropertiesBuf = ByteBuffer.wrap(blobPropertiesOutput);
    messageFormatStream.read(blobPropertiesOutput);
    Assert.assertEquals(blobPropertiesBuf.getShort(), 1);
    BlobProperties propOutput = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(blobPropertiesBuf)));
    Assert.assertEquals(10, propOutput.getBlobSize());
    Assert.assertEquals("servid", propOutput.getServiceId());
    Assert.assertEquals("AccountId mismatch", accountId, propOutput.getAccountId());
    Assert.assertEquals("ContainerId mismatch", containerId, propOutput.getContainerId());
    crc = new Crc32();
    crc.update(blobPropertiesOutput, 0, blobPropertiesRecordSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), blobPropertiesBuf.getLong());

    // verify user metadata
    byte[] userMetadataOutput = new byte[userMetadataSize];
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadataOutput);
    messageFormatStream.read(userMetadataOutput);
    Assert.assertEquals(userMetadataBuf.getShort(), 1);
    Assert.assertEquals(userMetadataBuf.getInt(), 1000);
    dest = new byte[1000];
    userMetadataBuf.get(dest);
    Assert.assertArrayEquals(dest, usermetadata);
    crc = new Crc32();
    crc.update(userMetadataOutput, 0, userMetadataSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify blob
    CrcInputStream crcstream = new CrcInputStream(messageFormatStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), blobVersion);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2) {
      Assert.assertEquals(streamData.readShort(), blobType.ordinal());
    }
    Assert.assertEquals(streamData.readLong(), blobContentSize);
    for (int i = 0; i < blobContentSize; i++) {
      Assert.assertEquals((byte) streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());

    // Verify Blob All
    stream = new ByteBufferInputStream(ByteBuffer.wrap(data));

    messageFormatStream = (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key,
        ByteBuffer.wrap(encryptionKey), prop, ByteBuffer.wrap(usermetadata), stream, blobContentSize, blobType)
        : new PutMessageFormatBlobV1InputStream(key, prop, ByteBuffer.wrap(usermetadata), stream, blobContentSize,
            blobType);

    int totalSize;
    if (useV2Header) {
      totalSize = headerSize + key.sizeInBytes() + blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize
          + (int) blobSize;
    } else {
      totalSize = headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize + (int) blobSize;
    }
    ByteBuffer allBuf = ByteBuffer.allocate(totalSize);
    messageFormatStream.read(allBuf.array());

    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(new ByteBufferInputStream(allBuf), keyFactory);
    Assert.assertEquals(key, blobAll.getStoreKey());
    Assert.assertArrayEquals(usermetadata, blobAll.getBlobInfo().getUserMetadata());
    Assert.assertEquals(blobContentSize, blobAll.getBlobData().getSize());
    Assert.assertEquals(blobType, blobAll.getBlobData().getBlobType());
    if (useV2Header) {
      Assert.assertEquals(ByteBuffer.wrap(encryptionKey), blobAll.getBlobEncryptionKey());
    } else {
      Assert.assertEquals(null, blobAll.getBlobEncryptionKey());
    }
    Assert.assertEquals(ByteBuffer.wrap(data), blobAll.getBlobData().getStream().getByteBuffer());
  }

  /**
   * Tests for {@link DeleteMessageFormatInputStream} in different versions.
   */
  @Test
  public void messageFormatDeleteRecordTest() throws IOException, MessageFormatException {
    short[] versions =
        {MessageFormatRecord.Update_Version_V1, MessageFormatRecord.Update_Version_V2, MessageFormatRecord.Update_Version_V3};
    for (short version : versions) {
      StoreKey key = new MockId("id1");
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      long deletionTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
      MessageFormatInputStream messageFormatStream;
      boolean useV2Header;
      int deleteRecordSize;
      if (version == MessageFormatRecord.Update_Version_V1) {
        messageFormatStream = new DeleteMessageFormatV1InputStream(key, accountId, containerId, deletionTimeMs);
        deleteRecordSize = MessageFormatRecord.Update_Format_V1.getRecordSize();
        useV2Header = false;
        // reset account, container ids and time
        accountId = Account.UNKNOWN_ACCOUNT_ID;
        containerId = Container.UNKNOWN_CONTAINER_ID;
        deletionTimeMs = Utils.Infinite_Time;
      } else if (version == MessageFormatRecord.Update_Version_V2) {
        messageFormatStream = new DeleteMessageFormatInputStream(key, accountId, containerId, deletionTimeMs);
        deleteRecordSize = MessageFormatRecord.Update_Format_V2.getRecordSize();
        useV2Header = MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V2;
      } else {
        messageFormatStream = new DeleteMessageFormatV3InputStream(key, accountId, containerId, deletionTimeMs);
        deleteRecordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(UpdateRecord.Type.DELETE);
        useV2Header = MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V2;
      }
      int headerSize = MessageFormatRecord.getHeaderSizeForVersion(
          useV2Header ? MessageFormatRecord.Message_Header_Version_V2 : MessageFormatRecord.Message_Header_Version_V1);
      Assert.assertEquals(headerSize + deleteRecordSize + key.sizeInBytes(), messageFormatStream.getSize());

      // check header
      byte[] headerOutput = new byte[headerSize];
      messageFormatStream.read(headerOutput);
      ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
      Assert.assertEquals(
          useV2Header ? MessageFormatRecord.Message_Header_Version_V2 : MessageFormatRecord.Message_Header_Version_V1,
          headerBuf.getShort());
      Assert.assertEquals(deleteRecordSize, headerBuf.getLong());
      // read encryption key relative offset
      if (useV2Header) {
        Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      }
      // blob properties relative offset
      Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      // delete record relative offset. This is the only relative offset with a valid value.
      Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
      // user metadata relative offset
      Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      // blob relative offset
      Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      Crc32 crc = new Crc32();
      crc.update(headerOutput, 0, headerSize - MessageFormatRecord.Crc_Size);
      Assert.assertEquals(crc.getValue(), headerBuf.getLong());

      // verify handle
      byte[] handleOutput = new byte[key.sizeInBytes()];
      messageFormatStream.read(handleOutput);
      Assert.assertArrayEquals(handleOutput, key.toBytes());

      // check delete record
      UpdateRecord updateRecord = MessageFormatRecord.deserializeUpdateRecord(messageFormatStream);
      Assert.assertEquals("Type of update record not DELETE", UpdateRecord.Type.DELETE, updateRecord.getType());
      Assert.assertNotNull("DeleteSubRecord should not be null", updateRecord.getDeleteSubRecord());
      Assert.assertEquals("AccountId mismatch", accountId, updateRecord.getAccountId());
      Assert.assertEquals("ContainerId mismatch", containerId, updateRecord.getContainerId());
      Assert.assertEquals("DeletionTime mismatch", deletionTimeMs, updateRecord.getUpdateTimeInMs());
    }
  }

  /**
   * Test calling the no-arg read method
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void messageFormatNoArgReadTest() throws Exception, MessageFormatException {
    StoreKey key = new MockId("id1");
    StoreKeyFactory keyFactory = new MockIdFactory();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop = new BlobProperties(10, "servid", accountId, containerId, false);
    byte[] encryptionKey = new byte[100];
    new Random().nextBytes(encryptionKey);
    byte[] usermetadata = new byte[1000];
    new Random().nextBytes(usermetadata);
    int blobContentSize = 2000;
    byte[] data = new byte[blobContentSize];
    new Random().nextBytes(data);
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(data));

    MessageFormatInputStream messageFormatStream =
        new PutMessageFormatInputStream(key, ByteBuffer.wrap(encryptionKey), prop, ByteBuffer.wrap(usermetadata),
            stream, blobContentSize, BlobType.DataBlob);

    TestUtils.validateInputStreamContract(messageFormatStream);
    TestUtils.readInputStreamAndValidateSize(messageFormatStream, messageFormatStream.getSize());
  }

  /**
   * Tests for {@link TtlUpdateMessageFormatInputStream} in different versions.
   */
  @Test
  public void messageFormatTtlUpdateRecordTest() throws IOException, MessageFormatException {
    StoreKey key = new MockId("id1");
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    long ttlUpdateTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
    long updatedExpiryMs = ttlUpdateTimeMs + TestUtils.RANDOM.nextInt();
    MessageFormatInputStream messageFormatStream =
        new TtlUpdateMessageFormatInputStream(key, accountId, containerId, updatedExpiryMs, ttlUpdateTimeMs);
    long ttlUpdateRecordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(UpdateRecord.Type.TTL_UPDATE);
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    Assert.assertEquals(headerSize + ttlUpdateRecordSize + key.sizeInBytes(), messageFormatStream.getSize());
    checkTtlUpdateMessage(messageFormatStream, ttlUpdateRecordSize, key, accountId, containerId, updatedExpiryMs,
        ttlUpdateTimeMs);
  }

  /**
   * Checks a TTL update message including headers and the {@link UpdateRecord}.
   * @param stream the {@link InputStream} to read data from
   * @param expectedRecordSize the expected size of the record in the message. Can be {@code null} if unknown (won't be
   *                            checked)
   * @param key the expected {@link StoreKey}
   * @param accountId the account id expected
   * @param containerId the container id expected
   * @param updatedExpiresAtMs the expected updated expiry time
   * @param updateTimeMs the expected time of update
   * @throws IOException
   * @throws MessageFormatException
   */
  public static void checkTtlUpdateMessage(InputStream stream, Long expectedRecordSize, StoreKey key, short accountId,
      short containerId, long updatedExpiresAtMs, long updateTimeMs) throws IOException, MessageFormatException {
    checkHeaderAndStoreKeyForUpdate(stream, expectedRecordSize, key);
    checkTtlUpdateSubRecord(stream, accountId, containerId, updatedExpiresAtMs, updateTimeMs);
  }

  /**
   * Checks the header and storekey for an update message in {@code stream}
   * @param stream the {@link InputStream} to read data from
   * @param expectedRecordSize the expected size of the record in the message. Can be {@code null} if unknown (won't be
   *                            checked)
   * @param key the expected {@link StoreKey}
   * @throws IOException
   * @throws MessageFormatException
   */
  private static void checkHeaderAndStoreKeyForUpdate(InputStream stream, Long expectedRecordSize, StoreKey key)
      throws IOException, MessageFormatException {
    MessageFormatRecord.MessageHeader_Format header = MessageFormatRecordTest.getHeader(new DataInputStream(stream));
    header.verifyHeader();

    Assert.assertEquals("Version not as expected", MessageFormatRecord.headerVersionToUse, header.getVersion());
    // update record relative offset. This is the only relative offset with a valid value.
    Assert.assertEquals("Update record relative offset not as expected",
        MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse) + key.sizeInBytes(),
        header.getUpdateRecordRelativeOffset());
    if (expectedRecordSize != null) {
      Assert.assertEquals("Size of record not as expected", expectedRecordSize.longValue(), header.getMessageSize());
    }
    Assert.assertEquals("Encryption key relative offset should be invalid",
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, header.getBlobEncryptionKeyRecordRelativeOffset());
    Assert.assertEquals("Blob props relative offset should be invalid",
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, header.getBlobPropertiesRecordRelativeOffset());
    Assert.assertEquals("UM relative offset should be invalid",
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, header.getUserMetadataRecordRelativeOffset());
    Assert.assertEquals("Blob relative offset should be invalid",
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, header.getBlobRecordRelativeOffset());

    // verify StoreKey
    byte[] keyBytes = Utils.readBytesFromStream(stream, key.sizeInBytes());
    Assert.assertArrayEquals("StoreKey not as expected", key.toBytes(), keyBytes);
  }

  /**
   * Verifies the values in the {@link UpdateRecord} obtained from {@code stream}
   * @param stream the {@link InputStream} to obtain the records from
   * @param accountId the account id expected
   * @param containerId the container id expected
   * @param updatedExpiresAtMs the expected updated expiry time
   * @param updateTimeMs the expected time of update
   * @throws IOException
   * @throws MessageFormatException
   */
  private static void checkTtlUpdateSubRecord(InputStream stream, short accountId, short containerId,
      long updatedExpiresAtMs, long updateTimeMs) throws IOException, MessageFormatException {
    UpdateRecord updateRecord = MessageFormatRecord.deserializeUpdateRecord(stream);
    Assert.assertEquals("Type of update record not TTL_UPDATE", UpdateRecord.Type.TTL_UPDATE, updateRecord.getType());
    Assert.assertNotNull("TtlUpdateSubRecord should not be null", updateRecord.getTtlUpdateSubRecord());
    Assert.assertEquals("AccountId mismatch", accountId, updateRecord.getAccountId());
    Assert.assertEquals("ContainerId mismatch", containerId, updateRecord.getContainerId());
    Assert.assertEquals("Updated expiry time mismatch", updatedExpiresAtMs,
        updateRecord.getTtlUpdateSubRecord().getUpdatedExpiryTimeMs());
    Assert.assertEquals("UpdateTime mismatch", updateTimeMs, updateRecord.getUpdateTimeInMs());
  }
}
