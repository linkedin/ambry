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

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageStoreRecovery;
import com.github.ambry.store.MockId;
import com.github.ambry.store.MockIdFactory;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class BlobStoreRecoveryTest {
  private static short messageFormatHeaderVersionSaved;

  @BeforeClass
  public static void saveMessageFormatHeaderVersionToUse() {
    messageFormatHeaderVersionSaved = MessageFormatRecord.headerVersionToUse;
  }

  @After
  public void resetMessageFormatHeaderVersionToUse() {
    MessageFormatRecord.headerVersionToUse = messageFormatHeaderVersionSaved;
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{MessageFormatRecord.Message_Header_Version_V1}, {MessageFormatRecord.Message_Header_Version_V2},
            {MessageFormatRecord.Message_Header_Version_V3}});
  }

  public BlobStoreRecoveryTest(short headerVersionToUse) {
    MessageFormatRecord.headerVersionToUse = headerVersionToUse;
  }

  public class ReadImp implements Read {
    private final boolean withPartialRecord;
    final List<Long> sizes = new ArrayList<>();
    final List<Long> startOffsets = new ArrayList<>();
    ByteBuffer buffer;
    public MockId[] keys =
        {new MockId("id1"), new MockId("id2"), new MockId("id3"), new MockId("id4"), new MockId("id5"), new MockId(
            "id6")};
    long expectedExpirationTimeMs = 0;

    ReadImp(boolean withPartialRecord) {
      this.withPartialRecord = withPartialRecord;
    }

    public void initialize() throws MessageFormatException, IOException {
      // write 3 new blob messages, and delete update messages. write the last
      // message that is partial
      byte[] encryptionKey = new byte[256];
      byte[] usermetadata = new byte[2000];
      byte[] blob = new byte[4000];
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(blob);
      TestUtils.RANDOM.nextBytes(encryptionKey);
      long updateTimeInMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();

      long offsetOfMessage = 0L;
      startOffsets.add(offsetOfMessage);
      // 1st message
      BlobProperties blobProperties =
          new BlobProperties(4000, "test", "mem1", "img", false, 9999, keys[0].getAccountId(), keys[0].getContainerId(),
              true, null, null, null);
      expectedExpirationTimeMs =
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
      PutMessageFormatInputStream msg1 =
          new PutMessageFormatInputStream(keys[0], ByteBuffer.wrap(encryptionKey), blobProperties,
              ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      sizes.add(msg1.getSize());

      // 2nd message
      offsetOfMessage += msg1.getSize();
      startOffsets.add(offsetOfMessage);
      PutMessageFormatInputStream msg2 = new PutMessageFormatInputStream(keys[1], ByteBuffer.wrap(encryptionKey),
          new BlobProperties(4000, "test", keys[1].getAccountId(), keys[1].getContainerId(), false),
          ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      sizes.add(msg2.getSize());

      // 3rd message (null encryption key)
      offsetOfMessage += msg2.getSize();
      startOffsets.add(offsetOfMessage);
      PutMessageFormatInputStream msg3 = new PutMessageFormatInputStream(keys[2], null,
          new BlobProperties(4000, "test", keys[2].getAccountId(), keys[2].getContainerId(), false),
          ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      sizes.add(msg3.getSize());

      // 4th message
      offsetOfMessage += msg3.getSize();
      startOffsets.add(offsetOfMessage);
      MessageFormatInputStream msg4;
      if (MessageFormatRecord.headerVersionToUse >= MessageFormatRecord.Message_Header_Version_V2) {
        msg4 = new TtlUpdateMessageFormatInputStream(keys[1], keys[1].getAccountId(), keys[1].getContainerId(),
            Utils.Infinite_Time, updateTimeInMs);
      } else {
        msg4 = new PutMessageFormatInputStream(keys[3], ByteBuffer.wrap(encryptionKey),
            new BlobProperties(4000, "test", keys[3].getAccountId(), keys[3].getContainerId(), false),
            ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      }
      sizes.add(msg4.getSize());

      // 5th message
      offsetOfMessage += msg4.getSize();
      startOffsets.add(offsetOfMessage);
      DeleteMessageFormatInputStream msg5 =
          new DeleteMessageFormatInputStream(keys[1], keys[1].getAccountId(), keys[1].getContainerId(), updateTimeInMs);
      sizes.add(msg5.getSize());

      // 6th message
      offsetOfMessage += msg5.getSize();
      startOffsets.add(offsetOfMessage);
      MessageFormatInputStream msg6;
      if (MessageFormatRecord.headerVersionToUse >= MessageFormatRecord.Message_Header_Version_V2) {
        msg6 = new TtlUpdateMessageFormatInputStream(keys[0], keys[0].getAccountId(), keys[0].getContainerId(),
            Utils.Infinite_Time, updateTimeInMs);
      } else {
        msg6 = new PutMessageFormatInputStream(keys[4], ByteBuffer.wrap(encryptionKey),
            new BlobProperties(4000, "test", keys[4].getAccountId(), keys[4].getContainerId(), false),
            ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      }
      sizes.add(msg6.getSize());

      PutMessageFormatInputStream msg7 = null;
      if (withPartialRecord) {
        // 7th message
        offsetOfMessage += msg6.getSize();
        startOffsets.add(offsetOfMessage);
        msg7 = new PutMessageFormatInputStream(keys[5], ByteBuffer.wrap(encryptionKey),
            new BlobProperties(4000, "test", keys[5].getAccountId(), keys[5].getContainerId(), false),
            ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
        sizes.add(msg7.getSize());
      }

      int bufferSize =
          (int) (msg1.getSize() + msg2.getSize() + msg3.getSize() + msg4.getSize() + msg5.getSize() + msg6.getSize());
      if (withPartialRecord) {
        bufferSize += msg7.getSize() / 2;
      }
      buffer = ByteBuffer.allocate(bufferSize);

      writeToBuffer(msg1, (int) msg1.getSize());
      writeToBuffer(msg2, (int) msg2.getSize());
      writeToBuffer(msg3, (int) msg3.getSize());
      writeToBuffer(msg4, (int) msg4.getSize());
      writeToBuffer(msg5, (int) msg5.getSize());
      writeToBuffer(msg6, (int) msg6.getSize());
      if (withPartialRecord) {
        writeToBuffer(msg7, (int) msg7.getSize() / 2);
      }
      buffer.position(0);
    }

    private void writeToBuffer(MessageFormatInputStream stream, int sizeToWrite) throws IOException {
      long sizeWritten = 0;
      while (sizeWritten < sizeToWrite) {
        int read = stream.read(buffer.array(), buffer.position(), (int) sizeToWrite);
        sizeWritten += read;
        buffer.position(buffer.position() + (int) sizeWritten);
      }
    }

    @Override
    public void readInto(ByteBuffer bufferToWrite, long position) throws IOException {
      bufferToWrite.put(buffer.array(), (int) position, bufferToWrite.remaining());
    }

    public int getSize() {
      return buffer.capacity();
    }
  }

  @Test
  public void successRecoveryTest() throws MessageFormatException, IOException {
    MessageStoreRecovery recovery = new BlobStoreRecovery();
    // create log and write to it
    ReadImp readrecovery = new ReadImp(false);
    readrecovery.initialize();
    MessageStoreRecovery.RecoveryResult recoveryResult =
        recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());
    Assert.assertNull(recoveryResult.recoveryException);
    Assert.assertEquals(readrecovery.getSize(), recoveryResult.currentStartOffset);
    List<MessageInfo> recoveredMessages = recoveryResult.recovered;
    Assert.assertEquals(recoveredMessages.size(), 6);
    verifyInfo(recoveredMessages.get(0), readrecovery.keys[0], readrecovery.sizes.get(0),
        readrecovery.expectedExpirationTimeMs, false, false);
    verifyInfo(recoveredMessages.get(1), readrecovery.keys[1], readrecovery.sizes.get(1), Utils.Infinite_Time, false,
        false);
    verifyInfo(recoveredMessages.get(2), readrecovery.keys[2], readrecovery.sizes.get(2), Utils.Infinite_Time, false,
        false);
    verifyInfo(recoveredMessages.get(4), readrecovery.keys[1], readrecovery.sizes.get(4), Utils.Infinite_Time, true,
        false);
    if (MessageFormatRecord.headerVersionToUse >= MessageFormatRecord.Message_Header_Version_V2) {
      verifyInfo(recoveredMessages.get(3), readrecovery.keys[1], readrecovery.sizes.get(3), Utils.Infinite_Time, false,
          true);
      verifyInfo(recoveredMessages.get(5), readrecovery.keys[0], readrecovery.sizes.get(5), Utils.Infinite_Time, false,
          true);
    } else {
      verifyInfo(recoveredMessages.get(3), readrecovery.keys[3], readrecovery.sizes.get(3), Utils.Infinite_Time, false,
          false);
      verifyInfo(recoveredMessages.get(5), readrecovery.keys[4], readrecovery.sizes.get(5), Utils.Infinite_Time, false,
          false);
    }
  }

  @Test
  public void partialMessageRecoveryTest() throws MessageFormatException, IOException {
    Assume.assumeTrue(MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V3);

    MessageStoreRecovery recovery = new BlobStoreRecovery();
    // create log and write to it
    ReadImp readrecovery = new ReadImp(true);
    readrecovery.initialize();
    MessageStoreRecovery.RecoveryResult recoveryResult =
        recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());

    Assert.assertNotNull(recoveryResult.recoveryException);
    Assert.assertEquals(StoreErrorCodes.LogFileFormatError, recoveryResult.recoveryException.getErrorCode());
    Assert.assertEquals(readrecovery.startOffsets.get(6).longValue(), recoveryResult.currentStartOffset);
    List<MessageInfo> recoveredMessages = recoveryResult.recovered;
    // Last message is a partial message and it shouldn't be recovered
    Assert.assertEquals(recoveredMessages.size(), 6);
    verifyInfo(recoveredMessages.get(0), readrecovery.keys[0], readrecovery.sizes.get(0),
        readrecovery.expectedExpirationTimeMs, false, false);
    verifyInfo(recoveredMessages.get(1), readrecovery.keys[1], readrecovery.sizes.get(1), Utils.Infinite_Time, false,
        false);
    verifyInfo(recoveredMessages.get(2), readrecovery.keys[2], readrecovery.sizes.get(2), Utils.Infinite_Time, false,
        false);
    verifyInfo(recoveredMessages.get(4), readrecovery.keys[1], readrecovery.sizes.get(4), Utils.Infinite_Time, true,
        false);
    verifyInfo(recoveredMessages.get(3), readrecovery.keys[1], readrecovery.sizes.get(3), Utils.Infinite_Time, false,
        true);
    verifyInfo(recoveredMessages.get(5), readrecovery.keys[0], readrecovery.sizes.get(5), Utils.Infinite_Time, false,
        true);
  }

  @Test
  public void unknownHeaderVersionRecoveryTest() throws MessageFormatException, IOException {
    Assume.assumeTrue(MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V3);

    MessageStoreRecovery recovery = new BlobStoreRecovery();
    // create log and write to it
    ReadImp readrecovery = new ReadImp(true);
    readrecovery.initialize();
    // Second message is put message, let's change the version of header to something we don't recognize
    long secondMessageStartOffset = readrecovery.startOffsets.get(1).longValue();
    readrecovery.buffer.putShort((int) secondMessageStartOffset, (short) 100);

    MessageStoreRecovery.RecoveryResult recoveryResult =
        recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());

    Assert.assertNotNull(recoveryResult.recoveryException);
    Assert.assertEquals(StoreErrorCodes.LogFileFormatError, recoveryResult.recoveryException.getErrorCode());
    Assert.assertEquals(readrecovery.startOffsets.get(1).longValue(), recoveryResult.currentStartOffset);
    List<MessageInfo> recoveredMessages = recoveryResult.recovered;
    Assert.assertEquals(recoveredMessages.size(), 1);

    verifyInfo(recoveredMessages.get(0), readrecovery.keys[0], readrecovery.sizes.get(0),
        readrecovery.expectedExpirationTimeMs, false, false);
  }

  @Test
  public void crcErrorRecoveryTest() throws MessageFormatException, IOException {
    Assume.assumeTrue(MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V3);

    MessageStoreRecovery recovery = new BlobStoreRecovery();
    // create log and write to it
    ReadImp readrecovery = new ReadImp(true);
    readrecovery.initialize();
    // Second message is put message, let's change the crc value
    long secondMessageCRCOffset = readrecovery.startOffsets.get(2).longValue() - 8;
    byte b = readrecovery.buffer.get((int) secondMessageCRCOffset);
    readrecovery.buffer.put((int) secondMessageCRCOffset, (byte) ~b);

    MessageStoreRecovery.RecoveryResult recoveryResult =
        recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());

    Assert.assertNotNull(recoveryResult.recoveryException);
    Assert.assertEquals(StoreErrorCodes.LogFileFormatError, recoveryResult.recoveryException.getErrorCode());
    List<MessageInfo> recoveredMessages = recoveryResult.recovered;
    Assert.assertEquals(recoveredMessages.size(), 1);
    Assert.assertEquals(readrecovery.startOffsets.get(1).longValue(), recoveryResult.currentStartOffset);
    verifyInfo(recoveredMessages.get(0), readrecovery.keys[0], readrecovery.sizes.get(0),
        readrecovery.expectedExpirationTimeMs, false, false);
  }

  /**
   * Verifies that {@code info} has details as provided.
   * @param info the {@link MessageInfo} to check
   * @param id the expected {@link com.github.ambry.store.StoreKey}
   * @param size the expected size
   * @param expiresAtMs the expected expiry time in ms
   * @param isDeleted the expected delete state
   * @param isTtlUpdated the expected ttl update state
   */
  private void verifyInfo(MessageInfo info, MockId id, long size, long expiresAtMs, boolean isDeleted,
      boolean isTtlUpdated) {
    Assert.assertEquals("StoreKey not as expected", id, info.getStoreKey());
    Assert.assertEquals("Size not as expected", size, info.getSize());
    Assert.assertEquals("ExpiresAtMs not as expected", expiresAtMs, info.getExpirationTimeInMs());
    Assert.assertEquals("isDeleted not as expected", isDeleted, info.isDeleted());
    Assert.assertEquals("isTtlUpdated not as expected", isTtlUpdated, info.isTtlUpdated());
    Assert.assertEquals("AccountId not as expected", id.getAccountId(), info.getAccountId());
    Assert.assertEquals("ContainerId not as expected", id.getContainerId(), info.getContainerId());
  }
}
