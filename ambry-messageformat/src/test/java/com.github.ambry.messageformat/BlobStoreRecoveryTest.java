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
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


class MockId extends StoreKey {

  private String id;
  private static final int Id_Size_In_Bytes = 2;

  public MockId(String id) {
    this.id = id;
  }

  public MockId(DataInputStream stream) throws IOException {
    id = Utils.readShortString(stream);
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(Id_Size_In_Bytes + id.length());
    idBuf.putShort((short) id.length());
    idBuf.put(id.getBytes());
    return idBuf.array();
  }

  @Override
  public String getID() {
    return toString();
  }

  @Override
  public String getLongForm() {
    return getID();
  }

  @Override
  public short getAccountId() {
    return -1;
  }

  @Override
  public short getContainerId() {
    return -1;
  }

  @Override
  public short sizeInBytes() {
    return (short) (Id_Size_In_Bytes + id.length());
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null) {
      throw new NullPointerException();
    }
    MockId otherId = (MockId) o;
    return id.compareTo(otherId.id);
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{id});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MockId other = (MockId) obj;

    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }
}

class MockIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getStoreKey(DataInputStream value) throws IOException {
    return new MockId(value);
  }
}

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
        new Object[][]{{MessageFormatRecord.Message_Header_Version_V1}, {MessageFormatRecord.Message_Header_Version_V2}});
  }

  public BlobStoreRecoveryTest(short headerVersionToUse) {
    MessageFormatRecord.headerVersionToUse = headerVersionToUse;
  }

  public class ReadImp implements Read {

    ByteBuffer buffer;
    public StoreKey[] keys = {new MockId("id1"), new MockId("id2"), new MockId("id3"), new MockId("id4")};
    long expectedExpirationTimeMs = 0;

    public void initialize() throws MessageFormatException, IOException {
      // write 3 new blob messages, and delete update messages. write the last
      // message that is partial
      byte[] encryptionKey = new byte[256];
      byte[] usermetadata = new byte[2000];
      byte[] blob = new byte[4000];
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(blob);
      TestUtils.RANDOM.nextBytes(encryptionKey);
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      long deletionTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();

      // 1st message
      BlobProperties blobProperties =
          new BlobProperties(4000, "test", "mem1", "img", false, 9999, accountId, containerId, true);
      expectedExpirationTimeMs =
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
      PutMessageFormatInputStream msg1 =
          new PutMessageFormatInputStream(keys[0], ByteBuffer.wrap(encryptionKey), blobProperties,
              ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);

      // 2nd message
      PutMessageFormatInputStream msg2 = new PutMessageFormatInputStream(keys[1], ByteBuffer.wrap(encryptionKey),
          new BlobProperties(4000, "test", accountId, containerId, false), ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);

      // 3rd message (null encryption key)
      PutMessageFormatInputStream msg3 = new PutMessageFormatInputStream(keys[2], null,
          new BlobProperties(4000, "test", accountId, containerId, false), ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);

      // 4th message
      DeleteMessageFormatInputStream msg4 =
          new DeleteMessageFormatInputStream(keys[1], accountId, containerId, deletionTimeMs);

      // 5th message
      PutMessageFormatInputStream msg5 = new PutMessageFormatInputStream(keys[3], ByteBuffer.wrap(encryptionKey),
          new BlobProperties(4000, "test", accountId, containerId, false), ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);

      buffer = ByteBuffer.allocate(
          (int) (msg1.getSize() + msg2.getSize() + msg3.getSize() + msg4.getSize() + msg5.getSize() / 2));

      writeToBuffer(msg1, (int) msg1.getSize());
      writeToBuffer(msg2, (int) msg2.getSize());
      writeToBuffer(msg3, (int) msg3.getSize());
      writeToBuffer(msg4, (int) msg4.getSize());
      writeToBuffer(msg5, (int) msg5.getSize() / 2);
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
  public void recoveryTest() throws MessageFormatException, IOException {
    MessageStoreRecovery recovery = new BlobStoreRecovery();
    // create log and write to it
    ReadImp readrecovery = new ReadImp();
    readrecovery.initialize();
    List<MessageInfo> recoveredMessages =
        recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());
    Assert.assertEquals(recoveredMessages.size(), 4);
    Assert.assertEquals(recoveredMessages.get(0).getStoreKey(), readrecovery.keys[0]);
    Assert.assertEquals(recoveredMessages.get(0).getExpirationTimeInMs(), readrecovery.expectedExpirationTimeMs);
    Assert.assertEquals(recoveredMessages.get(1).getStoreKey(), readrecovery.keys[1]);
    Assert.assertEquals(recoveredMessages.get(2).getStoreKey(), readrecovery.keys[2]);
    Assert.assertEquals(recoveredMessages.get(3).getStoreKey(), readrecovery.keys[1]);
    Assert.assertEquals(recoveredMessages.get(3).isDeleted(), true);
  }
}
