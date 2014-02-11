package com.github.ambry.messageformat;

import com.github.ambry.store.*;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import org.junit.Test;
import org.junit.Assert;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.List;

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
    idBuf.putShort((short)id.length());
    idBuf.put(id.getBytes());
    return idBuf.array();
  }

  @Override
  public short sizeInBytes() {
    return (short)(Id_Size_In_Bytes + id.length());
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null)
      throw new NullPointerException();
    MockId otherId = (MockId)o;
    return id.compareTo(otherId.id);
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{id});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MockId other = (MockId) obj;

    if (id == null) {
      if (other.id != null)
        return false;
    }
    else if (!id.equals(other.id))
      return false;
    return true;
  }
}

class MockIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getStoreKey(DataInputStream value) throws IOException {
    return new MockId(value);
  }
}

public class BlobStoreRecoveryTest {

  public class ReadImp implements Read {

    ByteBuffer buffer;
    public StoreKey[] keys = {
            new MockId("id1"),
            new MockId("id2"),
            new MockId("id3"),
            new MockId("id4")
    };

    public void initialize() throws MessageFormatException, IOException {
      // write 3 new blob messages, and 2 ttl and delete update messages. write the last
      // message that is partial
      byte[] usermetadata = new byte[2000];
      byte[] blob = new byte[4000];
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(blob);
      long sizeToWrite = 0;

      // 1st message
      PutMessageFormatInputStream msg1 = new PutMessageFormatInputStream(keys[0],
                                                                         new BlobProperties(4000,
                                                                                            "test",
                                                                                            "mem1",
                                                                                            "img",
                                                                                            false,
                                                                                            0,
                                                                                            9999),
                                                                         ByteBuffer.wrap(usermetadata),
                                                                         new ByteBufferInputStream(ByteBuffer.wrap(blob)),
                                                                         4000);
      // 2nd message
      PutMessageFormatInputStream msg2 = new PutMessageFormatInputStream(keys[1],
              new BlobProperties(4000, "test"),
              ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)),
              4000);

      // 3rd message
      PutMessageFormatInputStream msg3 = new PutMessageFormatInputStream(keys[2],
              new BlobProperties(4000, "test"),
              ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)),
              4000);

      // 4th message
      TTLMessageFormatInputStream msg4 = new TTLMessageFormatInputStream(keys[0],
              -1);

      // 5th message
      DeleteMessageFormatInputStream msg5 = new DeleteMessageFormatInputStream(keys[1]);

      // 6th message
      PutMessageFormatInputStream msg6 = new PutMessageFormatInputStream(keys[3],
              new BlobProperties(4000, "test"),
              ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)),
              4000);

      buffer = ByteBuffer.allocate((int)(msg1.getSize() +
              msg2.getSize() +
              msg3.getSize() +
              msg4.getSize() +
              msg5.getSize() +
              msg6.getSize() / 2));


      writeToBuffer(msg1, (int)msg1.getSize());
      writeToBuffer(msg2, (int)msg2.getSize());
      writeToBuffer(msg3, (int)msg3.getSize());
      writeToBuffer(msg4, (int)msg4.getSize());
      writeToBuffer(msg5, (int)msg5.getSize());
      writeToBuffer(msg6, (int)msg6.getSize() / 2);
      buffer.position(0);
    }

    private void writeToBuffer(MessageFormatInputStream stream, int sizeToWrite) throws IOException {
      long sizeWritten = 0;
      while (sizeWritten < sizeToWrite) {
        int read = stream.read(buffer.array(), buffer.position(), (int)sizeToWrite);
        sizeWritten += read;
        buffer.position(buffer.position() + (int)sizeWritten);
      }
    }

    @Override
    public void readInto(ByteBuffer bufferToWrite, long position) throws IOException {
      bufferToWrite.put(buffer.array(), (int)position, bufferToWrite.remaining());
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
    List<MessageInfo> recoveredMessages = recovery.recover(readrecovery, 0, readrecovery.getSize(), new MockIdFactory());
    Assert.assertEquals(recoveredMessages.size(), 5);
    Assert.assertEquals(recoveredMessages.get(0).getStoreKey(), readrecovery.keys[0]);
    Assert.assertEquals(recoveredMessages.get(0).getTimeToLiveInMs(), 9999);
    Assert.assertEquals(recoveredMessages.get(1).getStoreKey(), readrecovery.keys[1]);
    Assert.assertEquals(recoveredMessages.get(2).getStoreKey(), readrecovery.keys[2]);
    Assert.assertEquals(recoveredMessages.get(3).getStoreKey(), readrecovery.keys[0]);
    Assert.assertEquals(recoveredMessages.get(3).getTimeToLiveInMs(), BlobProperties.Infinite_TTL);
    Assert.assertEquals(recoveredMessages.get(4).getStoreKey(), readrecovery.keys[1]);
    Assert.assertEquals(recoveredMessages.get(4).isDeleted(), true);
  }
}
