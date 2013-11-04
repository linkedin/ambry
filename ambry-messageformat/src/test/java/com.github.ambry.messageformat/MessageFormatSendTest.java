package com.github.ambry.messageformat;

import com.github.ambry.store.MessageReadSet;
import com.github.ambry.utils.ByteBufferOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Random;

public class MessageFormatSendTest {

  class MockMessageReadSet implements MessageReadSet {

    ArrayList<ByteBuffer> buffers;

    public MockMessageReadSet(ArrayList<ByteBuffer> buffers) {
      this.buffers = buffers;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
      buffers.get(index).position((int)relativeOffset);
      buffers.get(index).limit((int)Math.min(buffers.get(index).limit(), relativeOffset + maxSize));
      int written = channel.write(buffers.get(index));
      buffers.get(index).clear();
      return written;
    }

    @Override
    public int count() {
      return buffers.size();
    }

    @Override
    public long sizeInBytes(int index) {
      return buffers.get(index).remaining();
    }
  }

  @Test
  public void sendWriteTest() throws IOException {
    // create three list of buffers each of size 1000 bytes

    // add header,system metadata, user metadata and data to the buffers
    ByteBuffer buf1 = ByteBuffer.allocate(1000);
    // fill header
    buf1.putShort((short)1);                    // version
    buf1.putLong(1000);                          // total size
    // put relative offsets
    buf1.putInt(50);                           // system metadata relative offset
    buf1.putInt(71);                           // user metadata relative offset
    buf1.putInt(181);                          // data relative offset
    buf1.putInt(123);                          // crc
    String id = new String("012345678910123456789012");     // blob id
    buf1.put(id.getBytes());

    buf1.putShort((short)0); // system metadata version
    String attribute1 = "ttl";
    String attribute2 = "del";
    buf1.put(attribute1.getBytes()); // ttl name
    buf1.putLong(12345);             // ttl value
    buf1.put(attribute2.getBytes()); // delete name
    byte b = 1;
    buf1.put(b);      // delete flag
    buf1.putInt(456); //crc

    buf1.putShort((short)0); // user metadata version
    buf1.putInt(100);
    byte[] usermetadata = new byte[100];
    new Random().nextBytes(usermetadata);
    buf1.put(usermetadata);
    buf1.putInt(123);

    buf1.putShort((short)0); // data version
    buf1.putLong(805);       // data size
    byte[] data = new byte[805];         // data
    new Random().nextBytes(data);
    buf1.put(data);
    buf1.putInt(123);                    // data crc
    buf1.flip();

    ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
    listbuf.add(buf1);
    MessageReadSet readSet = new MockMessageReadSet(listbuf);

    // get all
    MessageFormatSend send = new MessageFormatSend(readSet, MessageFormatFlags.All);
    Assert.assertEquals(send.sizeInBytes(), 1000);
    ByteBuffer bufresult = ByteBuffer.allocate(1000);
    WritableByteChannel channel1 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel1);
    }
    Assert.assertArrayEquals(buf1.array(), bufresult.array());

    // get data
    MessageFormatSend send1 = new MessageFormatSend(readSet, MessageFormatFlags.Data);
    Assert.assertEquals(send1.sizeInBytes(), 819);
    bufresult.clear();
    WritableByteChannel channel2 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send1.isSendComplete()) {
      send1.writeTo(channel2);
    }

    for (int i = 10; i < 815; i++) {
      Assert.assertEquals(data[i - 10], bufresult.array()[i]);
    }

    // get user metadata
    MessageFormatSend send2 = new MessageFormatSend(readSet, MessageFormatFlags.UserMetadata);
    Assert.assertEquals(send2.sizeInBytes(), 110);
    bufresult.clear();
    WritableByteChannel channel3 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send2.isSendComplete()) {
      send2.writeTo(channel2);
    }

    for (int i = 6; i < 102; i++) {
      Assert.assertEquals(usermetadata[i - 6], bufresult.array()[i]);
    }

    // get system metadata

  }
}
