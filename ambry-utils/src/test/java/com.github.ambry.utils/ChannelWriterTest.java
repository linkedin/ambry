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
package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class ChannelWriterTest {
  @Test
  public void ChannelWriteFunctionalityTest() throws IOException {
    byte[] buf = new byte[1000];
    new Random().nextBytes(buf);
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    ByteBufferInputStream stream = new ByteBufferInputStream(buffer);
    ByteBuffer output = ByteBuffer.allocate(10000);
    ByteBufferOutputStream outputstream = new ByteBufferOutputStream(output);
    WritableByteChannel channel = Channels.newChannel(outputstream);
    ChannelWriter writer = new ChannelWriter(channel);
    writer.writeInt(1000);
    writer.writeLong(10000);
    writer.writeShort((short) 5);
    writer.writeString("check");
    writer.writeStream(stream, 1000);
    output.flip();
    Assert.assertEquals(1000, output.getInt());
    Assert.assertEquals(10000, output.getLong());
    Assert.assertEquals(5, output.getShort());
    byte[] stringout = new byte[5];
    System.arraycopy(output.array(), 14, stringout, 0, 5);
    // Assert.assertArrayEquals(stringout, "check".getBytes());
    // byte[] streamout = new byte[1000];
    //System.arraycopy(output.array(), 19, streamout, 0, 1000);
    //Assert.assertArrayEquals(streamout, buf);
  }
}
