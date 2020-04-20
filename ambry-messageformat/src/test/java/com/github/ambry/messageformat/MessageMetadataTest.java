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
package com.github.ambry.messageformat;

import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;


public class MessageMetadataTest {
  /**
   * Test instantiation and serialization/deserialization of MessageMetadata.
   * @throws Exception
   */
  @Test
  public void testInstantiationAndSerDe() throws Exception {
    ByteBuffer encryptionKey = ByteBuffer.wrap(TestUtils.getRandomBytes(256));
    MessageMetadata messageMetadata = new MessageMetadata(encryptionKey.duplicate());
    ByteBuf serializedBuf = Unpooled.buffer(messageMetadata.sizeInBytes());
    messageMetadata.serializeMessageMetadata(serializedBuf);
    MessageMetadata deserialized =
        MessageMetadata.deserializeMessageMetadata(new DataInputStream(new ByteBufInputStream(serializedBuf)));
    Assert.assertEquals(encryptionKey, deserialized.getEncryptionKey());
  }
}
