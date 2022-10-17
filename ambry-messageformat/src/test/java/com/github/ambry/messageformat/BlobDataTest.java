/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test Blob Data structure.  It's a simple structure with the majority of logic in constructor.
 */
public class BlobDataTest {

  @Test
  public void testBlobDataConstructor() {
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4});
    BlobData data = new BlobData(BlobType.MetadataBlob, 100, buf);
    Assert.assertEquals(BlobType.MetadataBlob, data.getBlobType());
    Assert.assertEquals(100, data.getSize());
    Assert.assertFalse(data.isCompressed());

    // Test the other constructor.
    data = new BlobData(BlobType.MetadataBlob, 100, buf, true);
    Assert.assertTrue(data.isCompressed());
  }
}
