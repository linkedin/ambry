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

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test hard-delete input stream message format.
 */
public class HardDeleteMessageFormatInputStreamTest {

  /**
   * Majority of the logic resides in the constructor. Test constructor to make sure it does not throw.
   */
  @Test
  public void testConstructor() throws MessageFormatException, IOException {
    HardDeleteMessageFormatInputStream inputStream = new HardDeleteMessageFormatInputStream(1000,
        MessageFormatRecord.UserMetadata_Version_V1, 100, MessageFormatRecord.Blob_Version_V3,
        BlobType.DataBlob, 2000);

    Assert.assertTrue(inputStream.streamLength > 0);
  }
}
