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

import com.github.ambry.utils.Crc32;
import java.nio.ByteBuffer;
import java.util.Random;


public class MessageFormatTestUtils {

  /**
   * Creates a test data for metadata blob content, followed by creating the actual blob record V2
   * @param blobSize size of the metadata content
   * @return entire blob content as a {@link ByteBuffer}
   */
  public static ByteBuffer getBlobContentForMetadataBlob(int blobSize) {
    ByteBuffer blobContent = ByteBuffer.allocate(blobSize);
    new Random().nextBytes(blobContent.array());
    int size = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobSize);
    ByteBuffer entireBlob = ByteBuffer.allocate(size);
    MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(entireBlob, blobSize, BlobType.MetadataBlob);
    entireBlob.put(blobContent);
    Crc32 crc = new Crc32();
    crc.update(entireBlob.array(), 0, entireBlob.position());
    entireBlob.putLong(crc.getValue());
    entireBlob.flip();
    return entireBlob;
  }
}
