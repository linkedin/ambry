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
