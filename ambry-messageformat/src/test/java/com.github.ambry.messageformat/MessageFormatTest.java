package com.github.ambry.messageformat;

import java.io.BufferedInputStream;
import java.nio.ByteBuffer;

import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Test;

public class MessageFormatTest {
  @Test
  public void deserializeTest() {
    try {
      BlobProperties properties = new BlobProperties(1234, true, "test", "member", "parent", 1234, "id");
      ByteBuffer stream = ByteBuffer.allocate(BlobPropertySerDe.getBlobPropertySize(properties));
      BlobPropertySerDe.putBlobPropertyToBuffer(stream, properties);
      BlobProperties result = MessageFormat.deserializeBlobProperties(new ByteBufferInputStream(stream));
      Assert.assertEquals(properties.getBlobSize(), result.getBlobSize());
      Assert.assertEquals(properties.getContentType(), result.getContentType());
      Assert.assertEquals(properties.getCreationTimeInMs(), result.getCreationTimeInMs());
      Assert.assertEquals(properties.getMemberId(), result.getMemberId());
      Assert.assertEquals(properties.getServiceId(), result.getServiceId());
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
   }
}
