/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.*;


/**
 * Class to test the {@link NonBlockingRouter} for S3.
 */
@RunWith(Parameterized.class)
public class S3NonBlockingRouterTest extends NonBlockingRouterTestBase {

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @param metadataContentVersion the metadata content version to test with.
   * @param includeCloudDc {@code true} to make the local datacenter a cloud DC.
   * @throws Exception if initialization fails
   */
  public S3NonBlockingRouterTest(boolean testEncryption, int metadataContentVersion, boolean includeCloudDc)
      throws Exception {
    super(testEncryption, metadataContentVersion, includeCloudDc);
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2, false},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, false},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, false},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, false}});
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link QuotaAwareOperationController}.
   * @return the created VerifiableProperties instance.
   */
  @Override
  protected Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties =
        super.getNonBlockingRouterProperties(routerDataCenter, PUT_REQUEST_PARALLELISM, DELETE_REQUEST_PARALLELISM);
    properties.setProperty("router.operation.controller", "com.github.ambry.router.QuotaAwareOperationController");

    return properties;
  }

  /**
   * Test the PutBlob with skipCompositeChunk option.
   * We don't generate composite chunk for it.
   * Instead, we return PutBlobMetaInfo which includes the data chunk list.
   * @throws Exception
   */
  @Test
  public void testS3PartUpload() throws Exception {
    int chunkNumber = 9;
    // composite blob with "chunkNumber" of data chunks
    maxPutChunkSize = PUT_CONTENT_SIZE / chunkNumber;
    if (PUT_CONTENT_SIZE % maxPutChunkSize != 0) {
      maxPutChunkSize++;
    }
    setRouter();
    setOperationParams();

    // set skipCompositeChunk to true
    PutBlobOptions putBlobOptions = new PutBlobOptionsBuilder().skipCompositeChunk(true).build();
    String compositeBlobInfo = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putBlobOptions)
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    PutBlobMetaInfo metaInfo = PutBlobMetaInfo.deserialize(compositeBlobInfo);

    // Serialize and Deserialize, the PutBlobMetaInfo should be the same
    String metaInfoStr = metaInfo.toString();
    PutBlobMetaInfo deserializedMetaInfo = PutBlobMetaInfo.deserialize(metaInfoStr);

    // verify the deserialized PutBlobMetaInfo
    Assert.assertEquals(chunkNumber, deserializedMetaInfo.getNumChunks());
    // verify the content, one data chunk after another
    List<Pair<String, Long>> chunks = deserializedMetaInfo.getOrderedChunkIdSizeList();
    int chunkSize = maxPutChunkSize;
    for (int i = 0; i < chunkNumber; i++) {
      Pair<String, Long> chunk = chunks.get(i);
      GetBlobResult chunkResult =
          router.getBlob(chunk.getFirst(), new GetBlobOptionsBuilder().build(), null, null).get();
      // last chunk
      if (i == chunkNumber - 1) {
        chunkSize = PUT_CONTENT_SIZE - maxPutChunkSize * i;
      }
      Assert.assertEquals(chunkSize, chunk.getSecond().longValue());

      // verify data content
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      chunkResult.getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      InputStream input = retainingAsyncWritableChannel.consumeContentAsInputStream();
      retainingAsyncWritableChannel.close();
      Assert.assertEquals(chunkSize, input.available());
      byte[] readContent = Utils.readBytesFromStream(input, chunkSize);
      input.close();

      byte[] originSlice = Arrays.copyOfRange(putContent, i * maxPutChunkSize, i * maxPutChunkSize + chunkSize);
      Assert.assertArrayEquals(originSlice, readContent);
    }
  }
}
