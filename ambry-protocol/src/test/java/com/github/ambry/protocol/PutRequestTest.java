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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class PutRequestTest {

  @Test
  public void testReadFrom() throws IOException {

    int correlationId = 1234;
    String clientId = "client";
    PartitionId partitionId = new MockPartitionId();
    BlobId blobId = new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, (short) 1, (short) 1, partitionId, false,
        BlobId.BlobDataType.DATACHUNK);
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", (short) 2222, (short) 3333, true);
    ByteBuffer userMetadata = ByteBuffer.wrap("testMetadata".getBytes());
    ByteBuf blobData = Unpooled.wrappedBuffer("testBlobData".getBytes());
    long blobSize = blobData.readableBytes();
    BlobType blobType = BlobType.DataBlob;
    byte[] encryptionKey = new byte[] { 1, 2, 3, 4, 5};

    PutRequest request = new PutRequest(correlationId, clientId, blobId, blobProperties,
        userMetadata, blobData, blobSize, blobType, ByteBuffer.wrap(encryptionKey), Crc32Impl.getAmbryInstance(), true);
    ByteBuf content = request.content();
    NettyByteBufDataInputStream inputStream = new NettyByteBufDataInputStream(content);
    inputStream.readLong();   // Skip the total size.
    inputStream.readShort();  // skip the operation type (PutOperation).
    PutRequest newPutRequest = PutRequest.readFrom(inputStream, new MockClusterMap());

    Assert.assertTrue(newPutRequest.versionId >= PutRequest.PUT_REQUEST_VERSION_V5);
    Assert.assertEquals(correlationId, newPutRequest.correlationId);
    Assert.assertEquals(clientId, newPutRequest.clientId);
    Assert.assertEquals(blobSize, newPutRequest.blobSize);
    Assert.assertTrue(newPutRequest.isCompressed());
    Assert.assertEquals("testServiceID", newPutRequest.getBlobProperties().getServiceId());
  }
}
