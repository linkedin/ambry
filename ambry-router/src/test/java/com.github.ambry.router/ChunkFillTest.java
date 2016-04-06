/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * A class to test the chunk filling flow in the {@link PutManager}. Tests create operations with a channel and
 * ensure that chunks are filled correctly, and continue filling in as chunks get consumed.
 */
public class ChunkFillTest {
  private ByteBuffer[] compositeBuffers;
  private int totalSizeWritten = 0;
  private int numChunks = 0;
  private byte[] putContent;
  private int blobSize;
  private int chunkSize;
  Random random = new Random();

  @Before
  public void setChunkSize() {
    // a random non-zero chunkSize in the range [2, 1024]
    chunkSize = random.nextInt(1023) + 2;
  }

  /**
   * Test chunk filling with blob size zero.
   */
  @Test
  public void testChunkFillingBlobSizeZero()
      throws Exception {
    blobSize = 0;
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with a non-zero blobSize that is less than the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeLessThanChunkSize()
      throws Exception {
    blobSize = random.nextInt(chunkSize - 1) + 1;
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with blob size a multiple of the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeMultipleOfChunkSize()
      throws Exception {
    blobSize = chunkSize * (random.nextInt(10) + 1);
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with blob size not a multiple of the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeNotMultipleOfChunkSize()
      throws Exception {
    blobSize = chunkSize * (random.nextInt(10) + 1) + random.nextInt(chunkSize - 1) + 1;
    fillChunksAndAssertSuccess();
  }

  /**
   * Get default {@link Properties}.
   * @return {@link Properties} with default values.
   */
  private VerifiableProperties getNonBlockingRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    return new VerifiableProperties(properties);
  }

  /**
   * Create a {@link PutOperation} and pass in a channel with the blobSize set by the caller; and test the chunk
   * filling flow for puts.
   * Note that this test is for the chunk filling flow, not for the ChunkFiller thread (which never gets exercised,
   * as we do not even instantiate the {@link PutManager})
   */
  private void fillChunksAndAssertSuccess()
      throws Exception {
    VerifiableProperties vProps = getNonBlockingRouterProperties();
    MockClusterMap mockClusterMap = new MockClusterMap();
    RouterConfig routerConfig = new RouterConfig(vProps);
    ResponseHandler responseHandler = new ResponseHandler(mockClusterMap);
    BlobProperties putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    final ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    FutureResult<String> futureResult = new FutureResult<String>();
    PutOperation op =
        new PutOperation(routerConfig, mockClusterMap, responseHandler, putBlobProperties, putUserMetadata, putChannel,
            futureResult, null, new MockTime());
    numChunks = op.getNumDataChunks();
    compositeBuffers = new ByteBuffer[numChunks];
    final AtomicReference<Exception> operationException = new AtomicReference<Exception>(null);

    do {
      op.fillChunks();
      // All existing chunks must have been filled if no work was done in the last call,
      // since the channel is ByteBuffer based.
      for (PutOperation.PutChunk putChunk : op.putChunks) {
        if (putChunk.isFree()) {
          continue;
        }
        Assert.assertEquals("Chunk should be ready.", ChunkState.Ready, putChunk.getState());
        ByteBuffer buf = putChunk.buf;
        totalSizeWritten += buf.remaining();
        compositeBuffers[putChunk.getChunkIndex()] = ByteBuffer.allocate(buf.remaining()).put(buf);
        putChunk.clear();
      }
    } while (!op.isChunkFillComplete());

    Assert.assertEquals("total size written out should match the blob size", blobSize, totalSizeWritten);

    Exception exception = operationException.get();
    if (exception != null) {
      throw exception;
    }
    assertDataIdentity();
  }

  /**
   * Ensure that the data filled in is exactly identical to the original content.
   */
  private void assertDataIdentity() {
    ByteBuffer dest = ByteBuffer.allocate(totalSizeWritten);
    for (ByteBuffer buf : compositeBuffers) {
      Assert.assertNotNull("All chunks should have come in", buf);
      buf.flip();
      dest.put(buf);
    }
    Assert.assertTrue("Filled chunk contents must exactly match the input buffer ",
        Arrays.equals(putContent, dest.array()));
  }
}
