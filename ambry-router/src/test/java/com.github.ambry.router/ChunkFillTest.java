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
package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  public void testChunkFillingBlobSizeZero() throws Exception {
    blobSize = 0;
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with a non-zero blobSize that is less than the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeLessThanChunkSize() throws Exception {
    blobSize = random.nextInt(chunkSize - 1) + 1;
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with blob size a multiple of the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeMultipleOfChunkSize() throws Exception {
    blobSize = chunkSize * (random.nextInt(10) + 1);
    fillChunksAndAssertSuccess();
  }

  /**
   * Test chunk filling with blob size not a multiple of the chunk size.
   */
  @Test
  public void testChunkFillingBlobSizeNotMultipleOfChunkSize() throws Exception {
    blobSize = chunkSize * (random.nextInt(10) + 1) + random.nextInt(chunkSize - 1) + 1;
    fillChunksAndAssertSuccess();
  }

  /**
   * Test the calculation of number of chunks and the size of each chunk, using a very large blob size. No content
   * comparison is done. This test does not consume memory more than chunkSize.
   */
  @Test
  public void testChunkNumAndSizeCalculations() throws Exception {
    chunkSize = 4 * 1024 * 1024;
    // a large blob greater than Integer.MAX_VALUE and not at chunk size boundary.
    final long blobSize = ((long) Integer.MAX_VALUE / chunkSize + 1) * chunkSize + random.nextInt(chunkSize - 1) + 1;
    VerifiableProperties vProps = getNonBlockingRouterProperties();
    MockClusterMap mockClusterMap = new MockClusterMap();
    RouterConfig routerConfig = new RouterConfig(vProps);
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap);
    ResponseHandler responseHandler = new ResponseHandler(mockClusterMap);
    BlobProperties putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    final MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize);
    FutureResult<String> futureResult = new FutureResult<String>();
    MockTime time = new MockTime();
    MockNetworkClientFactory networkClientFactory = new MockNetworkClientFactory(vProps, null, 0, 0, 0, null, time);
    PutOperation op = new PutOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, putBlobProperties,
        putUserMetadata, putChannel, futureResult, null,
        new RouterCallback(networkClientFactory.getNetworkClient(), new ArrayList<StoreKey>()), null, new MockTime());
    op.startReadingFromChannel();
    numChunks = op.getNumDataChunks();
    // largeBlobSize is not a multiple of chunkSize
    int expectedNumChunks = (int) (blobSize / chunkSize + 1);
    Assert.assertEquals("numChunks should be as expected", expectedNumChunks, numChunks);
    int lastChunkSize = (int) (blobSize % chunkSize);
    final AtomicReference<Exception> channelException = new AtomicReference<Exception>(null);
    int chunkIndex = 0;

    // The write to the MockReadableStreamChannel blocks until the data is read as part fo the chunk filling,
    // so create a thread that fills the MockReadableStreamChannel.
    Utils.newThread(new Runnable() {
      @Override
      public void run() {
        try {
          byte[] writeBuf = new byte[chunkSize];
          long written = 0;
          while (written < blobSize) {
            int toWrite = (int) Math.min(chunkSize, blobSize - written);
            putChannel.write(ByteBuffer.wrap(writeBuf, 0, toWrite));
            written += toWrite;
          }
        } catch (Exception e) {
          channelException.set(e);
        }
      }
    }, false).start();

    // Do the chunk filling.
    do {
      op.fillChunks();
      // All existing chunks must have been filled if no work was done in the last call,
      // since the channel is ByteBuffer based.
      for (PutOperation.PutChunk putChunk : op.putChunks) {
        Assert.assertNull("Mock channel write should not have caused an exception", channelException.get());
        if (putChunk.isFree()) {
          continue;
        }
        Assert.assertEquals("Chunk should be ready.", PutOperation.ChunkState.Ready, putChunk.getState());
        Assert.assertEquals("Chunk size should be maxChunkSize unless this is the last chunk",
            chunkIndex < numChunks - 1 ? chunkSize : lastChunkSize, putChunk.buf.remaining());
        chunkIndex++;
        putChunk.clear();
      }
    } while (!op.isChunkFillComplete());
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
  private void fillChunksAndAssertSuccess() throws Exception {
    VerifiableProperties vProps = getNonBlockingRouterProperties();
    MockClusterMap mockClusterMap = new MockClusterMap();
    RouterConfig routerConfig = new RouterConfig(vProps);
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap);
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
    MockTime time = new MockTime();
    MockNetworkClientFactory networkClientFactory = new MockNetworkClientFactory(vProps, null, 0, 0, 0, null, time);
    PutOperation op = new PutOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, putBlobProperties,
        putUserMetadata, putChannel, futureResult, null,
        new RouterCallback(networkClientFactory.getNetworkClient(), new ArrayList<StoreKey>()), null, time);
    op.startReadingFromChannel();
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
        Assert.assertEquals("Chunk should be ready.", PutOperation.ChunkState.Ready, putChunk.getState());
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
