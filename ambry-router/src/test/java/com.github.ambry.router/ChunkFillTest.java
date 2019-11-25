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

import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * A class to test the chunk filling flow in the {@link PutManager}. Tests create operations with a channel and
 * ensure that chunks are filled correctly, and continue filling in as chunks get consumed.
 */
@RunWith(Parameterized.class)
public class ChunkFillTest {
  private final boolean testEncryption;
  private ByteBuf[] compositeBuffers;
  private ByteBuffer[] compositeEncryptionKeys;
  private BlobId[] compositeBlobIds;
  private int totalSizeWritten = 0;
  private int numChunks = 0;
  private byte[] putContent;
  private int blobSize;
  private int chunkSize;
  private Random random = new Random();
  private NonBlockingRouterMetrics routerMetrics;
  private MockKeyManagementService kms = null;
  private MockCryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public ChunkFillTest(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

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
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    BlobProperties putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time, accountId,
            containerId, false, null);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    final MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize, false);
    FutureResult<String> futureResult = new FutureResult<String>();
    MockTime time = new MockTime();
    MockNetworkClientFactory networkClientFactory = new MockNetworkClientFactory(vProps, null, 0, 0, 0, null, time);
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), putUserMetadata, putChannel, PutBlobOptions.DEFAULT, futureResult,
            null, new RouterCallback(networkClientFactory.getNetworkClient(), new ArrayList<>()), null, null, null,
            null, new MockTime(), putBlobProperties, MockClusterMap.DEFAULT_PARTITION_CLASS);
    op.startOperation();
    numChunks = RouterUtils.getNumChunksForBlobAndChunkSize(blobSize, chunkSize);
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
    boolean fillingComplete = false;
    do {
      op.fillChunks();
      // All existing chunks must have been filled if no work was done in the last call,
      // since the channel is ByteBuffer based.
      for (PutOperation.PutChunk putChunk : op.putChunks) {
        Assert.assertNull("Mock channel write should not have caused an exception", channelException.get());
        if (putChunk.isFree()) {
          continue;
        }
        if (chunkIndex == numChunks - 1) {
          // last chunk may not be Ready as it is dependent on the completion callback to be called.
          Assert.assertTrue("Chunk should be Building or Ready.", putChunk.getState() == PutOperation.ChunkState.Ready
              || putChunk.getState() == PutOperation.ChunkState.Building);
          if (putChunk.getState() == PutOperation.ChunkState.Ready) {
            Assert.assertEquals("Chunk size should be the last chunk size", lastChunkSize, putChunk.buf.readableBytes());
            Assert.assertTrue("Chunk Filling should be complete at this time", op.isChunkFillingDone());
            fillingComplete = true;
          }
        } else {
          // if not last chunk, then the chunk should be full and Ready.
          Assert.assertEquals("Chunk should be ready.", PutOperation.ChunkState.Ready, putChunk.getState());
          Assert.assertEquals("Chunk size should be maxChunkSize", chunkSize, putChunk.buf.readableBytes());
          chunkIndex++;
          putChunk.clear();
        }
      }
    } while (!fillingComplete);
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
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    ResponseHandler responseHandler = new ResponseHandler(mockClusterMap);
    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    BlobProperties putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time, accountId,
            containerId, testEncryption, null);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    final ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    FutureResult<String> futureResult = new FutureResult<String>();
    MockTime time = new MockTime();
    MockNetworkClientFactory networkClientFactory = new MockNetworkClientFactory(vProps, null, 0, 0, 0, null, time);
    if (testEncryption) {
      kms = new MockKeyManagementService(new KMSConfig(vProps),
          TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
      cryptoService = new MockCryptoService(new CryptoServiceConfig(vProps));
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    }
    MockRouterCallback routerCallback =
        new MockRouterCallback(networkClientFactory.getNetworkClient(), Collections.EMPTY_LIST);
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), putUserMetadata, putChannel, PutBlobOptions.DEFAULT, futureResult,
            null, routerCallback, null, kms, cryptoService, cryptoJobHandler, time, putBlobProperties,
            MockClusterMap.DEFAULT_PARTITION_CLASS);
    op.startOperation();
    numChunks = RouterUtils.getNumChunksForBlobAndChunkSize(blobSize, chunkSize);
    compositeBuffers = new ByteBuf[numChunks];
    compositeEncryptionKeys = new ByteBuffer[numChunks];
    compositeBlobIds = new BlobId[numChunks];
    final AtomicReference<Exception> operationException = new AtomicReference<Exception>(null);

    int chunksLeftToBeFilled = numChunks;
    do {
      if (testEncryption) {
        int chunksPerBatch = Math.min(routerConfig.routerMaxInMemPutChunks, chunksLeftToBeFilled);
        CountDownLatch onPollLatch = new CountDownLatch(chunksPerBatch);
        routerCallback.setOnPollLatch(onPollLatch);
        op.fillChunks();
        Assert.assertTrue("Latch should have been zeroed out", onPollLatch.await(1000, TimeUnit.MILLISECONDS));
        chunksLeftToBeFilled -= chunksPerBatch;
      } else {
        op.fillChunks();
      }
      // All existing chunks must have been filled if no work was done in the last call,
      // since the channel is ByteBuffer based.
      for (PutOperation.PutChunk putChunk : op.putChunks) {
        if (putChunk.isFree()) {
          continue;
        }
        Assert.assertEquals("Chunk should be ready.", PutOperation.ChunkState.Ready, putChunk.getState());
        ByteBuf buf = putChunk.buf.retainedDuplicate();
        totalSizeWritten += buf.readableBytes();
        compositeBuffers[putChunk.getChunkIndex()] = buf;
        if (testEncryption) {
          compositeEncryptionKeys[putChunk.getChunkIndex()] = putChunk.encryptedPerBlobKey.duplicate();
          compositeBlobIds[putChunk.getChunkIndex()] = putChunk.chunkBlobId;
        }
        putChunk.clear();
      }
    } while (!op.isChunkFillingDone());

    if (!testEncryption) {
      Assert.assertEquals("total size written out should match the blob size", blobSize, totalSizeWritten);
    }
    // for encrypted path, size will be implicitly tested via assertDataIdentity

    Exception exception = operationException.get();
    if (exception != null) {
      throw exception;
    }
    assertDataIdentity(mockClusterMap);
  }

  /**
   * Ensure that the data filled in is exactly identical to the original content.
   */
  private void assertDataIdentity(ClusterMap clusterMap) throws IOException {
    if (!testEncryption) {
      ByteBuffer dest = ByteBuffer.allocate(totalSizeWritten);
      for (ByteBuf buf : compositeBuffers) {
        Assert.assertNotNull("All chunks should have come in", buf);
        for (ByteBuffer buffer: buf.nioBuffers()) {
          dest.put(buffer);
        }
      }
      Assert.assertTrue("Filled chunk contents must exactly match the input buffer ",
          Arrays.equals(putContent, dest.array()));
    } else {
      byte[] content = new byte[blobSize];
      AtomicInteger offset = new AtomicInteger(0);
      for (int i = 0; i < numChunks; i++) {
        DecryptJob decryptJob =
            new DecryptJob(compositeBlobIds[i], compositeEncryptionKeys[i], compositeBuffers[i], null, cryptoService,
                kms, new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics), (result, exception) -> {
              Assert.assertNull("Exception shouldn't have been thrown", exception);
              int chunkSize = result.getDecryptedBlobContent().remaining();
              result.getDecryptedBlobContent().get(content, offset.get(), chunkSize);
              offset.addAndGet(chunkSize);
            });
        decryptJob.run();
      }
      Assert.assertTrue("Filled chunk contents must exactly match the input buffer ",
          Arrays.equals(putContent, content));
    }
  }
}
