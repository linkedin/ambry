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

import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Class to test the {@link NonBlockingRouter} with quota callbacks.
 */
@RunWith(Parameterized.class)
public class NonBlockingRouterQuotaCallbackTest extends NonBlockingRouterTestBase {
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterQuotaCallbackTest.class);

  private final QuotaMode throttlingMode;
  private final boolean throttleInProgressRequests;

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise.
   * @param metadataContentVersion the metadata content version to test with.
   * @param quotaModeStr {@link QuotaMode} for router.
   * @param throttleInProgressRequests {@code true} if in progress request can be throttled. {@code false} otherwise.
   * @throws Exception if initialization fails.
   */
  public NonBlockingRouterQuotaCallbackTest(boolean testEncryption, int metadataContentVersion, String quotaModeStr,
      boolean throttleInProgressRequests) throws Exception {
    super(testEncryption, metadataContentVersion, false);
    this.throttlingMode = QuotaMode.valueOf(quotaModeStr);
    this.throttleInProgressRequests  = throttleInProgressRequests;
  }

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.THROTTLING.name(), true},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.THROTTLING.name(), true},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.THROTTLING.name(), true},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.THROTTLING.name(), true},
        {false, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.TRACKING.name(), true},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.TRACKING.name(), true},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.TRACKING.name(), true},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.TRACKING.name(), true},
        {false, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.THROTTLING.name(), false},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.THROTTLING.name(), false},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.THROTTLING.name(), false},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.THROTTLING.name(), false},
        {false, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.TRACKING.name(), false},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.TRACKING.name(), false},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, QuotaMode.TRACKING.name(), false},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, QuotaMode.TRACKING.name(), false}});
  }

  /**
   * Test Router with single scaling unit for correct accounting in {@link QuotaChargeCallback}.
   */
  @Test
  public void testRouterWithQuotaCallback() throws Exception {
    try {
      setRouter();
      assertExpectedThreadCounts(2, 1);
      AtomicInteger listenerCalledCount = new AtomicInteger(0);
      int expectedChargeCallbackCount = 0;
      // create a quota charge listener that increments an atomic counter everytime its called.
      // Also tests that in case quota if charged in tracking mode with throttleInProgress config set to false
      // then the requests go through even in case of exception.
      QuotaChargeCallback quotaChargeCallback = () -> {
        listenerCalledCount.incrementAndGet();
        throw new RouterException("Quota exceeded.", RouterErrorCode.TooManyRequests);
      };

      // test for a composite blob.
      int blobSize = 3000;
      int numChunks = (blobSize > maxPutChunkSize) ? 1 : 0;
      numChunks += (blobSize % maxPutChunkSize) == 0 ? blobSize / maxPutChunkSize : (blobSize / maxPutChunkSize) + 1;
      setOperationParams(blobSize, TTL_SECS);
      String compositeBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT, null,
              quotaChargeCallback).get();
      expectedChargeCallbackCount += numChunks;
      assertEquals(expectedChargeCallbackCount, listenerCalledCount.get());
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback).get()
          .getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      expectedChargeCallbackCount += numChunks;
      // read out all the chunks to make sure all the chunks are consumed and accounted for.
      retainingAsyncWritableChannel.consumeContentAsInputStream().close();
      assertEquals(expectedChargeCallbackCount, listenerCalledCount.get());

      // test for regular blobs.
      setOperationParams();
      List<String> blobIds = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        setOperationParams();
        String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT, null,
            quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        logger.info("Put blob {}", blobId);
        blobIds.add(blobId);
      }
      setOperationParams();

      for (String blobId : blobIds) {
        router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        router.updateBlobTtl(blobId, null, Utils.Infinite_Time, null, quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(),
            null, quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        router.deleteBlob(blobId, null, null, quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        try {
          router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback).get();
          fail("Get blob should fail");
        } catch (ExecutionException e) {
          RouterException r = (RouterException) e.getCause();
          Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
          assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        }
        router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build(), null,
            quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build(), null,
            quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
      }

      // test for stitched blobs.
      blobIds = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        setOperationParams();
        String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT, null,
            quotaChargeCallback).get();
        assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
        logger.info("Put blob {}", blobId);
        blobIds.add(blobId);
      }

      String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, blobIds.stream()
          .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time))
          .collect(Collectors.toList()), null, quotaChargeCallback).get();
      assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());

      retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      expectedChargeCallbackCount += 3;
      router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback).get()
          .getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      // read out all the chunks to make sure all the chunks are consumed and accounted for.
      retainingAsyncWritableChannel.consumeContentAsInputStream().close();
      assertEquals(expectedChargeCallbackCount, listenerCalledCount.get());

      router.updateBlobTtl(stitchedBlobId, null, Utils.Infinite_Time, null, quotaChargeCallback).get();
      assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());

      router.deleteBlob(stitchedBlobId, null, null, quotaChargeCallback).get();
      assertEquals(++expectedChargeCallbackCount, listenerCalledCount.get());
    } finally {
      router.close();
      assertExpectedThreadCounts(0, 0);

      //submission after closing should return a future that is already done.
      assertClosed();
    }
  }
}
