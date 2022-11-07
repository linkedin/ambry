/*
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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for {@link BatchOperationCallbackTracker}.
 */
public class BatchOperationCallbackTrackerTest extends NonBlockingRouterTestBase {
  private static final QuotaChargeCallback quotaChargeCallback = new QuotaTestUtils.TestQuotaChargeCallback();
  private static final int NUM_CHUNKS = 5;
  private static Exception trackerException;
  private final List<BlobId> chunkIds;
  private final BlobId finalBlobId;
  private final Callback<Void> callback;
  private final AtomicBoolean finalOperationCalled = new AtomicBoolean(false);
  private final BiConsumer<BlobId, Callback> finalOperation = (b, c) -> {
    finalOperationCalled.set(true);
  };

  /**
   * Constructor for {@link BatchOperationCallbackTracker}.
   */
  public BatchOperationCallbackTrackerTest() throws Exception {
    super(false, MessageFormatRecord.Metadata_Content_Version_V3, false);
    chunkIds = new ArrayList<>();
    for (int i = 0; i < NUM_CHUNKS; i++) {
      chunkIds.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), new Partition(i, "test", PartitionState.READ_WRITE, 1073741824),
          false, BlobId.BlobDataType.DATACHUNK));
    }
    finalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), new Partition(6, "test", PartitionState.READ_WRITE, 1073741824), false,
        BlobId.BlobDataType.DATACHUNK);

    callback = (Void result, Exception exception) -> {
      trackerException = exception;
    };
    setRouter();
  }

  /**
   * Test that if final blob id is completed before any other blob, it causes an error.
   */
  @Test
  public void testFinalBlobCompletedBeforeOtherChunks() throws Exception {
    // test if final blob is the first blob to be completed.
    BatchOperationCallbackTracker tracker =
        new BatchOperationCallbackTracker(chunkIds, finalBlobId, new FutureResult<>(), callback, quotaChargeCallback,
            finalOperation, router);
    tracker.getCallback(finalBlobId).onCompletion(null, null);
    Assert.assertTrue(trackerException instanceof RouterException);
    Assert.assertTrue(tracker.isCompleted());
    Assert.assertFalse(finalOperationCalled.get());

    // test if final blob completion arrives after some blobs (but not all blobs) are completed.
    for (int i = 0; i < NUM_CHUNKS - 1; i++) {
      tracker =
          new BatchOperationCallbackTracker(chunkIds, finalBlobId, new FutureResult<>(), callback, quotaChargeCallback,
              finalOperation, router);
      for (int j = 0; j <= i; j++) {
        tracker.getCallback(chunkIds.get(j)).onCompletion(null, null);
        Assert.assertFalse(finalOperationCalled.get());
      }
      tracker.getCallback(finalBlobId).onCompletion(null, null);
      Assert.assertTrue(trackerException instanceof RouterException);
      Assert.assertTrue(tracker.isCompleted());
      Assert.assertFalse(finalOperationCalled.get());
    }
  }

  /**
   * Test that operation is completed only after the final blob is completed.
   */
  @Ignore("Final operation count is 1 but expected 0 in after() method. Test owner to fix test.")
  @Test
  public void testOperationCompleteAfterFinalBlob() {
    BatchOperationCallbackTracker tracker =
        new BatchOperationCallbackTracker(chunkIds, finalBlobId, new FutureResult<>(), callback, quotaChargeCallback,
            finalOperation, router);

    // update all data chunks and ensure that tracker isn't marked as complete.
    for (int i = 0; i < NUM_CHUNKS; i++) {
      tracker.getCallback(chunkIds.get(i)).onCompletion(null, null);
      Assert.assertFalse(tracker.isCompleted());
    }

    // test that final blob completion, after the completion of all chunks, completes the tracker.
    tracker.getCallback(finalBlobId).onCompletion(null, null);
    Assert.assertNull(trackerException);
    Assert.assertTrue(tracker.isCompleted());
    Assert.assertTrue(finalOperationCalled.get());
  }

  /**
   * Test duplicate acks.
   */
  @Test
  public void testDuplicateAcks() {
    BatchOperationCallbackTracker tracker =
        new BatchOperationCallbackTracker(chunkIds, finalBlobId, new FutureResult<>(), callback, quotaChargeCallback,
            finalOperation, router);
    tracker.getCallback(chunkIds.get(0)).onCompletion(null, null);
    Assert.assertFalse(tracker.isCompleted());
    Assert.assertFalse(finalOperationCalled.get());

    tracker.getCallback(chunkIds.get(0)).onCompletion(null, null);
    Assert.assertTrue(tracker.isCompleted());
    Assert.assertTrue(trackerException instanceof RouterException);
    Assert.assertFalse(finalOperationCalled.get());
  }

  /**
   * Test that if a data chunk has exception, the operation is completed and finalOperation is never attempted.
   */
  @Test
  public void testChunkException() {
    BatchOperationCallbackTracker tracker =
        new BatchOperationCallbackTracker(chunkIds, finalBlobId, new FutureResult<>(), callback, quotaChargeCallback,
            finalOperation, router);
    tracker.getCallback(chunkIds.get(0))
        .onCompletion(null, new RouterException("test", RouterErrorCode.UnexpectedInternalError));
    Assert.assertTrue(tracker.isCompleted());
    Assert.assertTrue(trackerException instanceof RouterException);
    Assert.assertFalse(finalOperationCalled.get());
    Assert.assertEquals(RouterErrorCode.UnexpectedInternalError, ((RouterException) trackerException).getErrorCode());
  }
}
