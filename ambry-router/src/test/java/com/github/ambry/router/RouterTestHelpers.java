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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Assert;

import static org.junit.Assert.*;


/**
 * Class with helper methods for testing the router.
 */
class RouterTestHelpers {
  private static final int AWAIT_TIMEOUT_SECONDS = 10;
  static final int AWAIT_TIMEOUT_MS = 2000;
  private static final short BLOB_ID_VERSION = CommonTestUtils.getCurrentBlobIdVersion();

  /**
   * Test whether persistent properties in two {@link BlobProperties} are same.
   * @return true if the persistent properties are equivalent in the two {@link BlobProperties}
   */
  static boolean arePersistedFieldsEquivalent(BlobProperties a, BlobProperties b) {
    return a.getServiceId().equals(b.getServiceId()) && a.getOwnerId().equals(b.getOwnerId()) && a.getContentType()
        .equals(b.getContentType()) && a.isPrivate() == b.isPrivate()
        && a.getTimeToLiveInSeconds() == b.getTimeToLiveInSeconds()
        && a.getCreationTimeInMs() == b.getCreationTimeInMs() && a.getAccountId() == b.getAccountId()
        && a.getContainerId() == b.getContainerId() && a.isEncrypted() == b.isEncrypted();
  }

  /**
   * Test that an operation returns a certain result when servers return error codes corresponding
   * to {@code serverErrorCodeCounts}
   * @param serverErrorCodeCounts A map from {@link ServerErrorCode}s to a count of how many servers should be set to
   *                              that error code.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(Map<ServerErrorCode, Integer> serverErrorCodeCounts, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    List<ServerErrorCode> serverErrorCodes = new ArrayList<>();
    for (Map.Entry<ServerErrorCode, Integer> entry : serverErrorCodeCounts.entrySet()) {
      for (int j = 0; j < entry.getValue(); j++) {
        serverErrorCodes.add(entry.getKey());
      }
    }
    Collections.shuffle(serverErrorCodes);
    testWithErrorCodes(serverErrorCodes.toArray(new ServerErrorCode[0]), serverLayout, expectedError, errorCodeChecker);
  }

  /**
   * Test that an operation returns a certain result when each server in the layout returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.  If
   *                                there are more values in this array than there are servers, an exception is thrown.
   *                                If there are fewer, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    setServerErrorCodes(Arrays.asList(serverErrorCodesInOrder), serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Test that an operation returns a certain result when each server in a partition returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are more values in this array than
   *                                there are servers containing the partition, an exception is thrown. If there are
   *                                fewer, the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout, RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker)
      throws Exception {
    setServerErrorCodes(serverErrorCodesInOrder, partition, serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Set the servers in the specified layout to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.
   *                                If there are fewer error codes in this array than there are servers,
   *                                the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(List<ServerErrorCode> serverErrorCodesInOrder, MockServerLayout serverLayout) {
    Collection<MockServer> servers = serverLayout.getMockServers();
    if (serverErrorCodesInOrder.size() > servers.size()) {
      throw new IllegalArgumentException("More server error codes provided than servers in cluster");
    }
    int i = 0;
    for (MockServer server : servers) {
      if (i < serverErrorCodesInOrder.size()) {
        server.setServerErrorForAllRequests(serverErrorCodesInOrder.get(i++));
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.No_Error);
      }
    }
  }

  /**
   * Set the servers containing the specified partition to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are fewer error codes in this array
   *                                than there are servers containing the partition, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout) {
    List<? extends ReplicaId> replicas = partition.getReplicaIds();
    if (serverErrorCodesInOrder.length > replicas.size()) {
      throw new IllegalArgumentException("More server error codes provided than replicas in partition");
    }
    // Assume number of nodes in each dc is 3 and do serverErrorCodes assignment based on Operation Tracker logic.
    // TODO: Use a shared queue among all mock servers to avoid assumption above.
    int numOfNodesEachDatacenter = 3;
    List<ServerErrorCode> serverErrorInLocalDC =
        Arrays.asList(serverErrorCodesInOrder).subList(0, numOfNodesEachDatacenter);
    List<ServerErrorCode> serverErrorInOtherDCs =
        Arrays.asList(serverErrorCodesInOrder).subList(numOfNodesEachDatacenter, serverErrorCodesInOrder.length);
    Collections.reverse(serverErrorInLocalDC);
    Collections.reverse(serverErrorInOtherDCs);
    List<ServerErrorCode> serverErrorCodesInOperationTrackerOrder = new ArrayList<>(serverErrorInLocalDC);
    serverErrorCodesInOperationTrackerOrder.addAll(serverErrorInOtherDCs);

    for (int i = 0; i < partition.getReplicaIds().size(); i++) {
      DataNodeId node = partition.getReplicaIds().get(i).getDataNodeId();
      MockServer mockServer = serverLayout.getMockServer(node.getHostname(), node.getPort());
      mockServer.setServerErrorForAllRequests(serverErrorCodesInOperationTrackerOrder.get(i));
    }
  }

  /**
   * Asserts that expected threads are not running after the router is closed.
   */
  static void assertCloseCleanup(NonBlockingRouter router) {
    router.close();
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Check that a router operation has failed with a router exception with the specified error code.
   * @param future the {@link Future} for the router operation
   * @param callback the {@link TestCallback} for the router operation. Can be {@code null}
   * @param expectedError the expected {@link RouterErrorCode}
   */
  static <T> void assertFailureAndCheckErrorCode(Future<T> future, TestCallback<T> callback,
      RouterErrorCode expectedError) {
    try {
      assertTrue("Callback was not called", callback.getLatch().await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      future.get(1, TimeUnit.SECONDS);
      fail("Operation should be unsuccessful. Exception is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be " + expectedError + " (future)", expectedError,
          ((RouterException) e.getCause()).getErrorCode());
      if (callback != null) {
        assertEquals("RouterErrorCode should be " + expectedError + " (callback)", expectedError,
            ((RouterException) callback.getException()).getErrorCode());
        assertNull("Result should be null", callback.getResult());
      }
    }
  }

  /**
   * Asserts that {@code blobId} has ttl {@code expectedTtl} (also doubles up as checking of GetBlobInfoOp and
   * GetBlobOp).
   * @param router the {@link Router} to use
   * @param blobIds the blob ids to query
   * @param expectedTtlSecs the expected ttl in seconds
   * @throws Exception
   */
  static void assertTtl(Router router, Collection<String> blobIds, long expectedTtlSecs) throws Exception {
    GetBlobOptions options[] = {new GetBlobOptionsBuilder().build(),
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build()};
    for (String blobId : blobIds) {
      for (GetBlobOptions option : options) {
        GetBlobResult result = router.getBlob(blobId, option).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("TTL not as expected", expectedTtlSecs,
            result.getBlobInfo().getBlobProperties().getTimeToLiveInSeconds());
        if (result.getBlobDataChannel() != null) {
          result.getBlobDataChannel().close();
        }
      }
    }
  }

  /**
   * @param totalSize the total size of the composite blob.
   * @param maxChunkSize the size to use for intermediate data chunks.
   * @return a {@link LongStream} that returns valid chunk sizes that conform to {@code totalSize} and
   *         {@code maxChunkSize}
   */
  static LongStream buildValidChunkSizeStream(long totalSize, int maxChunkSize) {
    int numChunks = RouterUtils.getNumChunksForBlobAndChunkSize(totalSize, maxChunkSize);
    return LongStream.range(0, numChunks)
        .map(index -> index == numChunks - 1 ? totalSize - (maxChunkSize * index) : maxChunkSize);
  }

  /**
   * Compare and assert that the content in the given {@link ReadableStreamChannel} is exactly the same as
   * the original content argument.
   * @param originalContent the content array to check against.
   * @param range if non-null, apply this range to {@code originalContent} before comparison.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that is the candidate for comparison.
   */
  static void compareContent(byte[] originalContent, ByteRange range, ReadableStreamChannel readableStreamChannel)
      throws ExecutionException, InterruptedException {
    ByteBuffer putContentBuf = ByteBuffer.wrap(originalContent);
    // If a range is set, compare the result against the specified byte range.
    if (range != null) {
      ByteRange resolvedRange = range.toResolvedByteRange(originalContent.length);
      putContentBuf =
          ByteBuffer.wrap(originalContent, (int) resolvedRange.getStartOffset(), (int) resolvedRange.getRangeSize());
    }
    ByteBufferAsyncWritableChannel getChannel = new ByteBufferAsyncWritableChannel();
    Future<Long> readIntoFuture = readableStreamChannel.readInto(getChannel, null);
    final int bytesToRead = putContentBuf.remaining();
    int readBytes = 0;
    do {
      ByteBuffer buf = getChannel.getNextChunk();
      int bufLength = buf.remaining();
      assertTrue("total content read should not be greater than length of put content",
          readBytes + bufLength <= bytesToRead);
      while (buf.hasRemaining()) {
        assertEquals("Get and Put blob content should match", putContentBuf.get(), buf.get());
        readBytes++;
      }
      getChannel.resolveOldestChunk(null);
    } while (readBytes < bytesToRead);
    assertEquals("the returned length in the future should be the length of data written", (long) readBytes,
        (long) readIntoFuture.get());
    assertNull("There should be no more data in the channel", getChannel.getNextChunk(0));
  }

  /**
   *
   * @param clusterMap the {@link ClusterMap} for generating blob IDs.
   * @param blobDataType the {@link BlobId.BlobDataType} field to set for the chunk blob IDs.
   * @param ttl the TTL for the chunks.
   * @param chunkSizeStream a stream of chunk sizes to use for the chunks in the list.
   * @return a list of {@link ChunkInfo} objects for a stitch call.
   */
  static List<ChunkInfo> buildChunkList(ClusterMap clusterMap, BlobId.BlobDataType blobDataType, long ttl,
      LongStream chunkSizeStream) {
    return chunkSizeStream.mapToObj(
        chunkSize -> new ChunkInfo(getRandomBlobId(clusterMap, blobDataType), chunkSize, ttl))
        .collect(Collectors.toList());
  }

  /**
   *
   * @param clusterMap the {@link ClusterMap} for generating blob IDs.
   * @param blobDataType the {@link BlobId.BlobDataType}.
   * @return a new blob ID with the specified {@link BlobId.BlobDataType} and a random UUID.
   */
  private static String getRandomBlobId(ClusterMap clusterMap, BlobId.BlobDataType blobDataType) {
    PartitionId partitionId = clusterMap.getRandomWritablePartition(MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    return new BlobId(BLOB_ID_VERSION, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, partitionId, false, blobDataType).getID();
  }

  /**
   * Get any of the replicas from local DC or remote DC (based on the given parameter).
   * @param blobId the id of blob that sits on the replica
   * @param fromLocal whether to get replica from local datacenter
   * @param localDcName the name of local datacenter
   * @return any {@link ReplicaId} that satisfies requirement
   */
  static ReplicaId getAnyReplica(BlobId blobId, boolean fromLocal, String localDcName) {
    ReplicaId replicaToReturn = null;
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      if (fromLocal && replicaId.getDataNodeId().getDatacenterName().equals(localDcName)) {
        replicaToReturn = replicaId;
        break;
      }
      if (!fromLocal && !replicaId.getDataNodeId().getDatacenterName().equals(localDcName)) {
        replicaToReturn = replicaId;
        break;
      }
    }
    return replicaToReturn;
  }

  /**
   * Implement this interface to provide {@link #testWithErrorCodes} with custom verification logic.
   */
  interface ErrorCodeChecker {
    /**
     * Test and make assertions related to the expected error code.
     * @param expectedError The {@link RouterErrorCode} that an operation is expected to report, or {@code null} if no
     *                      error is expected.
     * @throws Exception
     */
    void testAndAssert(RouterErrorCode expectedError) throws Exception;
  }

  /**
   * Class that is a helper callback to simply store the result and exception
   * @param <T> the type of the callback required
   */
  static class TestCallback<T> implements Callback<T> {
    private final CountDownLatch latch;
    private T result;
    private Exception exception;

    /**
     * Default ctor
     */
    TestCallback() {
      this(new CountDownLatch(1));
    }

    /**
     * @param latch latch that will be counted down when callback is received.
     */
    TestCallback(CountDownLatch latch) {
      this.latch = latch;
    }

    /**
     * @return the result received by this callback
     */
    T getResult() {
      return result;
    }

    /**
     * @return the {@link Exception} received by this callback
     */
    Exception getException() {
      return exception;
    }

    /**
     * @return the {@link CountDownLatch} associated with this callback
     */
    CountDownLatch getLatch() {
      return latch;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      this.result = result;
      this.exception = exception;
      latch.countDown();
    }
  }
}

