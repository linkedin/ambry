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
package com.github.ambry.server;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Selector;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;


/**
 * This class provides a framework for creating router/server integration test cases. It instantiates a non-blocking
 * router from the provided properties, cluster and notification system. The user defines chains of operations on a
 * certain blob (i.e. putBlob, getBlobInfo, deleteBlob). These chains can be executed asynchronously using the
 * {@link #startOperationChain(int, Container, int, Queue)} method. The results of each stage of the chains can be
 * checked using the {@link #checkOperationChains(List)} method. See {@link RouterServerPlaintextTest} and
 * {@link RouterServerSSLTest} for example usage.
 */
class RouterServerTestFramework {
  static final int AWAIT_TIMEOUT = 20;
  static final int CHUNK_SIZE = 1024 * 1024;
  private static final double BALANCE_FACTOR = 3.0;

  private final MockClusterMap clusterMap;
  private final MockNotificationSystem notificationSystem;
  private final Router router;
  private boolean testEncryption = false;

  final InMemAccountService accountService = new InMemAccountService(false, true);

  public static String transmissionSendBytesMetricName = Selector.class.getName() + ".TransmissionSendBytesRate";
  public static String transmissionReceiveBytesMetricName = Selector.class.getName() + ".TransmissionReceiveBytesRate";

  /**
   * Instantiate a framework for testing router-server interaction. Creates a non-blocking router to interact with the
   * passed-in {@link MockCluster}.
   * @param routerProps All of the properties to be used when instantiating the router.
   * @param clusterMap A {@link MockClusterMap} to be used in the tests.
   * @param notificationSystem A {@link MockNotificationSystem} that is used to determine if
   * @throws Exception
   */
  RouterServerTestFramework(Properties routerProps, MockClusterMap clusterMap,
      MockNotificationSystem notificationSystem) throws Exception {
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProps);
    router = new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, notificationSystem,
        ServerTestUtil.getSSLFactoryIfRequired(routerVerifiableProps), accountService).getRouter();
  }

  /**
   * Sets {@link #testEncryption}
   * @param testEncryption {@code true} if encryption needs to be tested. {@code false} otherwise
   */
  void setTestEncryption(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

  /**
   * Close the instantiated routers.
   * @throws IOException
   */
  void cleanup() throws IOException {
    if (router != null) {
      router.close();
    }
  }

  /**
   * Await completion of all {@link OperationChain}s in the {@code opChains} list. For each chain, check the results
   * of each stage of the chain. Also check that the blobs put into the cluster are relatively balanced between
   * partitions (no more than 3 times expected number of blobs per partition).
   * @param opChains the {@link OperationChain}s to await and check.
   * @throws Exception
   */
  void checkOperationChains(List<OperationChain> opChains) throws Exception {
    Map<PartitionId, Integer> partitionCount = new HashMap<>();
    double blobsPut = 0;
    for (OperationChain opChain : opChains) {
      if (!opChain.latch.await(AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        Assert.fail("Timeout waiting for operation chain " + opChain.chainId + " to finish.");
      }
      synchronized (opChain.testFutures) {
        for (TestFuture testFuture : opChain.testFutures) {
          testFuture.check();
        }
      }
      Assert.assertEquals("opChain stopped in the middle.", 0, opChain.operations.size());
      if (opChain.blobId != null) {
        blobsPut++;
        PartitionId partitionId = new BlobId(opChain.blobId, clusterMap).getPartition();
        int count = partitionCount.getOrDefault(partitionId, 0);
        partitionCount.put(partitionId, count + 1);
      }
    }
    double numPartitions = clusterMap.getWritablePartitionIds(null).size();
    if (opChains.size() > numPartitions) {
      double blobBalanceThreshold = BALANCE_FACTOR * Math.ceil(blobsPut / numPartitions);
      for (Map.Entry<PartitionId, Integer> entry : partitionCount.entrySet()) {
        Assert.assertTrue("Number of blobs is " + entry.getValue() + " on partition: " + entry.getKey()
                + ", which is greater than the threshold of " + blobBalanceThreshold,
            entry.getValue() <= blobBalanceThreshold);
      }
    }
  }

  /**
   * Create an {@link OperationChain} from a queue of {@link OperationType}s.  Start the operation chain asynchronously.
   * @param blobSize the size of the blob generated for put operations.
   * @param container the {@link Container} to put the blob into (can be {@code null}}
   * @param chainId a numeric identifying the operation chain.
   * @param operations the queue of operations to perform in the chain
   * @return an {@link OperationChain} object describing the started chain.
   */
  OperationChain startOperationChain(int blobSize, Container container, int chainId, Queue<OperationType> operations) {
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[blobSize];
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);
    short accountId = container == null ? Utils.getRandomShort(TestUtils.RANDOM) : container.getParentAccountId();
    short containerId = container == null ? Utils.getRandomShort(TestUtils.RANDOM) : container.getId();
    BlobProperties properties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, TestUtils.TTL_SECS, accountId, containerId,
            testEncryption, null);
    OperationChain opChain = new OperationChain(chainId, properties, userMetadata, data, operations);
    continueChain(opChain);
    return opChain;
  }

  /**
   * Generate the properties needed by the router.  NOTE: Properties for SSL interaction need
   * to be added manually.
   * @param routerDatacenter the datacenter name where the router will be running.
   * @return a {@link Properties} object with the properties needed to instantiate the router.
   */
  static Properties getRouterProperties(String routerDatacenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDatacenter);
    properties.setProperty("router.connection.checkout.timeout.ms", "5000");
    properties.setProperty("router.request.timeout.ms", "10000");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(CHUNK_SIZE));
    properties.setProperty("router.put.success.target", "1");
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", routerDatacenter);
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    properties.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    return properties;
  }

  /**
   * Check for blob ID validity.
   * @param blobId the blobId
   * @param properties the blob properties associated with the blob (to check that the blob was put in the right
   *                   partition)
   * @param operationName a name for the operation being checked
   */
  private void checkBlobId(String blobId, BlobProperties properties, String operationName) throws IOException {
    Assert.assertNotNull("Null blobId for operation: " + operationName, blobId);
    BlobId id = new BlobId(blobId, clusterMap);
    String partitionClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    Account account = accountService.getAccountById(properties.getAccountId());
    if (account != null) {
      Container container = account.getContainerById(properties.getContainerId());
      if (container != null && container.getReplicationPolicy() != null) {
        partitionClass = container.getReplicationPolicy();
      }
    }
    Assert.assertTrue("Partition that blob was put not as required by container",
        clusterMap.getWritablePartitionIds(partitionClass).contains(id.getPartition()));
  }

  /**
   * Check that {@code blobInfo} matches {@code opChain.blobInfo}.
   * @param blobInfo the {@link BlobInfo} to check
   * @param opChain the {@link OperationChain} structure to compare against
   * @param operationName a name for the operation being checked
   */
  private static void checkBlobInfo(BlobInfo blobInfo, OperationChain opChain, String operationName) {
    Assert.assertNotNull("Null blobInfo for operation: " + operationName, blobInfo);
    Assert.assertEquals("Blob size in info does not match expected for operation: " + operationName,
        opChain.properties.getBlobSize(), blobInfo.getBlobProperties().getBlobSize());
    Assert.assertEquals("Service ID in info does not match expected for operation: " + operationName,
        opChain.properties.getServiceId(), blobInfo.getBlobProperties().getServiceId());
    Assert.assertEquals("TTL is incorrect for operation: " + operationName, opChain.properties.getTimeToLiveInSeconds(),
        blobInfo.getBlobProperties().getTimeToLiveInSeconds());
    Assert.assertArrayEquals("Unexpected user metadata for operation: " + operationName, opChain.userMetadata,
        blobInfo.getUserMetadata());
  }

  /**
   * Check that the blob read from {@code channel} matches {@code opChain.data}.
   * @param channel the {@link ReadableStreamChannel} to check
   * @param opChain the {@link OperationChain} structure to compare against
   * @param operationName a name for the operation being checked
   */
  private static void checkBlob(ReadableStreamChannel channel, OperationChain opChain, String operationName) {
    Assert.assertNotNull("Null channel for operation: " + operationName, channel);
    try {
      ByteBufferAsyncWritableChannel getChannel = new ByteBufferAsyncWritableChannel();
      Future<Long> readIntoFuture = channel.readInto(getChannel, null);
      int readBytes = 0;
      do {
        ByteBuffer buf = getChannel.getNextChunk();
        int bufLength = buf.remaining();
        Assert.assertTrue(
            "total content read should not be greater than length of put content, operation: " + operationName,
            readBytes + bufLength <= opChain.data.length);
        while (buf.hasRemaining()) {
          Assert.assertEquals("Get and Put blob content should match, operation: " + operationName,
              opChain.data[readBytes++], buf.get());
        }
        getChannel.resolveOldestChunk(null);
      } while (readBytes < opChain.data.length);
      Assert.assertEquals(
          "the returned length in the future should be the length of data written, operation: " + operationName,
          (long) readBytes, (long) readIntoFuture.get(AWAIT_TIMEOUT, TimeUnit.SECONDS));
      Assert.assertNull("There should be no more data in the channel, operation: " + operationName,
          getChannel.getNextChunk(0));
    } catch (Exception e) {
      Assert.fail("Exception while reading from getChannel from operation: " + operationName);
    }
  }

  /**
   * Generate a readable label for a router operation.  For example, for a "getBlob" operation after a delete, this
   * function will generate the label "getBlob-failureExpected". This is used by {@link TestFuture} to provide tracking
   * info to the user if a test assertion fails.
   * @param name the type of operation (i.e. putBlob, getBlob, etc.)
   * @param expectFailure {@code true} if the operation is expected to fail.
   * @return the label of the operation
   */
  private static String genLabel(String name, boolean expectFailure) {
    return name + (expectFailure ? "-failureExpected" : "");
  }

  /**
   * Submit a putBlob operation.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startPutBlob(OperationChain opChain) {
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(opChain.data));
    Callback<String> callback = new TestCallback<String>(opChain, false) {
      @Override
      void action(String result) {
        opChain.blobId = result;
      }
    };
    Future<String> future =
        router.putBlob(opChain.properties, opChain.userMetadata, putChannel, new PutBlobOptionsBuilder().build(),
            callback);
    TestFuture<String> testFuture = new TestFuture<String>(future, genLabel("putBlob", false), opChain) {
      @Override
      void check() throws Exception {
        checkBlobId(get(), opChain.properties, getOperationName());
      }
    };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a getBlobInfo operation.
   * @param options the {@link GetOption} associated with the request.
   * @param checkDeleted {@code true}, checks that the blob is deleted.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startGetBlobInfo(GetOption options, final boolean checkDeleted, final OperationChain opChain) {
    Callback<GetBlobResult> callback = new TestCallback<>(opChain, checkDeleted);
    Future<GetBlobResult> future = router.getBlob(opChain.blobId,
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).getOption(options).build(),
        callback);
    TestFuture<GetBlobResult> testFuture =
        new TestFuture<GetBlobResult>(future, genLabel("getBlobInfo", checkDeleted), opChain) {
          @Override
          void check() throws Exception {
            if (checkDeleted) {
              checkDeleted();
            } else {
              checkBlobInfo(get().getBlobInfo(), opChain, getOperationName());
            }
          }
        };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a getBlob operation.
   * @param options the {@link GetOption} associated with the request.
   * @param checkDeleted {@code true}, checks that the blob is deleted.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startGetBlob(GetOption options, final boolean checkDeleted, final OperationChain opChain) {
    Callback<GetBlobResult> callback = new TestCallback<>(opChain, checkDeleted);
    Future<GetBlobResult> future = router.getBlob(opChain.blobId,
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).getOption(options).build(),
        callback);
    TestFuture<GetBlobResult> testFuture =
        new TestFuture<GetBlobResult>(future, genLabel("getBlob", checkDeleted), opChain) {
          @Override
          void check() throws Exception {
            if (checkDeleted) {
              checkDeleted();
            } else {
              checkBlobInfo(get().getBlobInfo(), opChain, getOperationName());
              checkBlob(get().getBlobDataChannel(), opChain, getOperationName());
            }
          }
        };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a getBlob operation with incorrect accountId/ContainerId in blobId.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startGetBlobAuthorizationFailTest(final OperationChain opChain) {
    Callback<GetBlobResult> callback = new TestCallback<>(opChain, true);
    BlobId originalId, fraudId;
    try {
      originalId = new BlobId(opChain.blobId, clusterMap);
      fraudId = BlobId.craft(originalId, originalId.getVersion(), (short) (originalId.getAccountId() + 1),
          (short) (originalId.getContainerId() + 1));
    } catch (IOException e) {
      // If there is a blobId creation failure, throw the exception and don't need to do actual action.
      FutureResult<Void> future = new FutureResult<>();
      // continue the chain
      future.done(null, e);
      callback.onCompletion(null, e);
      TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("getBlobAuthorizationFail", true), opChain) {
        @Override
        void check() throws Exception {
          throw e;
        }
      };
      opChain.testFutures.add(testFuture);
      return;
    }
    // If everything good:
    Future<GetBlobResult> future = router.getBlob(fraudId.getID(),
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
            .getOption(GetOption.Include_All)
            .build(), callback);
    TestFuture<GetBlobResult> testFuture =
        new TestFuture<GetBlobResult>(future, genLabel("getBlobAuthorizationFail", true), opChain) {
          @Override
          void check() throws Exception {
            checkExpectedRouterErrorCode(RouterErrorCode.BlobAuthorizationFailure);
          }
        };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a deleteBlob operation with incorrect accountId/ContainerId in blobId.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startDeleteBlobAuthorizationFailTest(final OperationChain opChain) {
    Callback<Void> callback = new TestCallback<>(opChain, true);
    BlobId originalId, fraudId;
    try {
      originalId = new BlobId(opChain.blobId, clusterMap);
      fraudId = BlobId.craft(originalId, originalId.getVersion(), (short) (originalId.getAccountId() + 1),
          (short) (originalId.getContainerId() + 1));
    } catch (IOException e) {
      // If there is a blobId creation failure, throw the exception and don't need to do actual action.
      FutureResult<Void> future = new FutureResult<>();
      // continue the chain
      future.done(null, e);
      callback.onCompletion(null, e);
      TestFuture<Void> testFuture =
          new TestFuture<Void>(future, genLabel("deleteBlobAuthorizationFail", true), opChain) {
            @Override
            void check() throws Exception {
              throw e;
            }
          };
      opChain.testFutures.add(testFuture);
      return;
    }
    Future<Void> future = router.deleteBlob(fraudId.getID(), null, callback);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("deleteBlobAuthorizationFail", true), opChain) {
      @Override
      void check() throws Exception {
        checkExpectedRouterErrorCode(RouterErrorCode.BlobAuthorizationFailure);
      }
    };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a ttl update operation.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startTtlUpdate(final OperationChain opChain) {
    Callback<Void> callback = new TestCallback<>(opChain, false);
    Future<Void> future = router.updateBlobTtl(opChain.blobId, null, Utils.Infinite_Time, callback);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("updateBlobTtl", false), opChain) {
      @Override
      void check() throws Exception {
        get();
        opChain.properties.setTimeToLiveInSeconds(Utils.Infinite_Time);
      }
    };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a deleteBlob operation.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startDeleteBlob(final OperationChain opChain) {
    Callback<Void> callback = new TestCallback<>(opChain, false);
    Future<Void> future = router.deleteBlob(opChain.blobId, null, callback);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("deleteBlob", false), opChain) {
      @Override
      void check() throws Exception {
        get();
      }
    };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Using the mock notification system, wait for the put blob in this operation chain to be replicated to all server
   * nodes before continuing the chain.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startAwaitCreation(final OperationChain opChain) {
    notificationSystem.awaitBlobCreations(opChain.blobId);
    continueChain(opChain);
  }

  /**
   * Using the mock notification system, wait for the deleted blob in this operation chain to be deleted from all server
   * nodes before continuing the chain.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startAwaitDeletion(final OperationChain opChain) {
    notificationSystem.awaitBlobDeletions(opChain.blobId);
    continueChain(opChain);
  }

  /**
   * Using the mock notification system, wait for the updated blob in this operation chain to be updated on all server
   * nodes before continuing the chain.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startAwaitTtlUpdate(final OperationChain opChain) {
    notificationSystem.awaitBlobUpdates(opChain.blobId, UpdateType.TTL_UPDATE);
    continueChain(opChain);
  }

  /**
   * Submit the next operation in the chain to the router. If there are no more operations in the queue,
   * mark the chain as completed.
   * @param opChain the {@link OperationChain} to get the next operation from.
   */
  private void continueChain(final OperationChain opChain) {
    synchronized (opChain.testFutures) {
      OperationType nextOp = opChain.operations.poll();
      if (nextOp == null) {
        opChain.latch.countDown();
        return;
      }
      GetOption options = GetOption.None;
      switch (nextOp) {
        case PUT:
          startPutBlob(opChain);
          break;
        case TTL_UPDATE:
          startTtlUpdate(opChain);
          break;
        case GET_INFO_DELETED_SUCCESS:
          options = GetOption.Include_Deleted_Blobs;
        case GET_INFO:
        case GET_INFO_DELETED:
          startGetBlobInfo(options, nextOp.checkDeleted, opChain);
          break;
        case GET_DELETED_SUCCESS:
          options = GetOption.Include_Deleted_Blobs;
        case GET:
        case GET_DELETED:
          startGetBlob(options, nextOp.checkDeleted, opChain);
          break;
        case DELETE:
          startDeleteBlob(opChain);
          break;
        case AWAIT_CREATION:
          startAwaitCreation(opChain);
          break;
        case AWAIT_DELETION:
          startAwaitDeletion(opChain);
          break;
        case AWAIT_TTL_UPDATE:
          startAwaitTtlUpdate(opChain);
          break;
        case GET_AUTHORIZATION_FAILURE:
          startGetBlobAuthorizationFailTest(opChain);
          break;
        case DELETE_AUTHORIZATION_FAILURE:
          startDeleteBlobAuthorizationFailTest(opChain);
          break;
        default:
          throw new IllegalArgumentException("Unknown op: " + nextOp);
      }
    }
  }

  /**
   * Used to specify operations to perform on a blob in an operation chain.
   */
  enum OperationType {
    /**
     * PutBlob with the nonblocking router
     */
    PUT(false),

    /**
     * GetBlobInfo with the nonblocking router and check the blob info against what was put in.
     */
    GET_INFO(false),

    /**
     * GetBlob with the nonblocking router and check the blob contents against what was put in.
     */
    GET(false),

    /**
     * GetBlob with incorrect accountId and containerId in blobId
     */
    GET_AUTHORIZATION_FAILURE(false),

    /**
     * Update the TTL of a blob
     */
    TTL_UPDATE(false),

    /**
     * DeleteBlob with the nonblocking router
     */
    DELETE(false),

    /**
     * DeleteBlob with incorrect accountId and containerId in blobId
     */
    DELETE_AUTHORIZATION_FAILURE(false),

    /**
     * GetBlobInfo with the nonblocking router. Expect an exception to occur because the blob should have already been
     * deleted
     */
    GET_INFO_DELETED(true),

    /**
     * GetBlob with the nonblocking router. Expect an exception to occur because the blob should have already been
     * deleted
     */
    GET_DELETED(true),

    /**
     * GetBlobInfo with the nonblocking router. Will use {@link GetOption#Include_Deleted_Blobs} and is expected to
     * succeed even though the blob is deleted.
     */
    GET_INFO_DELETED_SUCCESS(false),

    /**
     * GetBlob with the nonblocking router. Will use {@link GetOption#Include_Deleted_Blobs} and is expected to
     * succeed even though the blob is deleted.
     */
    GET_DELETED_SUCCESS(false),

    /**
     * Wait for the operation chain's blob ID to be reported as created on all replicas. Continue with the remaining
     * actions in the operation chain afterwards.
     */
    AWAIT_CREATION(false),

    /**
     * Wait for the operation chain's blob ID to be reported as deleted on all replicas. Continue with the remaining
     * actions in the operation chain afterwards.
     */
    AWAIT_DELETION(false),

    /**
     * Wait for the operation chain's blob ID to be reported as updated on all replicas. Continue with the remaining
     * actions in the operation chain afterwards.
     */
    AWAIT_TTL_UPDATE(false);

    /**
     * {@code true} if this operation needs to check that the response returned indicates that the blob is deleted.
     */
    final boolean checkDeleted;

    OperationType(boolean checkDeleted) {
      this.checkDeleted = checkDeleted;
    }
  }

  /**
   * Describes an operation chain and provides useful metadata and operation results for testing.
   */
  static class OperationChain {
    final int chainId;
    final BlobProperties properties;
    final byte[] userMetadata;
    final byte[] data;
    final Queue<OperationType> operations;
    final List<TestFuture> testFutures = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);
    String blobId;

    OperationChain(int chainId, BlobProperties properties, byte[] userMetadata, byte[] data,
        Queue<OperationType> operations) {
      this.chainId = chainId;
      this.properties = properties;
      this.userMetadata = userMetadata;
      this.data = data;
      this.operations = operations;
    }
  }

  /**
   * This class encapsulates a future and allows the user to define a check method that runs tests on
   * the retrieved value in the future.
   *
   * @param <T> The type of the encapsulated future
   */
  private static abstract class TestFuture<T> {
    final Future<T> future;
    final String operationType;
    final OperationChain opChain;

    TestFuture(Future<T> future, String operationType, OperationChain opChain) {
      this.future = future;
      this.operationType = operationType;
      this.opChain = opChain;
    }

    /**
     * Generate a name for the tested operation
     * @return the operation name
     */
    String getOperationName() {
      return operationType + "-" + opChain.chainId;
    }

    /**
     * Return the value inside the future or throw an {@link Exception} if an exception
     * occurred.
     * @return the value inside the future
     */
    T get() throws Exception {
      try {
        return future.get(AWAIT_TIMEOUT, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new Exception("Exception occured in operation: " + getOperationName(), e);
      }
    }

    /**
     * Check that a requested blob is deleted.
     * @throws Exception
     */
    void checkDeleted() throws Exception {
      try {
        future.get(AWAIT_TIMEOUT, TimeUnit.SECONDS);
        Assert.fail("Blob should have been deleted in operation: " + getOperationName());
      } catch (ExecutionException e) {
        Throwable rootCause = Utils.getRootCause(e);
        Assert.assertTrue("Exception cause is not RouterException in operation: " + getOperationName(),
            rootCause instanceof RouterException);
        Assert.assertEquals("Error code is not BlobDeleted in operation: " + getOperationName(),
            RouterErrorCode.BlobDeleted, ((RouterException) rootCause).getErrorCode());
      } catch (Exception e) {
        throw new Exception("Unexpected exception occured in operation: " + getOperationName(), e);
      }
    }

    /**
     * Check if router get expected RouterErrorCode.
     * @param routerErrorCode is the expected error code.
     */
    void checkExpectedRouterErrorCode(RouterErrorCode routerErrorCode) {
      try {
        future.get(AWAIT_TIMEOUT, TimeUnit.SECONDS);
        Assert.fail("Blob should have failed in operation: " + getOperationName());
      } catch (Exception e) {
        Assert.assertTrue("Expect RouterException", e.getCause() instanceof RouterException);
        Assert.assertEquals("RouterErrorCode doesn't match.", routerErrorCode,
            ((RouterException) e.getCause()).getErrorCode());
      }
    }

    /**
     * Implement any testing logic here.
     */
    abstract void check() throws Exception;
  }

  /**
   * A callback for router operations that starts the next operation in the chain after completion.
   * The user can define a custom action on the result of the operation by overriding the {@code action()} method.
   * @param <T> The callback's result type
   */
  private class TestCallback<T> implements Callback<T> {
    final OperationChain opChain;
    final boolean expectError;

    TestCallback(OperationChain opChain, boolean expectError) {
      this.opChain = opChain;
      this.expectError = expectError;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      if (exception != null && !expectError) {
        opChain.latch.countDown();
        return;
      }
      action(result);
      continueChain(opChain);
    }

    /**
     * Perform custom actions on the result of the operation here by overriding this method.
     * @param result the result of the completed operation
     */
    void action(T result) {
    }
  }
}
