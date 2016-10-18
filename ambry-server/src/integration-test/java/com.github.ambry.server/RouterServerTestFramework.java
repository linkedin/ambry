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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Selector;
import com.github.ambry.router.Callback;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;


/**
 * This class provides a framework for creating router/server integration test cases. It instantiates a non-blocking
 * router from the provided properties, cluster and notification system. The user defines chains of operations on a
 * certain blob (i.e. putBlob, getBlobInfo, deleteBlob). These chains can be executed asynchronously using the
 * {@link #startOperationChain(int, int, Queue)} method. The results of each stage of the chains can be checked using
 * the {@link #checkOperationChains(List)} method. See {@link RouterServerPlaintextTest} and {@link RouterServerSSLTest}
 * for example usage.
 */
class RouterServerTestFramework {
  static final int AWAIT_TIMEOUT = 20;
  static final int CHUNK_SIZE = 1024 * 1024;
  private static final double BALANCE_FACTOR = 3.0;

  private final MockClusterMap clusterMap;
  private final MockNotificationSystem notificationSystem;
  private final Router router;

  public static String sslSendBytesMetricName = Selector.class.getName() + ".SslSendBytesRate";
  public static String sslReceiveBytesMetricName = Selector.class.getName() + ".SslReceiveBytesRate";
  public static String plaintextSendBytesMetricName = Selector.class.getName() + ".PlaintextSendBytesRate";
  public static String plaintextReceiveBytesMetricName = Selector.class.getName() + ".PlaintextReceiveBytesRate";


  /**
   * Instantiate a framework for testing router-server interaction. Creates a non-blocking router to interact with the
   * passed-in {@link MockCluster}.
   * @param routerProps All of the properties to be used when instantiating the router.
   * @param clusterMap A {@link MockClusterMap} to be used in the tests.
   * @param notificationSystem A {@link MockNotificationSystem} that is used to determine if
   * @throws Exception
   */
  RouterServerTestFramework(Properties routerProps, MockClusterMap clusterMap,
      MockNotificationSystem notificationSystem)
      throws Exception {
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;

    VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProps);
    router = new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, notificationSystem).getRouter();
  }

  /**
   * Close the instantiated routers.
   * @throws IOException
   */
  void cleanup()
      throws IOException {
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
  void checkOperationChains(List<OperationChain> opChains)
      throws Exception {
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
      if (opChain.blobId != null) {
        blobsPut++;
        PartitionId partitionId = new BlobId(opChain.blobId, clusterMap).getPartition();
        int count = partitionCount.containsKey(partitionId) ? partitionCount.get(partitionId) : 0;
        partitionCount.put(partitionId, count + 1);
      }
    }
    double numPartitions = clusterMap.getWritablePartitionIds().size();
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
   * @param chainId a numeric identifying the operation chain.
   * @param operations the queue of operations to perform in the chain
   * @return an {@link OperationChain} object describing the started chain.
   */
  OperationChain startOperationChain(int blobSize, int chainId, Queue<OperationType> operations) {
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[blobSize];
    new Random().nextBytes(userMetadata);
    new Random().nextBytes(data);
    BlobProperties properties = new BlobProperties(blobSize, "serviceid1");
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
    return properties;
  }

  /**
   * Check for blob ID validity.
   * @param blobId the blobId
   * @param operationName a name for the operation being checked
   */
  private static void checkBlobId(String blobId, String operationName) {
    Assert.assertNotNull("Null blobId for operation: " + operationName, blobId);
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
   * function will generate the label "getBlob-deleted". This is used by {@link TestFuture} to provide tracking info to
   * the user if a test assertion fails.
   * @param name the type of operation (i.e. putBlob, getBlob, etc.)
   * @param afterDelete {@code true} if the operation comes after a blob was deleted.
   * @return the label of the operation
   */
  private static String genLabel(String name, boolean afterDelete) {
    return name + (afterDelete ? "-deleted" : "");
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
    Future<String> future = router.putBlob(opChain.properties, opChain.userMetadata, putChannel, callback);
    TestFuture<String> testFuture = new TestFuture<String>(future, genLabel("putBlob", false), opChain) {
      @Override
      void check()
          throws Exception {
        checkBlobId(get(), getOperationName());
      }
    };
    opChain.testFutures.add(testFuture);
  }

  /**
   * Submit a getBlobInfo operation.
   * @param afterDelete if {@code true}, verify that the blob info was not retrievable.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startGetBlobInfo(final boolean afterDelete, final OperationChain opChain) {
    Callback<GetBlobResult> callback = new TestCallback<>(opChain, afterDelete);
    Future<GetBlobResult> future =
        router.getBlob(opChain.blobId, new GetBlobOptions(GetBlobOptions.OperationType.BlobInfo, null), callback);
    TestFuture<GetBlobResult> testFuture =
        new TestFuture<GetBlobResult>(future, genLabel("getBlobInfo", afterDelete), opChain) {
          @Override
          void check()
              throws Exception {
            if (afterDelete) {
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
   * @param afterDelete if {@code true}, verify that the blob was not retrievable.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startGetBlob(final boolean afterDelete, final OperationChain opChain) {
    Callback<GetBlobResult> callback = new TestCallback<>(opChain, afterDelete);
    Future<GetBlobResult> future = router.getBlob(opChain.blobId, new GetBlobOptions(), callback);
    TestFuture<GetBlobResult> testFuture =
        new TestFuture<GetBlobResult>(future, genLabel("getBlob", afterDelete), opChain) {
          @Override
          void check()
              throws Exception {
            if (afterDelete) {
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
   * Submit a deleteBlob operation.
   * @param opChain the {@link OperationChain} object that this operation is a part of.
   */
  private void startDeleteBlob(final OperationChain opChain) {
    Callback<Void> callback = new TestCallback<>(opChain, false);
    Future<Void> future = router.deleteBlob(opChain.blobId, callback);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("deleteBlob", false), opChain) {
      @Override
      void check()
          throws Exception {
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
      switch (nextOp) {
        case PUT:
          startPutBlob(opChain);
          break;
        case GET_INFO:
        case GET_INFO_DELETED:
          startGetBlobInfo(nextOp.afterDelete, opChain);
          break;
        case GET:
        case GET_DELETED:
          startGetBlob(nextOp.afterDelete, opChain);
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
     * DeleteBlob with the nonblocking router
     */
    DELETE(false),
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
     * Wait for the operation chain's blob ID to be reported as created on all replicas. Continue with the remaining
     * actions in the operation chain afterwards.
     */
    AWAIT_CREATION(false),
    /**
     * Wait for the operation chain's blob ID to be reported as deleted on all replicas. Continue with the remaining
     * actions in the operation chain afterwards.
     */
    AWAIT_DELETION(false);

    /**
     * {@code true} if this operation follows a delete operation.
     */
    final boolean afterDelete;

    OperationType(boolean afterDelete) {
      this.afterDelete = afterDelete;
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
    T get()
        throws Exception {
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
    void checkDeleted()
        throws Exception {
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
     * Implement any testing logic here.
     */
    abstract void check()
        throws Exception;
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
