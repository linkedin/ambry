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
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.Callback;
import com.github.ambry.router.CoordinatorBackedRouter;
import com.github.ambry.router.CoordinatorBackedRouterMetrics;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;


class RouterServerTestFramework {
  static final int CHUNK_SIZE = 1024 * 1024;
  private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
  private final Router nonBlockingRouter;
  private final Router coordinatorBackedRouter;

  RouterServerTestFramework(Properties routerProps, MockCluster cluster, NotificationSystem notificationSystem)
      throws Exception {
    MockClusterMap clusterMap = cluster.getClusterMap();
    VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProps);
    this.nonBlockingRouter =
        new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, notificationSystem).getRouter();

    RouterConfig routerConfig = new RouterConfig(routerVerifiableProps);
    CoordinatorBackedRouterMetrics coordinatorBackedRouterMetrics =
        new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
    AmbryCoordinator coordinator = new HelperCoordinator(routerVerifiableProps, clusterMap);
    this.coordinatorBackedRouter =
        new CoordinatorBackedRouter(routerConfig, coordinatorBackedRouterMetrics, coordinator);
  }

  void cleanup()
      throws IOException {
    worker.shutdown();
    if (nonBlockingRouter != null) {
      nonBlockingRouter.close();
    }
    if (coordinatorBackedRouter != null) {
      coordinatorBackedRouter.close();
    }
  }

  static void checkFutures(List<OperationInfo> opInfos)
      throws Exception {
    for (OperationInfo opInfo : opInfos) {
      opInfo.latch.await();
      synchronized (opInfo.futures) {
        for (TestFuture future : opInfo.futures) {
          future.check();
        }
      }
    }
  }

  OperationInfo startOperationChain(int blobSize, int operationId, Queue<OperationType> opChain) {
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[blobSize];
    new Random().nextBytes(userMetadata);
    new Random().nextBytes(data);
    BlobProperties properties = new BlobProperties(blobSize, "serviceid1");
    OperationInfo opInfo = new OperationInfo(operationId, properties, userMetadata, data, opChain);
    continueChain(opInfo);
    return opInfo;
  }

  static Properties getRouterProperties(String routerDatacenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDatacenter);
    properties.setProperty("router.connection.checkout.timeout.ms", "5000");
    properties.setProperty("router.request.timeout.ms", "20000");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(CHUNK_SIZE));
    properties.setProperty("router.put.success.target", "1");
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", routerDatacenter);
    return properties;
  }

  private static void checkBlobId(String blobId, String operationName) {
    Assert.assertNotNull("Null blobId for operation: " + operationName, blobId);
  }

  private static void checkBlobInfo(BlobInfo blobInfo, OperationInfo operationInfo, String operationName) {
    Assert.assertNotNull("Null blobInfo for operation: " + operationName, blobInfo);
    Assert.assertEquals("Blob size in info does not match expected for operation: " + operationName,
        operationInfo.properties.getBlobSize(), blobInfo.getBlobProperties().getBlobSize());
    Assert.assertEquals("Service ID in info does not match expected for operation: " + operationName,
        operationInfo.properties.getServiceId(), blobInfo.getBlobProperties().getServiceId());
    Assert.assertArrayEquals("Unexpected user metadata for operation: " + operationName, operationInfo.userMetadata,
        blobInfo.getUserMetadata());
  }

  private static void checkBlob(ReadableStreamChannel channel, OperationInfo operationInfo, String operationName) {
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
            readBytes + bufLength <= operationInfo.data.length);
        while (buf.hasRemaining()) {
          Assert.assertEquals("Get and Put blob content should match, operation: " + operationName,
              operationInfo.data[readBytes++], buf.get());
        }
        getChannel.resolveOldestChunk(null);
      } while (readBytes < operationInfo.data.length);
      Assert.assertEquals(
          "the returned length in the future should be the length of data written, operation: " + operationName,
          (long) readBytes, (long) readIntoFuture.get());
      Assert.assertNull("There should be no more data in the channel, operation: " + operationName,
          getChannel.getNextChunk(0));
    } catch (Exception e) {
      Assert.fail("Exception while reading from getChannel from operation: " + operationName);
    }
  }

  private static String genLabel(String name, boolean nonBlocking, boolean afterDelete) {
    return name + (afterDelete ? "-deleted" : "") + (nonBlocking ? "-nb" : "-coord");
  }

  private void startPutBlob(boolean nonBlocking, OperationInfo opInfo) {
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(opInfo.data));
    Callback<String> callback = new TestCallback<String>(opInfo, false) {
      @Override
      void action(String result) {
        opInfo.blobId = result;
      }
    };
    Router router = nonBlocking ? nonBlockingRouter : coordinatorBackedRouter;
    Future<String> future = router.putBlob(opInfo.properties, opInfo.userMetadata, putChannel, callback);
    TestFuture<String> testFuture = new TestFuture<String>(future, genLabel("putBlob", nonBlocking, false), opInfo) {
      @Override
      void check() {
        checkBlobId(get(), getOperationName());
      }
    };
    opInfo.futures.add(testFuture);
  }

  private void startGetBlobInfo(boolean nonBlocking, final boolean afterDelete, final OperationInfo opInfo) {
    Callback<BlobInfo> callback = new TestCallback<>(opInfo, afterDelete);
    Router router = nonBlocking ? nonBlockingRouter : coordinatorBackedRouter;
    Future<BlobInfo> future = router.getBlobInfo(opInfo.blobId, callback);
    TestFuture<BlobInfo> testFuture =
        new TestFuture<BlobInfo>(future, genLabel("getBlobInfo", nonBlocking, afterDelete), opInfo) {
          @Override
          void check() {
            if (afterDelete) {
              try {
                future.get();
                Assert.fail("Blob should have been deleted in operation: " + getOperationName());
              } catch (Exception ignored) {
              }
            } else {
              checkBlobInfo(get(), opInfo, getOperationName());
            }
          }
        };
    opInfo.futures.add(testFuture);
  }

  private void startGetBlob(boolean nonBlocking, final boolean afterDelete, final OperationInfo opInfo) {
    Callback<ReadableStreamChannel> callback = new TestCallback<>(opInfo, afterDelete);
    Router router = nonBlocking ? nonBlockingRouter : coordinatorBackedRouter;
    Future<ReadableStreamChannel> future = router.getBlob(opInfo.blobId, callback);
    TestFuture<ReadableStreamChannel> testFuture =
        new TestFuture<ReadableStreamChannel>(future, genLabel("getBlob", nonBlocking, afterDelete), opInfo) {
          @Override
          void check() {
            if (afterDelete) {
              try {
                future.get();
                Assert.fail("Blob should have been deleted in operation: " + getOperationName());
              } catch (Exception ignored) {
              }
            } else {
              checkBlob(get(), opInfo, getOperationName());
            }
          }
        };
    opInfo.futures.add(testFuture);
  }

  private void startDeleteBlob(boolean nonBlocking, final OperationInfo opInfo) {
    Callback<Void> callback = new TestCallback<>(opInfo, false);
    Router router = nonBlocking ? nonBlockingRouter : coordinatorBackedRouter;
    Future<Void> future = router.deleteBlob(opInfo.blobId, callback);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, genLabel("deleteBlob", nonBlocking, false), opInfo) {
      @Override
      void check() {
        get();
      }
    };
    opInfo.futures.add(testFuture);
  }

  private void startWait(final OperationInfo opInfo) {
    final Callback<Void> callback = new TestCallback<>(opInfo, false);
    Future<Void> future = worker.schedule(new Callable<Void>() {
      @Override
      public Void call() {
        callback.onCompletion(null, null);
        return null;
      }
    }, 1, TimeUnit.SECONDS);
    TestFuture<Void> testFuture = new TestFuture<Void>(future, "wait", opInfo) {
      @Override
      void check() {
        get();
      }
    };
    opInfo.futures.add(testFuture);
  }

  private void continueChain(final OperationInfo opInfo) {
    synchronized (opInfo.futures) {
      OperationType nextOp = opInfo.opChain.poll();
      if (nextOp == null) {
        opInfo.latch.countDown();
        return;
      }
      switch (nextOp) {
        case PUT_NB:
        case PUT_COORD:
          startPutBlob(nextOp.nonBlocking, opInfo);
          break;
        case GET_INFO_NB:
        case GET_INFO_DELETED_NB:
        case GET_INFO_COORD:
        case GET_INFO_DELETED_COORD:
          startGetBlobInfo(nextOp.nonBlocking, nextOp.afterDelete, opInfo);
          break;
        case GET_NB:
        case GET_DELETED_NB:
        case GET_COORD:
        case GET_DELETED_COORD:
          startGetBlob(nextOp.nonBlocking, nextOp.afterDelete, opInfo);
          break;
        case DELETE_NB:
        case DELETE_COORD:
          startDeleteBlob(nextOp.nonBlocking, opInfo);
          break;
        case WAIT:
          startWait(opInfo);
          break;
      }
    }
  }

  enum OperationType {
    PUT_NB(true, false),
    GET_INFO_NB(true, false),
    GET_NB(true, false),
    DELETE_NB(true, false),
    GET_INFO_DELETED_NB(true, true),
    GET_DELETED_NB(true, true),
    PUT_COORD(false, false),
    GET_INFO_COORD(false, false),
    GET_COORD(false, false),
    DELETE_COORD(false, false),
    GET_INFO_DELETED_COORD(false, true),
    GET_DELETED_COORD(false, true),
    WAIT(false, false);

    final boolean nonBlocking;
    final boolean afterDelete;

    OperationType(boolean nonBlocking, boolean afterDelete) {
      this.nonBlocking = nonBlocking;
      this.afterDelete = afterDelete;
    }
  }

  static class OperationInfo {
    final int operationId;
    final BlobProperties properties;
    final byte[] userMetadata;
    final byte[] data;
    final Queue<OperationType> opChain;
    final List<TestFuture> futures = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);
    String blobId;

    OperationInfo(int operationId, BlobProperties properties, byte[] userMetadata, byte[] data,
        Queue<OperationType> opChain) {
      this.operationId = operationId;
      this.properties = properties;
      this.userMetadata = userMetadata;
      this.data = data;
      this.opChain = opChain;
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
    final OperationInfo opInfo;

    TestFuture(Future<T> future, String operationType, OperationInfo opInfo) {
      this.future = future;
      this.operationType = operationType;
      this.opInfo = opInfo;
    }

    /**
     * Generate a name for the tested operation
     * @return the operation name
     */
    String getOperationName() {
      return operationType + "-" + opInfo.operationId;
    }

    /**
     * Return the value inside the future or throw an {@link AssertionError} if an exception
     * occurred.
     * @return the value inside the future
     */
    T get() {
      try {
        return this.future.get();
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("Exception occured in operation: " + getOperationName() + ", exception: " + e);
        return null;
      }
    }

    /**
     * Implement any testing logic here.
     */
    abstract void check();
  }

  /**
   * A callback for router operations that starts the next operation in the chain after completion.
   * The user can define a custom action on the result of the operation by overriding the {@code action()} method.
   * @param <T> The callback's result type
   */
  private class TestCallback<T> implements Callback<T> {
    final OperationInfo opInfo;
    final boolean expectError;

    TestCallback(OperationInfo opInfo, boolean expectError) {
      this.opInfo = opInfo;
      this.expectError = expectError;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      if (exception != null && !expectError) {
        opInfo.latch.countDown();
        return;
      }
      action(result);
      continueChain(opInfo);
    }

    /**
     * Perform custom actions on the result of the operation here by overriding this method.
     * @param result the result of the completed operation
     */
    void action(T result) {
    }
  }
}
