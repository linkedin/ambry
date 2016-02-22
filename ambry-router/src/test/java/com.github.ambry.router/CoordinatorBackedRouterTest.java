package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link CoordinatorBackedRouter}.
 */
public class CoordinatorBackedRouterTest {

  /**
   * Tests instantiation of {@link CoordinatorBackedRouter} and {@link CoordinatorOperation} with various errors and
   * checks that the right exceptions are thrown.
   * @throws IOException
   */
  @Test
  public void instantiationTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    ClusterMap clusterMap = new MockClusterMap();
    CoordinatorBackedRouterMetrics metrics = new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);

    // RouterConfig null.
    try {
      new CoordinatorBackedRouter(null, metrics, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // CoordinatorBackedRouterMetrics null.
    try {
      new CoordinatorBackedRouter(routerConfig, null, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // Coordinator null.
    try {
      new CoordinatorBackedRouter(routerConfig, metrics, null);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    Properties properties = new Properties();
    properties.setProperty("router.scaling.unit.count", "0");
    verifiableProperties = getVProps(properties);
    routerConfig = new RouterConfig(verifiableProperties);
    try {
      new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // CoordinatorOperation instantiation test
    verifiableProperties = getVProps(new Properties());
    routerConfig = new RouterConfig(verifiableProperties);
    try {
      new CoordinatorOperation(new CoordinatorBackedRouter(routerConfig, metrics, coordinator),
          new FutureResult<String>(), "@@blobid@@", null, CoordinatorOperationType.PutBlob);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Closes the {@link CoordinatorBackedRouter} multiple times and checks that there is no exception.
   * @throws IOException
   */
  @Test
  public void multipleCloseTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    ClusterMap clusterMap = new MockClusterMap();
    CoordinatorBackedRouterMetrics metrics = new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    Router router = new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    router.close();
    // should not throw exception
    router.close();
  }

  /**
   * Tests the functionality of all variants of {@link CoordinatorBackedRouter#getBlob(String)},
   * {@link CoordinatorBackedRouter#getBlobInfo(String)}, {@link CoordinatorBackedRouter#deleteBlob(String)} and
   * {@link CoordinatorBackedRouter#putBlob(BlobProperties, byte[], ReadableStreamChannel)} by performing the following
   * operations with a link {@link MockCoordinator}:
   * 1. Generate random user metadata and blob data.
   * 2. Put blob and check that it succeeds. Use the obtained blob id for all subsequent operations.
   * 3. Obtain blob info and check that it matches with what was inserted.
   * 4. Obtain blob data and check that it matches with what was inserted.
   * 5. Delete blob and check that it succeeds.
   * 6. Try to get blob info and blob again and check that both throw an exception indicating that the blob is deleted.
   * 7. Delete blob again and check that no exception is thrown.
   * @throws Exception
   */
  @Test
  public void putGetDeleteTest()
      throws Exception {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    ClusterMap clusterMap = new MockClusterMap();
    CoordinatorBackedRouterMetrics metrics = new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    Router router = new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    for (int i = 0; i < 200; i++) {
      doPutGetDeleteTest(router, RouterUsage.WithCallback);
      doPutGetDeleteTest(router, RouterUsage.WithoutCallback);
    }
    router.close();
  }

  /**
   * Uses a {@link MockCoordinator} as the backing {@link Coordinator} for a {@link CoordinatorBackedRouter} and induces
   * various exceptions in the {@link MockCoordinator} to check for their handling in {@link CoordinatorBackedRouter}.
   * @throws Exception
   */
  @Test
  public void exceptionHandlingTest()
      throws Exception {
    ClusterMap clusterMap = new MockClusterMap();
    Properties properties = new Properties();
    properties.setProperty(MockCoordinator.CHECKED_EXCEPTION_ON_OPERATION_START, "true");
    VerifiableProperties verifiableProperties = getVProps(properties);
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    CoordinatorBackedRouterMetrics metrics = new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    Router router = new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    triggerExceptionHandlingTest(verifiableProperties, clusterMap, router, RouterErrorCode.UnexpectedInternalError,
        MockCoordinator.CHECKED_EXCEPTION_ON_OPERATION_START);

    try {
      router.close();
    } catch (IOException e) {
      assertEquals("Unexpected error message", MockCoordinator.CHECKED_EXCEPTION_ON_OPERATION_START, e.getMessage());
    }

    properties = new Properties();
    properties.setProperty(MockCoordinator.RUNTIME_EXCEPTION_ON_OPERATION_START, "true");
    verifiableProperties = getVProps(properties);
    routerConfig = new RouterConfig(verifiableProperties);
    coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    router = new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    triggerExceptionHandlingTest(verifiableProperties, clusterMap, router, RouterErrorCode.UnexpectedInternalError,
        MockCoordinator.RUNTIME_EXCEPTION_ON_OPERATION_START);
    router.close();

    verifiableProperties = getVProps(properties);
    routerConfig = new RouterConfig(verifiableProperties);
    coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    router = new CoordinatorBackedRouter(routerConfig, metrics, coordinator);
    router.close();
    triggerExceptionHandlingTest(verifiableProperties, clusterMap, router, RouterErrorCode.RouterClosed, null);
  }

  // helpers
  // general
  private byte[] getByteArray(int count) {
    byte[] buf = new byte[count];
    new Random().nextBytes(buf);
    return buf;
  }

  private VerifiableProperties getVProps(Properties properties) {
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DC1");
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties
        .setProperty("coordinator.connection.pool.factory", "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  private void verifyFutureCallbackExceptionMatch(RouterOperationCallback routerOperationCallback, Future future) {
    try {
      future.get();
      fail("Callback had an exception but future.get() did not throw exception");
    } catch (Exception e) {
      assertEquals("Callback and future exceptions do not match", routerOperationCallback.getException(), e.getCause());
    }
  }

  private void verifyExpectedException(Exception e, RouterErrorCode routerErrorCode, String expectedErrorMsg)
      throws Exception {
    matchRouterErrorCode(e, routerErrorCode);
    if (expectedErrorMsg != null) {
      String exceptionMsg = e.getMessage().substring(e.getMessage().lastIndexOf(": ") + 2);
      assertEquals("Unexpected error message", expectedErrorMsg, exceptionMsg);
    }
  }

  private void matchRouterErrorCode(Exception e, RouterErrorCode routerErrorCode)
      throws Exception {
    Throwable throwable = e;
    while (throwable != null) {
      if (throwable instanceof RouterException) {
        assertEquals("Unexpected RouterException", routerErrorCode, ((RouterException) throwable).getErrorCode());
        return;
      }
      throwable = throwable.getCause();
    }
    throw e;
  }

  private String putBlob(Router router, BlobProperties blobProperties, byte[] usermetadata, byte[] content,
      RouterOperationCallback<String> putBlobCallback)
      throws Exception {
    ReadableStreamChannel blobDataChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    Future<String> putBlobFuture;
    if (putBlobCallback == null) {
      putBlobFuture = router.putBlob(blobProperties, usermetadata, blobDataChannel);
    } else {
      putBlobFuture = router.putBlob(blobProperties, usermetadata, blobDataChannel, putBlobCallback);
      if (putBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("PutBlob: Future is not done but callback has been received", putBlobFuture.isDone());
        if (putBlobCallback.getException() == null) {
          assertEquals("PutBlob: Future BlobId and callback BlobId do not match", putBlobFuture.get(),
              putBlobCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(putBlobCallback, putBlobFuture);
          throw putBlobCallback.getException();
        }
      } else {
        throw new IllegalStateException("putBlob() timed out");
      }
    }
    return putBlobFuture.get();
  }

  private void getBlobInfoAndCompare(Router router, String blobId, BlobProperties blobProperties, byte[] usermetadata,
      RouterOperationCallback<BlobInfo> getBlobInfoCallback)
      throws Exception {
    Future<BlobInfo> getBlobInfoFuture;
    if (getBlobInfoCallback == null) {
      getBlobInfoFuture = router.getBlobInfo(blobId);
    } else {
      getBlobInfoFuture = router.getBlobInfo(blobId, getBlobInfoCallback);
      if (getBlobInfoCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("GetBlobInfo: Future is not done but callback has been received", getBlobInfoFuture.isDone());
        if (getBlobInfoCallback.getException() == null) {
          assertEquals("GetBlobInfo: Future BlobInfo and callback BlobInfo do not match", getBlobInfoFuture.get(),
              getBlobInfoCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(getBlobInfoCallback, getBlobInfoFuture);
          throw getBlobInfoCallback.getException();
        }
      } else {
        throw new IllegalStateException("getBlobInfo() timed out");
      }
    }
    BlobInfo blobInfo = getBlobInfoFuture.get();
    verifyBlobPropertiesMatch(blobProperties, blobInfo.getBlobProperties());
    byte[] getBlobInfoUserMetadata = blobInfo.getUserMetadata();
    assertArrayEquals("User metadata does not match what was put", usermetadata, getBlobInfoUserMetadata);
  }

  private void getBlobAndCompare(Router router, String blobId, byte[] content,
      RouterOperationCallback<ReadableStreamChannel> getBlobCallback)
      throws Exception {
    Future<ReadableStreamChannel> getBlobFuture;
    if (getBlobCallback == null) {
      getBlobFuture = router.getBlob(blobId);
    } else {
      getBlobFuture = router.getBlob(blobId, getBlobCallback);
      if (getBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("GetBlob: Future is not done but callback has been received", getBlobFuture.isDone());
        if (getBlobCallback.getException() == null) {
          assertEquals("GetBlob: Future Blob and callback Blob do not match", getBlobFuture.get(),
              getBlobCallback.getResult());
        } else {
          verifyFutureCallbackExceptionMatch(getBlobCallback, getBlobFuture);
          throw getBlobCallback.getException();
        }
      } else {
        throw new IllegalStateException("getBlob() timed out");
      }
    }
    ReadableStreamChannel blobData = getBlobFuture.get();

    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel((int) blobData.getSize());
    blobData.readInto(channel, null).get();
    assertArrayEquals("GetBlob data does not match what was put", content, channel.getData());
  }

  private void deleteBlob(Router router, String blobId, RouterOperationCallback<Void> deleteBlobCallback)
      throws Exception {
    Future<Void> deleteBlobFuture;
    if (deleteBlobCallback == null) {
      deleteBlobFuture = router.deleteBlob(blobId);
    } else {
      deleteBlobFuture = router.deleteBlob(blobId, deleteBlobCallback);
      if (deleteBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        assertTrue("DeleteBlob: Future is not done but callback has been received", deleteBlobFuture.isDone());
        if (deleteBlobCallback.getException() != null) {
          verifyFutureCallbackExceptionMatch(deleteBlobCallback, deleteBlobFuture);
          throw deleteBlobCallback.getException();
        }
      } else {
        throw new IllegalStateException("deleteBlob() timed out");
      }
    }
    // to throw any exceptions.
    deleteBlobFuture.get();
  }

  private void verifyBlobPropertiesMatch(BlobProperties srcBlobProperties, BlobProperties rcvdBlobProperties) {
    assertEquals("BlobProperties: Blob size does not match", srcBlobProperties.getBlobSize(),
        rcvdBlobProperties.getBlobSize());
    assertEquals("BlobProperties: Content type does not match", srcBlobProperties.getContentType(),
        rcvdBlobProperties.getContentType());
    assertEquals("BlobProperties: Creation time does not match", srcBlobProperties.getCreationTimeInMs(),
        rcvdBlobProperties.getCreationTimeInMs());
    assertEquals("BlobProperties: Owner ID does not match", srcBlobProperties.getOwnerId(),
        rcvdBlobProperties.getOwnerId());
    assertEquals("BlobProperties: Service ID does not match", srcBlobProperties.getServiceId(),
        rcvdBlobProperties.getServiceId());
    assertEquals("BlobProperties: TTL does not match", srcBlobProperties.getTimeToLiveInSeconds(),
        rcvdBlobProperties.getTimeToLiveInSeconds());
    assertEquals("BlobProperties: Privacy flag does not match", srcBlobProperties.isPrivate(),
        rcvdBlobProperties.isPrivate());
  }

  // putGetDeleteTest() helpers
  private void doPutGetDeleteTest(Router router, RouterUsage routerUsage)
      throws Exception {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = getByteArray(10);
    byte[] putContent = getByteArray(100);
    RouterOperationCallback<String> putBlobCallback = null;
    RouterOperationCallback<BlobInfo> getBlobInfoCallback = null;
    RouterOperationCallback<ReadableStreamChannel> getBlobCallback = null;
    RouterOperationCallback<Void> deleteBlobCallback = null;
    if (RouterUsage.WithCallback.equals(routerUsage)) {
      putBlobCallback = new RouterOperationCallback<String>();
      getBlobInfoCallback = new RouterOperationCallback<BlobInfo>();
      getBlobCallback = new RouterOperationCallback<ReadableStreamChannel>();
      deleteBlobCallback = new RouterOperationCallback<Void>();
    }

    String blobId = putBlob(router, putBlobProperties, putUserMetadata, putContent, putBlobCallback);
    getBlobInfoAndCompare(router, blobId, putBlobProperties, putUserMetadata, getBlobInfoCallback);
    getBlobAndCompare(router, blobId, putContent, getBlobCallback);
    deleteBlob(router, blobId, deleteBlobCallback);

    try {
      if (getBlobInfoCallback != null) {
        getBlobInfoCallback.reset();
      }
      getBlobInfoAndCompare(router, blobId, putBlobProperties, putUserMetadata, getBlobInfoCallback);
      fail("GetBlobInfo on deleted blob should have failed");
    } catch (Exception e) {
      verifyExpectedException(e, RouterErrorCode.BlobDeleted, null);
    }

    try {
      if (getBlobCallback != null) {
        getBlobCallback.reset();
      }
      getBlobAndCompare(router, blobId, putContent, getBlobCallback);
      fail("GetBlob on deleted blob should have failed");
    } catch (Exception e) {
      verifyExpectedException(e, RouterErrorCode.BlobDeleted, null);
    }
    // delete blob of deleted blob should NOT throw exception.
    if (deleteBlobCallback != null) {
      deleteBlobCallback.reset();
    }
    deleteBlob(router, blobId, deleteBlobCallback);
  }

  // exceptionHandlingTest() helpers
  private void triggerExceptionHandlingTest(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      Router router, RouterErrorCode routerErrorCode, String expectedErrorMsg)
      throws Exception {
    doExceptionHandlingTest(router, RouterUsage.WithCallback, routerErrorCode, expectedErrorMsg);
    doExceptionHandlingTest(router, RouterUsage.WithoutCallback, routerErrorCode, expectedErrorMsg);
  }

  private void doExceptionHandlingTest(Router router, RouterUsage routerUsage, RouterErrorCode routerErrorCode,
      String expectedErrorMsg)
      throws Exception {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = getByteArray(10);
    byte[] putContent = getByteArray(100);
    RouterOperationCallback<String> putBlobCallback = null;
    RouterOperationCallback<BlobInfo> getBlobInfoCallback = null;
    RouterOperationCallback<ReadableStreamChannel> getBlobCallback = null;
    RouterOperationCallback<Void> deleteBlobCallback = null;
    if (RouterUsage.WithCallback.equals(routerUsage)) {
      putBlobCallback = new RouterOperationCallback<String>();
      getBlobInfoCallback = new RouterOperationCallback<BlobInfo>();
      getBlobCallback = new RouterOperationCallback<ReadableStreamChannel>();
      deleteBlobCallback = new RouterOperationCallback<Void>();
    }

    String blobId = "@@placeholder@";
    try {
      blobId = putBlob(router, putBlobProperties, putUserMetadata, putContent, putBlobCallback);
      fail("Operation would have thrown exception which should have been propagated");
    } catch (Exception e) {
      verifyExpectedException(e, routerErrorCode, expectedErrorMsg);
    }

    try {
      getBlobInfoAndCompare(router, blobId, putBlobProperties, putUserMetadata, getBlobInfoCallback);
      fail("Operation would have thrown exception which should have been propagated");
    } catch (Exception e) {
      verifyExpectedException(e, routerErrorCode, expectedErrorMsg);
    }

    try {
      getBlobAndCompare(router, blobId, putContent, getBlobCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      verifyExpectedException(e, routerErrorCode, expectedErrorMsg);
    }

    try {
      deleteBlob(router, blobId, deleteBlobCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      verifyExpectedException(e, routerErrorCode, expectedErrorMsg);
    }
  }
}

/**
 * Enum used to select whether to use the callback or non callback variant of {@link Router} APIs.
 */
enum RouterUsage {
  WithCallback,
  WithoutCallback
}

/**
 * Class that can be used to receive callbacks from {@link Router}.
 * <p/>
 * On callback, stores the result and exception to be retrieved for later use.
 * @param <T> the type of result expected.
 */
class RouterOperationCallback<T> implements Callback<T> {
  private CountDownLatch callbackReceived = new CountDownLatch(1);
  private T result = null;
  private Exception exception = null;

  /**
   * Contains the result of the operation for which this was set as callback.
   * <p/>
   * If there was no result or if this was called before callback is received, it will return null.
   * @return the result of the operation (if any) for which this was set as callback.
   */
  public T getResult() {
    return result;
  }

  /**
   * Stores any exception thrown by the operation for which this was set as callback.
   * <p/>
   * If there was no exception or if this was called before callback is received, it will return null.
   * @return the exception encountered while performing the operation (if any) for which this was set as callback.
   */
  public Exception getException() {
    return exception;
  }

  @Override
  public void onCompletion(T result, Exception exception) {
    this.result = result;
    this.exception = exception;
    callbackReceived.countDown();
  }

  /**
   * Waits for the callback to be received.
   * @param timeout the time to wait for.
   * @param timeUnit the time unit of {@code timeout}.
   * @return {@code true} if callback was received within the timeout, {@code false} otherwise.
   * @throws InterruptedException if the wait for callback was interrupted.
   */
  public boolean awaitCallback(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return callbackReceived.await(timeout, timeUnit);
  }

  /**
   * Reset the state of this callback.
   */
  public void reset() {
    callbackReceived = new CountDownLatch(1);
    result = null;
    exception = null;
  }
}
