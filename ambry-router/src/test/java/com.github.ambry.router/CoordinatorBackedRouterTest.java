package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Blob;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorError;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.coordinator.MockCluster;
import com.github.ambry.coordinator.MockConnectionPool;
import com.github.ambry.coordinator.MockDataNode;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class CoordinatorBackedRouterTest {

  @Test
  public void instantiationTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    ClusterMap clusterMap = new MockClusterMap();
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);

    try {
      new CoordinatorBackedRouter(null, clusterMap, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    try {
      new CoordinatorBackedRouter(verifiableProperties, null, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    try {
      new CoordinatorBackedRouter(verifiableProperties, clusterMap, null);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    Properties properties = new Properties();
    properties.setProperty("router.operation.pool.size", "0");
    verifiableProperties = getVProps(properties);
    try {
      new CoordinatorBackedRouter(verifiableProperties, clusterMap, coordinator);
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  @Test
  public void multipleCloseTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    ClusterMap clusterMap = new MockClusterMap();
    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    Router router = new CoordinatorBackedRouter(verifiableProperties, clusterMap, coordinator);
    router.close();
    // should not throw exception
    router.close();
  }

  @Test
  public void putGetDeleteTest()
      throws Exception {
    VerifiableProperties verifiableProperties = getVProps(new Properties());
    ClusterMap clusterMap = new MockClusterMap();

    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    Coordinator coordinator = new AmbryCoordinator(verifiableProperties, clusterMap);
    triggerPutGetDeleteTest(verifiableProperties, clusterMap, coordinator);

    coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    triggerPutGetDeleteTest(verifiableProperties, clusterMap, coordinator);
  }

  @Test
  public void exceptionHandlingTest()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(MockCoordinator.CHECKED_EXCEPTION_ON_OPERATION_START, "true");
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();

    Coordinator coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    triggerExceptionHandlingTest(verifiableProperties, clusterMap, coordinator,
        MockCoordinator.CHECKED_EXCEPTION_ON_OPERATION_START);

    properties = new Properties();
    properties.setProperty(MockCoordinator.RUNTIME_EXCEPTION_ON_OPERATION_START, "true");
    verifiableProperties = getVProps(properties);
    coordinator = new MockCoordinator(verifiableProperties, clusterMap);
    triggerExceptionHandlingTest(verifiableProperties, clusterMap, coordinator,
        MockCoordinator.RUNTIME_EXCEPTION_ON_OPERATION_START);
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
    properties
        .setProperty("coordinator.connection.pool.factory", "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  private void verifyExceptionMatch(RouterOperationCallback routerOperationCallback, Future future) {
    try {
      future.get();
      fail("Callback had an exception but future.get() did not throw exception");
    } catch (Exception e) {
      assertEquals("Callback and future exceptions do not match", routerOperationCallback.getException(),
          e.getCause().getCause());
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
    ReadableByteChannel blobDataChannel = Channels.newChannel(new ByteArrayInputStream(content));
    Future<String> putBlobFuture;
    if (putBlobCallback == null) {
      putBlobFuture = router.putBlob(blobProperties, usermetadata, blobDataChannel);
    } else {
      putBlobFuture = router.putBlob(blobProperties, usermetadata, blobDataChannel, putBlobCallback);
      if (!putBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("putBlob() timed out");
      } else if (putBlobCallback.getException() != null) {
        verifyExceptionMatch(putBlobCallback, putBlobFuture);
        throw putBlobCallback.getException();
      }
      assertTrue("PutBlob: Future is not done but callback has been received", putBlobFuture.isDone());
      assertEquals("PutBlob: Future BlobId and callback BlobId do not match", putBlobFuture.get(),
          putBlobCallback.getResult());
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
      if (!getBlobInfoCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("getBlobInfo() timed out");
      } else if (getBlobInfoCallback.getException() != null) {
        verifyExceptionMatch(getBlobInfoCallback, getBlobInfoFuture);
        throw getBlobInfoCallback.getException();
      }
      assertTrue("GetBlobInfo: Future is not done but callback has been received", getBlobInfoFuture.isDone());
      assertEquals("GetBlobInfo: Future BlobInfo and callback BlobInfo do not match", getBlobInfoFuture.get(),
          getBlobInfoCallback.getResult());
    }
    BlobInfo blobInfo = getBlobInfoFuture.get();
    compareBlobProperties(blobProperties, blobInfo.getBlobProperties());
    byte[] getBlobInfoUserMetadata = blobInfo.getUserMetadata();
    assertArrayEquals("User metadata does not match what was put", usermetadata, getBlobInfoUserMetadata);
  }

  private void getBlobAndCompare(Router router, String blobId, byte[] content,
      RouterOperationCallback<BlobOutput> getBlobCallback)
      throws Exception {
    Future<BlobOutput> getBlobFuture;
    if (getBlobCallback == null) {
      getBlobFuture = router.getBlob(blobId);
    } else {
      getBlobFuture = router.getBlob(blobId, getBlobCallback);
      if (!getBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("getBlob() timed out");
      } else if (getBlobCallback.getException() != null) {
        verifyExceptionMatch(getBlobCallback, getBlobFuture);
        throw getBlobCallback.getException();
      }
      assertTrue("GetBlob: Future is not done but callback has been received", getBlobFuture.isDone());
      assertEquals("GetBlob: Future BlobOutput and callback BlobOutput do not match", getBlobFuture.get(),
          getBlobCallback.getResult());
    }
    BlobOutput blobOutput = getBlobFuture.get();
    byte[] blobDataBytes = new byte[(int) blobOutput.getSize()];
    blobOutput.read(ByteBuffer.wrap(blobDataBytes));
    assertArrayEquals("GetBlob data does not match what was put", content, blobDataBytes);
  }

  private void deleteBlob(Router router, String blobId, RouterOperationCallback<Void> deleteBlobCallback)
      throws Exception {
    Future<Void> deleteBlobFuture;
    if (deleteBlobCallback == null) {
      deleteBlobFuture = router.deleteBlob(blobId);
    } else {
      deleteBlobFuture = router.deleteBlob(blobId, deleteBlobCallback);
      if (!deleteBlobCallback.awaitCallback(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("deleteBlob() timed out");
      } else if (deleteBlobCallback.getException() != null) {
        verifyExceptionMatch(deleteBlobCallback, deleteBlobFuture);
        throw deleteBlobCallback.getException();
      }
      assertTrue("DeleteBlob: Future is not done but callback has been received", deleteBlobFuture.isDone());
    }
    // to throw any exceptions.
    deleteBlobFuture.get();
  }

  private void compareBlobProperties(BlobProperties srcBlobProperties, BlobProperties rcvdBlobProperties) {
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
  private void triggerPutGetDeleteTest(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      Coordinator coordinator)
      throws Exception {
    Router router = new CoordinatorBackedRouter(verifiableProperties, clusterMap, coordinator);
    for (int i = 0; i < 200; i++) {
      doPutGetDeleteTest(router, RouterUsage.WithCallback);
      doPutGetDeleteTest(router, RouterUsage.WithoutCallback);
    }
    router.close();
  }

  private void doPutGetDeleteTest(Router router, RouterUsage routerUsage)
      throws Exception {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = getByteArray(10);
    byte[] putContent = getByteArray(100);
    RouterOperationCallback<String> putBlobCallback = null;
    RouterOperationCallback<BlobInfo> getBlobInfoCallback = null;
    RouterOperationCallback<BlobOutput> getBlobCallback = null;
    RouterOperationCallback<Void> deleteBlobCallback = null;
    if (RouterUsage.WithCallback.equals(routerUsage)) {
      putBlobCallback = new RouterOperationCallback<String>();
      getBlobInfoCallback = new RouterOperationCallback<BlobInfo>();
      getBlobCallback = new RouterOperationCallback<BlobOutput>();
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
      matchRouterErrorCode(e, RouterErrorCode.BlobDeleted);
    }

    try {
      if (getBlobCallback != null) {
        getBlobCallback.reset();
      }
      getBlobAndCompare(router, blobId, putContent, getBlobCallback);
      fail("GetBlob on deleted blob should have failed");
    } catch (Exception e) {
      matchRouterErrorCode(e, RouterErrorCode.BlobDeleted);
    }
    // delete blob of deleted blob should NOT throw exception.
    if (deleteBlobCallback != null) {
      deleteBlobCallback.reset();
    }
    deleteBlob(router, blobId, deleteBlobCallback);
  }

  // exceptionHandlingTest() helpers
  private void triggerExceptionHandlingTest(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      Coordinator coordinator, String expectedErrorMsg)
      throws Exception {
    Router router = new CoordinatorBackedRouter(verifiableProperties, clusterMap, coordinator);
    doExceptionHandlingTest(router, RouterUsage.WithCallback, expectedErrorMsg);
    doExceptionHandlingTest(router, RouterUsage.WithoutCallback, expectedErrorMsg);
    router.close();
  }

  private void doExceptionHandlingTest(Router router, RouterUsage routerUsage, String expectedErrorMsg)
      throws Exception {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = getByteArray(10);
    byte[] putContent = getByteArray(100);
    RouterOperationCallback<String> putBlobCallback = null;
    RouterOperationCallback<BlobInfo> getBlobInfoCallback = null;
    RouterOperationCallback<BlobOutput> getBlobCallback = null;
    RouterOperationCallback<Void> deleteBlobCallback = null;
    if (RouterUsage.WithCallback.equals(routerUsage)) {
      putBlobCallback = new RouterOperationCallback<String>();
      getBlobInfoCallback = new RouterOperationCallback<BlobInfo>();
      getBlobCallback = new RouterOperationCallback<BlobOutput>();
      deleteBlobCallback = new RouterOperationCallback<Void>();
    }

    String blobId = "@@placeholder@";
    try {
      blobId = putBlob(router, putBlobProperties, putUserMetadata, putContent, putBlobCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      matchRouterErrorCode(e, RouterErrorCode.UnexpectedInternalError);
      matchExpectedCoordinatorErrorMessage(e, expectedErrorMsg);
    }

    try {
      getBlobInfoAndCompare(router, blobId, putBlobProperties, putUserMetadata, getBlobInfoCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      matchRouterErrorCode(e, RouterErrorCode.UnexpectedInternalError);
      matchExpectedCoordinatorErrorMessage(e, expectedErrorMsg);
    }

    try {
      getBlobAndCompare(router, blobId, putContent, getBlobCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      matchRouterErrorCode(e, RouterErrorCode.UnexpectedInternalError);
      matchExpectedCoordinatorErrorMessage(e, expectedErrorMsg);
    }

    try {
      deleteBlob(router, blobId, deleteBlobCallback);
      fail("MockCoordinator would have thrown exception which should have been propagated");
    } catch (Exception e) {
      matchRouterErrorCode(e, RouterErrorCode.UnexpectedInternalError);
      matchExpectedCoordinatorErrorMessage(e, expectedErrorMsg);
    }
  }

  private void matchExpectedCoordinatorErrorMessage(Exception e, String expectedErrorMsg) {
    String exceptionMsg = e.getMessage().substring(e.getMessage().lastIndexOf(": ") + 2);
    assertEquals("Unexpected error message", expectedErrorMsg, exceptionMsg);
  }
}

enum RouterUsage {
  WithCallback,
  WithoutCallback
}

class RouterOperationCallback<T> implements Callback<T> {
  private CountDownLatch callbackReceived = new CountDownLatch(1);
  private T result;
  private Exception exception;

  public T getResult() {
    return result;
  }

  public Exception getException() {
    return exception;
  }

  @Override
  public void onCompletion(T result, Exception exception) {
    this.result = result;
    this.exception = exception;
    callbackReceived.countDown();
  }

  public boolean awaitCallback(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return callbackReceived.await(timeout, timeUnit);
  }

  public void reset() {
    callbackReceived = new CountDownLatch(1);
    result = null;
    exception = null;
  }
}

class MockCoordinator implements Coordinator {
  protected static String CHECKED_EXCEPTION_ON_OPERATION_START = "coordinator.checked.exception.on.operation.start";
  protected static String RUNTIME_EXCEPTION_ON_OPERATION_START = "coordinator.runtime.exception.on.operation.start";

  private final AtomicBoolean open = new AtomicBoolean(true);
  private final ClusterMap clusterMap;
  private final MockCluster cluster;
  private final VerifiableProperties verifiableProperties;

  MockCoordinator(VerifiableProperties verifiableProperties, ClusterMap clusterMap) {
    this.verifiableProperties = verifiableProperties;
    this.clusterMap = clusterMap;
    cluster = new MockCluster(clusterMap);
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blobInputStream)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      ByteBufferInputStream materializedBlobStream =
          new ByteBufferInputStream(blobInputStream, (int) blobProperties.getBlobSize());
      int index = new Random().nextInt(clusterMap.getWritablePartitionIds().size());
      PartitionId writablePartition = clusterMap.getWritablePartitionIds().get(index);
      BlobId blobId = new BlobId(writablePartition);
      ServerErrorCode error = ServerErrorCode.No_Error;
      for (ReplicaId replicaId : writablePartition.getReplicaIds()) {
        DataNodeId dataNodeId = replicaId.getDataNodeId();
        MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
        BlobOutput blobOutput = new BlobOutput(blobProperties.getBlobSize(), materializedBlobStream.duplicate());
        Blob blob = new Blob(blobProperties, userMetadata, blobOutput);
        error = dataNode.put(blobId, blob);
        if (!ServerErrorCode.No_Error.equals(error)) {
          break;
        }
      }
      switch (error) {
        case No_Error:
          break;
        case Disk_Unavailable:
          throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
        default:
          throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
      }
      return blobId.getID();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public void deleteBlob(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      ServerErrorCode error = ServerErrorCode.No_Error;
      for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
        DataNodeId dataNodeId = replicaId.getDataNodeId();
        MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
        error = dataNode.delete(blobId);
        if (!(ServerErrorCode.No_Error.equals(error) || ServerErrorCode.Blob_Deleted.equals(error))) {
          break;
        }
      }
      switch (error) {
        case No_Error:
        case Blob_Deleted:
          break;
        case Blob_Not_Found:
        case Partition_Unknown:
          throw new CoordinatorException(error.toString(), CoordinatorError.BlobDoesNotExist);
        case Blob_Expired:
          throw new CoordinatorException(error.toString(), CoordinatorError.BlobExpired);
        case Disk_Unavailable:
          throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
        case IO_Error:
        default:
          throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
      }
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public BlobProperties getBlobProperties(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.BlobPropertiesAndError bpae = dataNode.getBlobProperties(blobId);
      handleGetError(bpae.getError());
      return bpae.getBlobProperties();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public ByteBuffer getBlobUserMetadata(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.UserMetadataAndError umae = dataNode.getUserMetadata(blobId);
      handleGetError(umae.getError());
      return umae.getUserMetadata();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public BlobOutput getBlob(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.BlobOutputAndError boae = dataNode.getData(blobId);
      handleGetError(boae.getError());
      return boae.getBlobOutput();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public void close()
      throws IOException {
    if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new IOException(CHECKED_EXCEPTION_ON_OPERATION_START);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    open.set(false);
  }

  private void handleGetError(ServerErrorCode error)
      throws CoordinatorException {
    switch (error) {
      case No_Error:
        break;
      case Blob_Not_Found:
      case Partition_Unknown:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobDoesNotExist);
      case Blob_Deleted:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobDeleted);
      case Blob_Expired:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobExpired);
      case Disk_Unavailable:
        throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
      case IO_Error:
      case Data_Corrupt:
      default:
        throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
    }
  }
}