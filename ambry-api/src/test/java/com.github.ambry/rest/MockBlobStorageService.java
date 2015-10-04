package com.github.ambry.rest;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of the {@link BlobStorageService} that can be used in tests.
 * <p/>
 * Expected to echo back {@link RestMethod} when the request does not define a custom operation. Otherwise used to
 * induce errors and test error handling in the layers above {@link BlobStorageService}.
 * <p/>
 * If you ever need to implement functionality that cannot go here:-
 * Check if {@link RestRequest} is an instance of {@link MockRestRequest}. If it is, you can support any kind of
 * custom function as long as it is implemented in {@link MockRestRequest} or reachable through it as a callback.
 */
public class MockBlobStorageService implements BlobStorageService {
  public final static int BLOB_ID_SIZE = UUID.randomUUID().toString().length();
  public final static String OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION = "blobStorageThrowHandlingRuntimeException";
  public final static String OPERATION_THROW_HANDLING_REST_EXCEPTION = "blobStorageThrowHandlingRestException";

  private final ConcurrentHashMap<String, MaterializedBlob> blobs = new ConcurrentHashMap<String, MaterializedBlob>();
  private final ConcurrentSkipListSet<String> deletedBlobs = new ConcurrentSkipListSet<String>();
  private final BlobReader blobReader = new BlobReader(blobs);

  private volatile boolean serviceRunning = false;

  public MockBlobStorageService(ClusterMap clusterMap) {
    // This constructor is around so that this can be instantiated from the NioServerFactory.
    // We might have uses for the arguments in the future.
  }

  @Override
  public void start()
      throws InstantiationException {
    new Thread(blobReader).start();
    serviceRunning = true;
  }

  @Override
  public void shutdown() {
    serviceRunning = false;
    blobReader.storeBlob(null, null, null);
  }

  @Override
  public Future<ReadableStreamChannel> handleGet(RestRequest restRequest, Callback<ReadableStreamChannel> callback) {
    MockFuture<ReadableStreamChannel> future = new MockFuture<ReadableStreamChannel>();
    verifyServiceStarted(future, callback);
    handlePreChecks(restRequest, future, callback);
    String blobId = getBlobId(restRequest);
    ReadableStreamChannel blobStream = null;
    Exception e = null;
    if (deletedBlobs.contains(blobId)) {
      e = new RestServiceException("Blob deleted", RestServiceErrorCode.BlobDeleted);
    } else if (!blobs.contains(blobId)) {
      e = new RestServiceException("Blob not found", RestServiceErrorCode.BlobNotFound);
    } else {
      MaterializedBlob blob = blobs.get(blobId);
      blobStream = new ByteBufferRSC(blob.getBlob());
    }
    setFutureAndInvokeCallback(future, callback, blobStream, e);
    return future;
  }

  @Override
  public Future<String> handlePost(RestRequest restRequest, Callback<String> callback) {
    MockFuture<String> future = new MockFuture<String>();
    verifyServiceStarted(future, callback);
    handlePreChecks(restRequest, future, callback);
    blobReader.storeBlob(restRequest, future, callback);
    return future;
  }

  @Override
  public Future<Void> handleDelete(RestRequest restRequest, Callback<Void> callback) {
    MockFuture<Void> future = new MockFuture<Void>();
    verifyServiceStarted(future, callback);
    handlePreChecks(restRequest, future, callback);
    String blobId = getBlobId(restRequest);
    Exception e = null;
    if (!deletedBlobs.contains(blobId) && blobs.containsKey(blobId)) {
      deletedBlobs.add(blobId);
      blobs.remove(blobId);
    } else if (!deletedBlobs.contains(blobId)) {
      e = new RestServiceException("Blob not found", RestServiceErrorCode.BlobNotFound);
    }
    setFutureAndInvokeCallback(future, callback, null, e);
    return future;
  }

  @Override
  public Future<BlobInfo> handleHead(RestRequest restRequest, Callback<BlobInfo> callback) {
    MockFuture<BlobInfo> future = new MockFuture<BlobInfo>();
    verifyServiceStarted(future, callback);
    handlePreChecks(restRequest, future, callback);
    String blobId = getBlobId(restRequest);
    BlobInfo blobInfo = null;
    Exception e = null;
    if (deletedBlobs.contains(blobId)) {
      e = new RestServiceException("Blob deleted", RestServiceErrorCode.BlobDeleted);
    } else if (!blobs.contains(blobId)) {
      e = new RestServiceException("Blob not found", RestServiceErrorCode.BlobNotFound);
    } else {
      MaterializedBlob blob = blobs.get(blobId);
      blobInfo = new BlobInfo(blob.getBlobProperties(), blob.getUserMetadata());
    }
    setFutureAndInvokeCallback(future, callback, blobInfo, e);
    return future;
  }

  private void verifyServiceStarted(MockFuture future, Callback callback) {
    if (!serviceRunning) {
      setFutureAndInvokeCallback(future, callback, null, new IllegalStateException("BlobStorageService not started"));
    }
  }

  /**
   * Performs any pre checks and also throws exceptions if the request desires it.
   * @param restRequest the {@link RestRequest} that needs to be handled
   */
  private void handlePreChecks(RestRequest restRequest, MockFuture future, Callback callback) {
    String blobId = getBlobId(restRequest);
    if (OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION.equals(blobId)) {
      // exception message is blobId so that it can be verified by the test.
      setFutureAndInvokeCallback(future, callback, null, new RuntimeException(blobId));
    } else if (OPERATION_THROW_HANDLING_REST_EXCEPTION.equals(blobId)) {
      setFutureAndInvokeCallback(future, callback, null,
          new RestServiceException(blobId, RestServiceErrorCode.InternalServerError));
    } else if (restRequest.getRestMethod() != RestMethod.POST && blobId.length() != BLOB_ID_SIZE) {
      setFutureAndInvokeCallback(future, callback, null,
          new RestServiceException("Invalid blob id", RestServiceErrorCode.InvalidArgs));
    }
  }

  /**
   * Determines the blob ID desired by the request.
   * @param restRequest a {@link RestRequest} that represents the request.
   * @return the blob ID desired by the request.
   */
  private String getBlobId(RestRequest restRequest) {
    String path = restRequest.getPath();
    return path.startsWith("/") ? path.substring(1, path.length()) : path;
  }

  protected static void setFutureAndInvokeCallback(MockFuture future, Callback callback, Object result, Exception e) {
    RuntimeException re;
    if (e instanceof RuntimeException) {
      re = (RuntimeException) e;
    } else {
      re = new RuntimeException(e);
    }
    future.done(result, re);
    if (callback != null) {
      callback.onCompletion(result, e);
    }
  }
}

class BlobReader implements Runnable {
  private final ConcurrentHashMap<String, MaterializedBlob> blobs;
  private final LinkedBlockingQueue<ReadableRestRequest<String>> blobsToBeRead =
      new LinkedBlockingQueue<ReadableRestRequest<String>>();

  public BlobReader(ConcurrentHashMap<String, MaterializedBlob> blobs) {
    this.blobs = blobs;
  }

  @Override
  public void run() {
    ReadableRestRequest<String> blobToBeRead = null;
    String blobId = null;
    Exception exception = null;
    while (true) {
      try {
        blobToBeRead = blobsToBeRead.take();
        if (blobToBeRead.getRestRequest() == null) {
          break;
        }
        blobId = readBlob(blobToBeRead.getRestRequest());
      } catch (Exception e) {
        exception = e;
      } finally {
        if (blobToBeRead == null) {
          throw new IllegalStateException(exception);
        } else {
          MockBlobStorageService
              .setFutureAndInvokeCallback(blobToBeRead.getFuture(), blobToBeRead.getCallback(), blobId, exception);
          blobToBeRead = null;
          blobId = null;
          exception = null;
        }
      }
    }
  }

  public void storeBlob(RestRequest restRequest, MockFuture<String> future, Callback<String> callback) {
    // TODO: extract blob props and user metadata
    blobsToBeRead.offer(new ReadableRestRequest<String>(restRequest, future, callback));
  }

  private String readBlob(RestRequest blobToBeRead)
      throws IOException {
    ByteBuffer blob = ByteBuffer.allocate((int) blobToBeRead.getSize());
    WritableByteChannel channel = new ByteBufferChannel(blob);
    while (blob.hasRemaining() && (blobToBeRead.read(channel) != -1)) {
      ;
    }
    if (!blob.hasRemaining() && blobToBeRead.read(channel) != -1) {
      throw new IllegalStateException("Channel filled up before request content was exhausted");
    }
    blob.flip();
    String blobId = UUID.randomUUID().toString();
    blobs.putIfAbsent(blobId, new MaterializedBlob(null, null, blob));
    return blobId;
  }
}

class ReadableRestRequest<T> {
  private final RestRequest restRequest;
  private final MockFuture<T> future;
  private final Callback<T> callback;

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public MockFuture<T> getFuture() {
    return future;
  }

  public Callback<T> getCallback() {
    return callback;
  }

  public ReadableRestRequest(RestRequest restRequest, MockFuture<T> future, Callback<T> callback) {
    this.restRequest = restRequest;
    this.future = future;
    this.callback = callback;
  }
}

class MaterializedBlob {
  private final BlobProperties blobProperties;
  private final byte[] userMetadata;
  private final ByteBuffer blob;

  public MaterializedBlob(BlobProperties blobProperties, byte[] userMetadata, ByteBuffer blob) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.blob = blob;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public byte[] getUserMetadata() {
    return userMetadata;
  }

  public ByteBuffer getBlob() {
    return blob;
  }
}

class MockFuture<T> implements Future<T> {
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile RuntimeException error;
  private volatile T result;

  /**
   * Mark this request as complete and unblock any threads waiting on its completion.
   * @param result The result for this request
   * @param error The error that occurred if there was one, or null.
   */
  public void done(T result, RuntimeException error) {
    this.error = error;
    this.result = result;
    this.latch.countDown();
  }

  /**
   * Await the completion of this request
   */
  public void await()
      throws InterruptedException {
    latch.await();
  }

  /**
   * Await the completion of this request (up to the given time interval)
   * @param timeout The maximum time to wait
   * @param unit The unit for the max time
   * @return true if the request completed, false if we timed out
   */
  public boolean await(long timeout, TimeUnit unit)
      throws InterruptedException {
    return latch.await(timeout, unit);
  }

  /**
   * The result for the request
   */
  public T result() {
    return result;
  }

  /**
   * The error thrown while processing this request
   */
  public RuntimeException error() {
    return error;
  }

  /**
   * Has the request completed?
   */
  public boolean completed() {
    return this.latch.getCount() == 0L;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return completed();
  }

  @Override
  public T get()
      throws InterruptedException, ExecutionException {
    this.await();
    return resultOrThrow();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean occurred = this.await(timeout, unit);
    if (!occurred) {
      throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit) + " ms.");
    }
    return resultOrThrow();
  }

  private T resultOrThrow()
      throws ExecutionException {
    if (this.error() != null) {
      throw new ExecutionException(this.error());
    } else {
      return result();
    }
  }
}

/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
class ByteBufferRSC implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final ByteBuffer buffer;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buffer}.
   * @param buffer the {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferRSC(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public long getSize() {
    return buffer.capacity();
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = -1;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else {
      try {
        bufferReadLock.lock();
        if (buffer.hasRemaining()) {
          bytesWritten = channel.write(buffer);
        }
      } finally {
        bufferReadLock.unlock();
      }
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}
