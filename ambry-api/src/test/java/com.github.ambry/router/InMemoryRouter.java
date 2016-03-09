package com.github.ambry.router;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * An implementation of {@link Router} that holds blobs in memory.
 */
public class InMemoryRouter implements Router {
  public final static String OPERATION_THROW_EARLY_RUNTIME_EXCEPTION = "routerThrowEarlyRuntimeException";
  public final static String OPERATION_THROW_LATE_RUNTIME_EXCEPTION = "routerThrowLateRuntimeException";
  public final static String OPERATION_THROW_ROUTER_EXCEPTION = "routerThrowRouterException";

  private static final long BLOB_ID_SIZE = UUID.randomUUID().toString().length();

  private final ConcurrentHashMap<String, InMemoryBlob> blobs = new ConcurrentHashMap<String, InMemoryBlob>();
  private final ConcurrentSkipListSet<String> deletedBlobs = new ConcurrentSkipListSet<String>();

  private final AtomicBoolean routerOpen = new AtomicBoolean(true);
  private final ExecutorService operationPool;
  private VerifiableProperties verifiableProperties;

  /**
   * Changes the {@link VerifiableProperties} instance with the router so that the behaviour can be changed on the fly.
   * @param verifiableProperties the{@link VerifiableProperties} that will dictate behaviour.
   */
  public void setVerifiableProperties(VerifiableProperties verifiableProperties) {
    this.verifiableProperties = verifiableProperties;
  }

  /**
   * Creates an instance of InMemoryRouter.
   * @param verifiableProperties properties map that defines the behavior of this instance.
   */
  public InMemoryRouter(VerifiableProperties verifiableProperties) {
    setVerifiableProperties(verifiableProperties);
    operationPool = Executors.newFixedThreadPool(1);
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getBlobInfo(blobId, null);
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    handlePrechecks(futureResult, callback);
    BlobInfo operationResult = null;
    Exception exception = null;
    if (blobId == null || blobId.length() != BLOB_ID_SIZE) {
      completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because blob ID is invalid", RouterErrorCode.InvalidBlobId));
    } else {
      try {
        if (deletedBlobs.contains(blobId)) {
          exception = new RouterException("Blob deleted", RouterErrorCode.BlobDeleted);
        } else if (!blobs.containsKey(blobId)) {
          exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
        } else {
          InMemoryBlob blob = blobs.get(blobId);
          operationResult = new BlobInfo(blob.getBlobProperties(), blob.getUserMetadata());
        }
      } catch (Exception e) {
        exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
      } finally {
        completeOperation(futureResult, callback, operationResult, exception);
      }
    }
    return futureResult;
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    return getBlob(blobId, null);
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    handlePrechecks(futureResult, callback);
    ReadableStreamChannel operationResult = null;
    Exception exception = null;
    if (blobId == null || blobId.length() != BLOB_ID_SIZE) {
      completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because blob ID is invalid", RouterErrorCode.InvalidBlobId));
    } else {
      try {
        if (deletedBlobs.contains(blobId)) {
          exception = new RouterException("Blob deleted", RouterErrorCode.BlobDeleted);
        } else if (!blobs.containsKey(blobId)) {
          exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
        } else {
          InMemoryBlob blob = blobs.get(blobId);
          operationResult = new ByteBufferRSC(blob.getBlob());
        }
      } catch (Exception e) {
        exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
      } finally {
        completeOperation(futureResult, callback, operationResult, exception);
      }
    }
    return futureResult;
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel) {
    return putBlob(blobProperties, usermetadata, channel, null);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    FutureResult<String> futureResult = new FutureResult<String>();
    handlePrechecks(futureResult, callback);
    PostData postData = new PostData(blobProperties, usermetadata, channel, futureResult, callback);
    operationPool.submit(new InMemoryBlobPoster(postData, blobs));
    return futureResult;
  }

  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    FutureResult<Void> futureResult = new FutureResult<Void>();
    handlePrechecks(futureResult, callback);
    Exception exception = null;
    if (blobId == null || blobId.length() != BLOB_ID_SIZE) {
      completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because blob ID is invalid", RouterErrorCode.InvalidBlobId));
    } else {
      try {
        if (!deletedBlobs.contains(blobId) && blobs.containsKey(blobId)) {
          deletedBlobs.add(blobId);
          blobs.remove(blobId);
        } else if (!deletedBlobs.contains(blobId)) {
          exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
        }
      } catch (Exception e) {
        exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
      } finally {
        completeOperation(futureResult, callback, null, exception);
      }
    }
    return futureResult;
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (routerOpen.compareAndSet(true, false)) {
        operationPool.shutdown();
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
      } else {
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
      }
    } catch (InterruptedException e) {
      // too bad.
    }
  }

  /**
   * Does pre checks and throws exceptions if necessary or requested for.
   * @param futureResult the {@link FutureResult} to update in case the operation has to be completed.
   * @param callback the {@link Callback} that needs to be invoked in case the operation has to be completed. Can be
   *                 null.
   */
  private void handlePrechecks(FutureResult futureResult, Callback callback) {
    if (!routerOpen.get()) {
      completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    } else if (verifiableProperties.containsKey(OPERATION_THROW_EARLY_RUNTIME_EXCEPTION)) {
      throw new RuntimeException(OPERATION_THROW_EARLY_RUNTIME_EXCEPTION);
    } else if (verifiableProperties.containsKey(OPERATION_THROW_LATE_RUNTIME_EXCEPTION)) {
      completeOperation(futureResult, callback, null, new RuntimeException(OPERATION_THROW_LATE_RUNTIME_EXCEPTION));
    } else if (verifiableProperties.containsKey(OPERATION_THROW_ROUTER_EXCEPTION)) {
      RouterErrorCode errorCode = RouterErrorCode.UnexpectedInternalError;
      try {
        errorCode = RouterErrorCode.valueOf(verifiableProperties.getString(OPERATION_THROW_ROUTER_EXCEPTION));
      } catch (IllegalArgumentException e) {
        // it's alright.
      }
      RouterException routerException = new RouterException(OPERATION_THROW_ROUTER_EXCEPTION, errorCode);
      completeOperation(futureResult, callback, null, routerException);
    }
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback the {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   */
  protected static void completeOperation(FutureResult futureResult, Callback callback, Object operationResult,
      Exception exception) {
    futureResult.done(operationResult, exception);
    if (callback != null) {
      callback.onCompletion(operationResult, exception);
    }
  }
}

/**
 * Thread to read the post data async and store it.
 */
class InMemoryBlobPoster implements Runnable {
  private final PostData postData;
  private final ConcurrentHashMap<String, InMemoryBlob> blobs;

  /**
   * Create a new instance.
   * @param postData the data that came with the POST request as {@link PostData}.
   * @param blobs the list of blobs in memory.
   */
  public InMemoryBlobPoster(PostData postData, ConcurrentHashMap<String, InMemoryBlob> blobs) {
    this.postData = postData;
    this.blobs = blobs;
  }

  @Override
  public void run() {
    String operationResult = null;
    Exception exception = null;
    try {
      operationResult = UUID.randomUUID().toString();
      if (blobs.containsKey(operationResult)) {
        exception =
            new RouterException("UUID is broken. Blob ID duplicate created.", RouterErrorCode.UnexpectedInternalError);
      }
      ByteBuffer blobData = readBlob(postData.getReadableStreamChannel());
      InMemoryBlob blob = new InMemoryBlob(postData.getBlobProperties(), postData.getUsermetadata(), blobData);
      blobs.put(operationResult, blob);
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      InMemoryRouter.completeOperation(postData.getFuture(), postData.getCallback(), operationResult, exception);
    }
  }

  /**
   * Reads blob data and returns the content as a {@link ByteBuffer}.
   * @param postContent the blob data.
   * @return the blob data in a {@link ByteBuffer}.
   * @throws InterruptedException
   */
  private ByteBuffer readBlob(ReadableStreamChannel postContent)
      throws InterruptedException {
    ByteBuffer blobData = ByteBuffer.allocate((int) postContent.getSize());
    ByteBufferAWC channel = new ByteBufferAWC();
    postContent.readInto(channel, new CloseWriteChannelCallback(channel));
    ByteBuffer chunk = channel.getNextChunk();
    IllegalStateException exception = null;
    while (chunk != null) {
      if (chunk.remaining() > blobData.remaining()) {
        exception = new IllegalStateException("Data size advertized does not match actual size of data");
      } else {
        blobData.put(chunk);
      }
      channel.resolveChunk(chunk, exception);
      if (exception != null) {
        channel.close();
        throw exception;
      } else {
        chunk = channel.getNextChunk();
      }
    }
    blobData.flip();
    return blobData;
  }
}

/**
 * Data that comes with the POST request. Contains blob properties, user metadata and blob data. Also has the
 * future and callback that need to be invoked on operation completion.
 */
class PostData {
  private final BlobProperties blobProperties;
  private final byte[] usermetadata;
  private final ReadableStreamChannel readableStreamChannel;
  private final FutureResult<String> future;
  private final Callback<String> callback;

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public byte[] getUsermetadata() {
    return usermetadata;
  }

  public ReadableStreamChannel getReadableStreamChannel() {
    return readableStreamChannel;
  }

  public FutureResult<String> getFuture() {
    return future;
  }

  public Callback<String> getCallback() {
    return callback;
  }

  public PostData(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel readableStreamChannel,
      FutureResult<String> future, Callback<String> callback) {
    this.blobProperties = blobProperties;
    this.usermetadata = usermetadata;
    this.readableStreamChannel = readableStreamChannel;
    this.future = future;
    this.callback = callback;
  }
}

/**
 * Representation of a blob in memory. Contains blob properties, user metadata and blob data.
 */
class InMemoryBlob {
  private final BlobProperties blobProperties;
  private final byte[] userMetadata;
  private final ByteBuffer blob;

  public InMemoryBlob(BlobProperties blobProperties, byte[] userMetadata, ByteBuffer blob) {
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
    return ByteBuffer.wrap(blob.array());
  }
}

/**
 * Callback for {@link ByteBufferAWC} that closes the channel on {@link #onCompletion(Long, Exception)}.
 */
class CloseWriteChannelCallback implements Callback<Long> {
  private final ByteBufferAWC channel;

  /**
   * Creates a callback to close {@code channel} on {@link #onCompletion(Long, Exception)}.
   * @param channel the {@link ByteBufferAWC} that needs to be closed.
   */
  public CloseWriteChannelCallback(ByteBufferAWC channel) {
    this.channel = channel;
  }

  @Override
  public void onCompletion(Long result, Exception exception) {
    channel.close();
  }
}
