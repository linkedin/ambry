/*
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.IdConverterFactory;
import com.github.ambry.frontend.Operations;
import com.github.ambry.frontend.PutBlobMetaInfo;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * An implementation of {@link Router} that holds blobs in memory.
 */
public class InMemoryRouter implements Router {
  public final static String OPERATION_THROW_EARLY_RUNTIME_EXCEPTION = "routerThrowEarlyRuntimeException";
  public final static String OPERATION_THROW_LATE_RUNTIME_EXCEPTION = "routerThrowLateRuntimeException";
  public final static String OPERATION_THROW_ROUTER_EXCEPTION = "routerThrowRouterException";
  public final static String ID_CONVERTER_FAIL_EXCEPTION = "idConverterFailException";
  private static final EnumSet<GetOption> ALLOW_EXPIRED_BLOB_GET =
      EnumSet.of(GetOption.Include_All, GetOption.Include_Expired_Blobs);
  private static final EnumSet<GetOption> ALLOW_DELETED_BLOB_GET =
      EnumSet.of(GetOption.Include_All, GetOption.Include_Deleted_Blobs);
  private final ConcurrentHashMap<String, InMemoryBlob> blobs = new ConcurrentHashMap<>();
  private final ConcurrentSkipListSet<String> deletedBlobs = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<String> undeletedBlobs = new ConcurrentSkipListSet<>();
  private final AtomicBoolean routerOpen = new AtomicBoolean(true);
  private final ExecutorService operationPool;
  private final NotificationSystem notificationSystem;
  private final ClusterMap clusterMap;
  private VerifiableProperties verifiableProperties;
  private List<StoreKey> mockedBlobChunkIds;
  private IdConverter idConverter = null;

  /**
   * Changes the {@link VerifiableProperties} instance with the router so that the behaviour can be changed on the fly.
   * @param verifiableProperties the{@link VerifiableProperties} that will dictate behaviour.
   */
  public void setVerifiableProperties(VerifiableProperties verifiableProperties) {
    this.verifiableProperties = verifiableProperties;
  }

  /**
   * Add the list of mock blob chunk Ids for get chunk BlobId test.
   * @param mockedBlobChunkIds List of {@link StoreKey}s.
   */
  public void addMockedBlobChunkIds(List<StoreKey> mockedBlobChunkIds) {
    this.mockedBlobChunkIds = mockedBlobChunkIds;
  }

  /**
   * Creates an instance of InMemoryRouter.
   *
   * @param verifiableProperties properties map that defines the behavior of this instance.
   * @param notificationSystem   the notification system to use to notify creation/deletion of blobs.
   * @param clusterMap           the cluster map for the cluster.
   * @param idConverterFactory
   */
  public InMemoryRouter(VerifiableProperties verifiableProperties, NotificationSystem notificationSystem,
      ClusterMap clusterMap, IdConverterFactory idConverterFactory) {
    Objects.requireNonNull(clusterMap);
    setVerifiableProperties(verifiableProperties);
    operationPool = Executors.newFixedThreadPool(1);
    this.notificationSystem = notificationSystem;
    this.clusterMap = clusterMap;
    if (idConverterFactory != null) {
      try {
        idConverter = idConverterFactory.getIdConverter();
      } catch (InstantiationException e) {
        throw new RuntimeException(ID_CONVERTER_FAIL_EXCEPTION);
      }
    }
  }

  /**
   * Creates an instance of InMemoryRouter.
   *
   * @param verifiableProperties properties map that defines the behavior of this instance.
   * @param clusterMap           the cluster map for the cluster.
   * @param idConverterFactory   the {@link IdConverterFactory}
   */
  public InMemoryRouter(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      IdConverterFactory idConverterFactory) {
    this(verifiableProperties, null, clusterMap, idConverterFactory);
  }

  /**
   * Representation of a blob in memory. Contains blob properties, user metadata and blob data.
   */
  public static class InMemoryBlob {
    private BlobProperties blobProperties;
    private final byte[] userMetadata;
    private final ByteBuffer blob;
    private final List<ChunkInfo> stitchedChunks;
    private short lifeVersion;

    public InMemoryBlob(BlobProperties blobProperties, byte[] userMetadata, ByteBuffer blob,
        List<ChunkInfo> stitchedChunks) {
      this.blobProperties =
          new BlobProperties(blob.remaining(), blobProperties.getServiceId(), blobProperties.getOwnerId(),
              blobProperties.getContentType(), blobProperties.isPrivate(), blobProperties.getTimeToLiveInSeconds(),
              blobProperties.getCreationTimeInMs(), blobProperties.getAccountId(), blobProperties.getContainerId(),
              blobProperties.isEncrypted(), blobProperties.getExternalAssetTag(), blobProperties.getContentEncoding(),
              blobProperties.getFilename(), blobProperties.getReservedMetadataBlobId());
      this.userMetadata = userMetadata;
      this.blob = blob;
      this.stitchedChunks = stitchedChunks;
      this.lifeVersion = 0;
    }

    public BlobProperties getBlobProperties() {
      return blobProperties;
    }

    public byte[] getUserMetadata() {
      return userMetadata;
    }

    /**
     * @return a list of the stitched chunks in this blob, or {@code null} if this was a direct upload.
     */
    public List<ChunkInfo> getStitchedChunks() {
      return stitchedChunks;
    }

    /**
     * @return the entire blob as a {@link ByteBuffer}
     */
    public ByteBuffer getBlob() {
      return ByteBuffer.wrap(blob.array());
    }

    /**
     * @param options any options specified for fetching the blob.
     * @return the blob content within the provided range, or the entire blob, if the range is null.
     * @throws RouterException if the range was non-null, but could not be resolved.
     */
    public ByteBuffer getBlob(GetBlobOptions options) throws RouterException {
      ByteBuffer buf;
      if (options.getRange() == null) {
        buf = getBlob();
      } else {
        ByteRange resolvedRange;
        try {
          resolvedRange =
              options.getRange().toResolvedByteRange(blob.array().length, options.resolveRangeOnEmptyBlob());
        } catch (IllegalArgumentException e) {
          throw new RouterException("Invalid range for blob", e, RouterErrorCode.RangeNotSatisfiable);
        }
        byte[] bytes = new byte[(int) resolvedRange.getRangeSize()];
        ByteBuffer duplicate = blob.duplicate();
        duplicate.position((int) resolvedRange.getStartOffset());
        duplicate.get(bytes);
        buf = ByteBuffer.wrap(bytes);
      }
      return buf;
    }

    /**
     * @return the lifeVersion of this blob.
     */
    public short getLifeVersion() {
      return lifeVersion;
    }

    /**
     * Set the lifeVersion of this blob.
     * @param lifeVersion The lifeVersion to set.
     */
    public void setLifeVersion(short lifeVersion) {
      if (lifeVersion < 0) {
        throw new IllegalArgumentException("Invalid lifeVersion " + lifeVersion);
      }
      this.lifeVersion = lifeVersion;
    }
  }

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback,
      QuotaChargeCallback quotaChargeCallback) {
    FutureResult<GetBlobResult> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    ReadableStreamChannel blobDataChannel = null;
    BlobInfo blobInfo = null;
    List<StoreKey> blobChunkIds = new ArrayList<>();
    Exception exception = null;
    try {
      checkBlobId(blobId);
      if (deletedBlobs.contains(blobId) && !ALLOW_DELETED_BLOB_GET.contains(options.getGetOption())) {
        exception = new RouterException("Blob deleted", RouterErrorCode.BlobDeleted);
      } else if (!blobs.containsKey(blobId)) {
        exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
      } else {
        InMemoryBlob blob = blobs.get(blobId);
        long expiresAtMs = Utils.addSecondsToEpochTime(blob.getBlobProperties().getCreationTimeInMs(),
            blob.getBlobProperties().getTimeToLiveInSeconds());
        if (expiresAtMs == Utils.Infinite_Time || expiresAtMs > SystemTime.getInstance().milliseconds()
            || ALLOW_EXPIRED_BLOB_GET.contains(options.getGetOption())) {
          switch (options.getOperationType()) {
            case Data:
              blobDataChannel = new ByteBufferRSC(blob.getBlob(options));
              break;
            case BlobInfo:
              blobInfo = new BlobInfo(blob.getBlobProperties(), blob.getUserMetadata());
              break;
            case BlobChunkIds:
              blobInfo = new BlobInfo(blob.getBlobProperties(), blob.getUserMetadata());
              blobChunkIds = new ArrayList<>(mockedBlobChunkIds);
              break;
            case All:
              blobDataChannel = new ByteBufferRSC(blob.getBlob(options));
              blobInfo = new BlobInfo(blob.getBlobProperties(), blob.getUserMetadata());
              break;
          }
        } else {
          exception = new RouterException("Blob expired", RouterErrorCode.BlobExpired);
        }
      }
    } catch (RouterException e) {
      exception = e;
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      GetBlobResult operationResult =
          exception == null ? new GetBlobResult(blobInfo, blobDataChannel, blobChunkIds) : null;
      completeOperation(futureResult, callback, operationResult, exception);
    }
    return futureResult;
  }

  @Override
  public Future<String> putBlob(RestRequest restRequest, BlobProperties blobProperties, byte[] usermetadata,
      ReadableStreamChannel channel, PutBlobOptions options, Callback<String> callback,
      QuotaChargeCallback quotaChargeCallback) {
    FutureResult<String> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    Callback<String> wrappedCallback =
        restRequest != null ? createIdConverterCallbackForPutAndStitch(restRequest, blobProperties, futureResult,
            callback) : callback;
    PostData postData =
        new PostData(blobProperties, usermetadata, channel, null, options, wrappedCallback, futureResult);
    operationPool.submit(new InMemoryBlobPoster(postData, blobs, notificationSystem, clusterMap,
        CommonTestUtils.getCurrentBlobIdVersion()));
    return futureResult;
  }

  @Override
  public Future<String> stitchBlob(RestRequest restRequest, BlobProperties blobProperties, byte[] userMetadata,
      List<ChunkInfo> chunksToStitch, PutBlobOptions options, Callback<String> callback,
      QuotaChargeCallback quotaChargeCallback) {
    FutureResult<String> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    Callback<String> wrappedCallback =
        restRequest != null && idConverter != null ? createIdConverterCallbackForPutAndStitch(restRequest,
            blobProperties, futureResult, callback) : callback;
    PostData postData =
        new PostData(blobProperties, userMetadata, null, chunksToStitch, PutBlobOptions.DEFAULT, wrappedCallback,
            futureResult);
    operationPool.submit(new InMemoryBlobPoster(postData, blobs, notificationSystem, clusterMap,
        CommonTestUtils.getCurrentBlobIdVersion()));
    return futureResult;
  }

  @Override
  public Future<Void> deleteBlob(RestRequest restRequest, String blobId, String serviceId, Callback<Void> callback,
      QuotaChargeCallback quotaChargeCallback) {
    FutureResult<Void> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    if (restRequest == null) {
      proceedWithDelete(blobId, serviceId, callback, futureResult);
    } else {
      try {
        String blobIdStr = getRequestPath(restRequest).getOperationOrBlobId(true);

        // Call idConverter to get blobId asynchronously
        idConverter.convert(restRequest, blobIdStr, null, new Callback<String>() {
          @Override
          public void onCompletion(String convertedBlobId, Exception exception) {
            if (exception != null) {
              // Handle error in conversion
              callback.onCompletion(null, exception);
            } else {
              List<String> blobIds = Arrays.stream(convertedBlobId.split(",")).collect(Collectors.toList());
              List<FutureResult<Void>> futures = new ArrayList<>();
              for (String blobId : blobIds) {
                FutureResult<Void> futureResult = new FutureResult<>();
                proceedWithDelete(blobId, serviceId, null, futureResult);
                futures.add(futureResult);
              }
              for (FutureResult<Void> future : futures) {
                try {
                  future.get();
                } catch (Exception e) {
                  // Handle error in delete operation
                  RouterException routerException = Utils.getRootCause(e, RouterException.class);
                  if (routerException == null) {
                    routerException = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
                  }
                  callback.onCompletion(null, routerException);
                  return;
                }
              }
              callback.onCompletion(null, null);
            }
          }
        });
      } catch (Exception e) {
        // Handle synchronous errors during header extraction
        callback.onCompletion(null, e);
        return futureResult;
      }
    }
    // Blob ID is not available, use idConverter to get it
    return futureResult;
  }

  @Override
  public Future<Void> updateBlobTtl(RestRequest restRequest, String blobId, String serviceId, long expiresAtMs,
      Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    FutureResult<Void> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    Callback<String> stringCallback = (result, exception) -> {
      callback.onCompletion(null, exception);
    };
    //!RequestPath.matchesOperation(blobId, Operations.NAMED_BLOB)
    //&& RestUtils.getRequestPath(restRequest).matchesOperation(Operations.NAMED_BLOB)
    //which means already go through id converter once. (put and stitch named blob first phase with short ttl)
    if (restRequest == null || !RequestPath.matchesOperation(blobId, Operations.NAMED_BLOB)
        && RestUtils.getRequestPath(restRequest).matchesOperation(Operations.NAMED_BLOB)) {
      proceedWithTtlUpdate(blobId, serviceId, expiresAtMs, restRequest, stringCallback, callback, futureResult);
    } else {
      try {
        idConverter.convert(restRequest, blobId, null, (convertedBlobId, exception) -> {
          if (exception != null) {
            stringCallback.onCompletion(null, exception);
          } else {
            proceedWithTtlUpdate(convertedBlobId, serviceId, expiresAtMs, restRequest, stringCallback, callback,
                futureResult);
          }
        });
      } catch (Exception e) {
        callback.onCompletion(null, e);
      }
    }
    return futureResult;
  }

  /**
   * Helper method to perform TTL update once blobId is available
   */
  /**
   * Helper method to perform TTL update once blobId is available
   */
  private void proceedWithTtlUpdate(String blobId, String serviceId, long expiresAtMs, RestRequest restRequest,
      Callback<String> stringCallback, Callback<Void> originalCallback, FutureResult<Void> futureResult) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    Exception exception = null;
    blobId = RestUtils.stripSlashAndExtensionFromId(blobId);
    Callback<Void> wrappedCallback = (restRequest != null)
        ? createIdConverterCallbackForTtlUpdate(restRequest, blobId, futureResult, stringCallback)
        : originalCallback;

    try {
      // Check blobId before performing the update
      checkBlobId(blobId);

      // Perform TTL update if blob is not deleted and exists
      if (!deletedBlobs.contains(blobId) && blobs.containsKey(blobId)) {
        InMemoryBlob blob = blobs.get(blobId);
        BlobProperties currentProps = blob.blobProperties;
        long newTtlSecs = Utils.getTtlInSecsFromExpiryMs(expiresAtMs, currentProps.getCreationTimeInMs());
        blob.blobProperties.setTimeToLiveInSeconds(newTtlSecs);

        // Notify the notification system if applicable
        if (notificationSystem != null) {
          notificationSystem.onBlobTtlUpdated(blobId, serviceId, expiresAtMs, null, null);
        }
      } else if (deletedBlobs.contains(blobId)) {
        exception = new RouterException("Blob has been deleted", RouterErrorCode.BlobDeleted);
      } else {
        exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
      }
    } catch (RouterException e) {
      exception = e;
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      // Complete the operation by calling the callback
      completeOperation(futureResult, wrappedCallback, null, exception);
    }
  }


  @Override
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback,
      QuotaChargeCallback quotaChargeCallback) {
    FutureResult<Void> futureResult = new FutureResult<>();
    if (!handlePrechecks(futureResult, callback)) {
      return futureResult;
    }
    Exception exception = null;
    try {
      checkBlobId(blobId);
      if (!blobs.containsKey(blobId)) {
        exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
      } else if (!deletedBlobs.contains(blobId)) {
        exception = new RouterException("Blob not deleted", RouterErrorCode.BlobNotDeleted);
      }
      undeletedBlobs.add(blobId);
      deletedBlobs.remove(blobId);
      if (notificationSystem != null) {
        notificationSystem.onBlobUndeleted(blobId, serviceId, null, null);
      }
    } catch (RouterException e) {
      exception = e;
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      completeOperation(futureResult, callback, null, exception);
    }
    return futureResult;
  }

  @Override
  public IdConverter getIdConverter() {
    return idConverter;
  }

  @Override
  public RouterConfig getRouterConfig() {
    return new RouterConfig(verifiableProperties);
  }

  @Override
  public void close() {
    if (routerOpen.compareAndSet(true, false)) {
      shutDownExecutorService(operationPool, 1, TimeUnit.MINUTES);
    }
  }

  /**
   * Put a blob with a specific blob ID version. Used for testing response path processing of such blobs.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param usermetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param blobIdVersion The blob ID version to use. The request will fail if this isn't a valid blob ID version.
   * @return A future that would contain the BlobId eventually.
   */
  public Future<String> putBlobWithIdVersion(BlobProperties blobProperties, byte[] usermetadata,
      ReadableStreamChannel channel, Short blobIdVersion) {
    FutureResult<String> futureResult = new FutureResult<>();
    PostData postData =
        new PostData(blobProperties, usermetadata, channel, null, PutBlobOptions.DEFAULT, null, futureResult);
    operationPool.submit(new InMemoryBlobPoster(postData, blobs, notificationSystem, clusterMap, blobIdVersion));
    return futureResult;
  }

  /**
   * Gets all the blobs that are "active" (not deleted).
   * @return a map of all blobs that are active.
   */
  public Map<String, InMemoryBlob> getActiveBlobs() {
    return Collections.unmodifiableMap(blobs);
  }

  /**
   * Gets the set of ids of blobs that have been deleted.
   * @return the set of ids of blobs that have been deleted.
   */
  public Set<String> getDeletedBlobs() {
    return Collections.unmodifiableSet(deletedBlobs);
  }

  /**
   * Does pre checks and throws exceptions if necessary or requested for.
   * @param futureResult the {@link FutureResult} to update in case the operation has to be completed.
   * @param callback the {@link Callback} that needs to be invoked in case the operation has to be completed. Can be
   *                 null.
   * @return if {@code true}, the rest of the code can continue.
   */
  private boolean handlePrechecks(FutureResult futureResult, Callback callback) {
    boolean continueOp = true;
    if (!routerOpen.get()) {
      continueOp = false;
      completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    } else if (verifiableProperties.containsKey(OPERATION_THROW_EARLY_RUNTIME_EXCEPTION)) {
      throw new RuntimeException(OPERATION_THROW_EARLY_RUNTIME_EXCEPTION);
    } else if (verifiableProperties.containsKey(OPERATION_THROW_LATE_RUNTIME_EXCEPTION)) {
      continueOp = false;
      completeOperation(futureResult, callback, null, new RuntimeException(OPERATION_THROW_LATE_RUNTIME_EXCEPTION));
    } else if (verifiableProperties.containsKey(OPERATION_THROW_ROUTER_EXCEPTION)) {
      continueOp = false;
      RouterErrorCode errorCode = RouterErrorCode.UnexpectedInternalError;
      try {
        errorCode = RouterErrorCode.valueOf(verifiableProperties.getString(OPERATION_THROW_ROUTER_EXCEPTION));
      } catch (IllegalArgumentException e) {
        // it's alright.
      }
      RouterException routerException = new RouterException(OPERATION_THROW_ROUTER_EXCEPTION, errorCode);
      completeOperation(futureResult, callback, null, routerException);
    }
    return continueOp;
  }

  private void checkBlobId(String blobId) throws RouterException {
    try {
      new BlobId(blobId, clusterMap);
    } catch (Exception e) {
      throw new RouterException("BlobId is invalid " + blobId, RouterErrorCode.InvalidBlobId);
    }
  }

  /**
   * Create id converter callback after router put the blob.
   * @param restRequest {@link RestRequest} to put the blob.
   * @param blobProperties {@link BlobProperties} for the blob.
   * @return
   */
  private Callback<String> createIdConverterCallbackForPutAndStitch(RestRequest restRequest, BlobProperties blobProperties,
      FutureResult<String> futureResult, Callback<String> callback) {
    return (blobId, exception) -> {
      if (exception != null) {
        // If putBlob fails, complete the future and callback with an error
        futureResult.done(null, exception);
        if (callback != null) {
          callback.onCompletion(null, exception);
        }
      } else {
        // Set internal header so ttl update don't need the converter to convert from blobName to blobId.
        if (!isStitchRequest(restRequest)) {
          blobProperties.setBlobSize(restRequest.getBlobBytesReceived());
        }
        // Call idConverter.convert after putBlob succeeds
        try {
          idConverter.convert(restRequest, blobId, blobProperties, callback);
        } catch (Exception e) {
          callback.onCompletion(null, e);
        }
      }
    };
  }

  private boolean isStitchRequest(RestRequest restRequest) {
    return RestUtils.isNamedBlobStitchRequest(restRequest) || RestUtils.getRequestPath(restRequest)
        .matchesOperation(Operations.STITCH);
  }

  /**
   * Create id converter callback after router ttl update.
   * @param restRequest {@link RestRequest} to put the blob.
   * @param blobId the blobId to update ttl.
   * @return
   */
  private Callback<Void> createIdConverterCallbackForTtlUpdate(RestRequest restRequest, String blobId,
      FutureResult<Void> futureResult, Callback<String> callback) {
    return (result, exception) -> {
      if (exception != null) {
        // If putBlob fails, complete the future and callback with an error
        futureResult.done(null, exception);
        if (callback != null) {
          callback.onCompletion(null, exception);
        }
      } else {
        // Call idConverter.convert after putBlob succeeds
        try {
          idConverter.convert(restRequest, blobId, null, callback);
        } catch (Exception e) {
          callback.onCompletion(null, e);
        }
      }
    };
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

  private String removeLeadingSlashIfNeeded(String blobId) {
    return blobId.startsWith("/") ? blobId.substring(1) : blobId;
  }

  /**
   * Helper method to perform delete once the blob Id is available.
   */
  private void proceedWithDelete(String blobId, String serviceId, Callback<Void> callback,
      FutureResult<Void> futureResult) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    Exception exception = null;

    try {
      checkBlobId(blobId);
      if (!deletedBlobs.contains(blobId) && blobs.containsKey(blobId)) {
        deletedBlobs.add(blobId);
        undeletedBlobs.remove(blobId);
        if (notificationSystem != null) {
          notificationSystem.onBlobDeleted(blobId, serviceId, null, null);
        }
      } else if (!deletedBlobs.contains(blobId)) {
        exception = new RouterException("Blob not found", RouterErrorCode.BlobDoesNotExist);
      }
    } catch (RouterException e) {
      exception = e;
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      completeOperation(futureResult, callback, null, exception);
    }
  }

  /**
   * Thread to read the post data async and store it.
   */
  private static class InMemoryBlobPoster implements Runnable {
    private final PostData postData;
    private final ConcurrentHashMap<String, InMemoryBlob> blobs;
    private final NotificationSystem notificationSystem;
    private final ClusterMap clusterMap;
    private final short blobIdVersion;

    /**
     * Create a new instance.
     * @param postData the data that came with the POST request as {@link PostData}.
     * @param blobs the list of blobs in memory.
     * @param notificationSystem the notification system to use to notify creation/deletion of blobs.
     * @param clusterMap the cluster map for the cluster.
     * @param blobIdVersion the blob ID version to use.
     */
    public InMemoryBlobPoster(PostData postData, ConcurrentHashMap<String, InMemoryBlob> blobs,
        NotificationSystem notificationSystem, ClusterMap clusterMap, short blobIdVersion) {
      this.postData = postData;
      this.blobs = blobs;
      this.notificationSystem = notificationSystem;
      this.clusterMap = clusterMap;
      this.blobIdVersion = blobIdVersion;
    }

    @Override
    public void run() {
      String operationResult = null;
      Exception exception = null;
      try {
        String blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID,
            postData.getBlobProperties().getAccountId(), postData.getBlobProperties().getContainerId(),
            getPartitionForPut(), false, BlobId.BlobDataType.DATACHUNK).getID();
        if (blobs.containsKey(blobId)) {
          exception = new RouterException("Blob ID duplicate created.", RouterErrorCode.UnexpectedInternalError);
        }
        ByteBuffer blobData;
        if (postData.getChunksToStitch() != null) {
          ByteArrayOutputStream stitchedContentStream = new ByteArrayOutputStream();
          for (ChunkInfo chunkInfo : postData.getChunksToStitch()) {
            stitchedContentStream.write(blobs.get(chunkInfo.getBlobId()).getBlob().array());
          }
          blobData = ByteBuffer.wrap(stitchedContentStream.toByteArray());
        } else {
          blobData = readBlob(postData.getReadableStreamChannel(), postData.getOptions().getMaxUploadSize());
        }
        InMemoryBlob blob = new InMemoryBlob(postData.getBlobProperties(), postData.getUsermetadata(), blobData,
            postData.getChunksToStitch());
        blobs.put(blobId, blob);
        if (notificationSystem != null) {
          notificationSystem.onBlobCreated(blobId, postData.getBlobProperties(), null, null,
              postData.getOptions().isChunkUpload() ? NotificationBlobType.DataChunk : NotificationBlobType.Simple);
        }
        if (postData.getOptions().skipCompositeChunk()) {
          // now generate the PutBlobMetaInfo which includes the data chunk list
          List<Pair<String, Long>> orderedChunkList = new ArrayList<>();
          orderedChunkList.add(new Pair<>(blobId, blob.getBlobProperties().getBlobSize()));
          String reservedMetadataBlobId = blob.getBlobProperties().getReservedMetadataBlobId();
          PutBlobMetaInfo putBlobMetaInfoObj = new PutBlobMetaInfo(orderedChunkList, reservedMetadataBlobId);
          operationResult = PutBlobMetaInfo.serialize(putBlobMetaInfoObj);
        } else {
          operationResult = blobId;
        }
      } catch (RouterException e) {
        exception = e;
      } catch (Exception e) {
        exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
      } finally {
        completeOperation(postData.getFuture(), postData.getCallback(), operationResult, exception);
      }
    }

    /**
     * Reads blob data and returns the content as a {@link ByteBuffer}.
     * @param postContent the blob data.
     * @param maxBlobSize the max blob size to be enforced, or null for no restriction.
     * @return the blob data in a {@link ByteBuffer}.
     * @throws RouterException
     * @throws InterruptedException
     */
    private ByteBuffer readBlob(ReadableStreamChannel postContent, Long maxBlobSize)
        throws RouterException, InterruptedException {
      ByteArrayOutputStream blobDataStream = new ByteArrayOutputStream();
      ByteBufferAWC channel = new ByteBufferAWC();
      postContent.readInto(channel, (result, exception) -> channel.close());
      ByteBuffer chunk = channel.getNextChunk();
      IllegalStateException exception = null;
      while (chunk != null) {
        byte[] chunkData = new byte[chunk.remaining()];
        chunk.get(chunkData);
        blobDataStream.write(chunkData, 0, chunkData.length);
        channel.resolveOldestChunk(exception);
        if (exception != null) {
          channel.close();
          throw exception;
        } else {
          chunk = channel.getNextChunk();
        }
      }
      if (maxBlobSize != null && blobDataStream.size() > maxBlobSize) {
        throw new RouterException("Blob exceeded max allowed size: " + maxBlobSize, RouterErrorCode.BlobTooLarge);
      }
      return ByteBuffer.wrap(blobDataStream.toByteArray());
    }

    /**
     * Choose a random {@link PartitionId} and return it. This code is partially copied from
     * {@code PutOperation#getPartitionForPut}.
     * @return the chosen {@link PartitionId}
     * @throws RouterException
     */
    private PartitionId getPartitionForPut() throws RouterException {
      List<? extends PartitionId> partitions = clusterMap.getWritablePartitionIds(null);
      if (partitions.isEmpty()) {
        throw new RouterException("No writable partitions available.", RouterErrorCode.AmbryUnavailable);
      }
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    }
  }

  /**
   * Data that comes with the POST request. Contains blob properties, user metadata and blob data. Also has the
   * future and callback that need to be invoked on operation completion.
   */
  private static class PostData {
    private final BlobProperties blobProperties;
    private final byte[] usermetadata;
    private final ReadableStreamChannel readableStreamChannel;
    private final List<ChunkInfo> chunksToStitch;
    private final PutBlobOptions options;
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

    /**
     * @return the list of chunks to stitch, or null if this is a direct upload request.
     */
    public List<ChunkInfo> getChunksToStitch() {
      return chunksToStitch;
    }

    public PutBlobOptions getOptions() {
      return options;
    }

    public FutureResult<String> getFuture() {
      return future;
    }

    public Callback<String> getCallback() {
      return callback;
    }

    PostData(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel readableStreamChannel,
        List<ChunkInfo> chunksToStitch, PutBlobOptions options, Callback<String> callback,
        FutureResult<String> future) {
      this.blobProperties = blobProperties;
      this.usermetadata = usermetadata;
      this.readableStreamChannel = readableStreamChannel;
      this.chunksToStitch = chunksToStitch;
      this.options = options;
      this.future = future;
      this.callback = callback;
    }
  }
}

