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
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobId.BlobDataType;
import com.github.ambry.commons.BlobId.BlobIdType;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.frontend.PutBlobMetaInfo;
import com.github.ambry.frontend.ReservedMetadataIdMetrics;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.NettyRequest;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutOperation class is responsible for maintaining the state associated with a put operation, and completing it.
 * The associated object of a put operation ends up as one or more chunks, depending on its size. If the size is less
 * than max put chunk size, then the operation will involve a single chunk. Those that are larger are split into
 * multiple chunks and distributed across partitions individually. Each chunk will be a blob at the server and will
 * have its own blob id. For such composite blobs, an associated metadata chunk will be created that consists of
 * the blob ids of every data chunk; and the blob id of this metadata chunk will become the blob id of the overall
 * object.
 *
 * For simple blobs (blobs that are under the max put chunk size), only a single chunk is created and held
 * within a single PutChunk object which is used to complete the whole operation. No metadata chunks are created. The
 * blob id of the single data chunk will be the blob id of the blob.
 *
 * For composite blobs, a PutOperation consists of a fixed number of PutChunk objects and a MetadataPutChunk object.
 * Each PutChunk holds one of the chunks of the object at any time and attempts to put the chunk successfully, and then
 * possibly moves on to hold another chunk of the object. This is repeated until either the operation fails or until all
 * the chunks are put successfully. If all the chunks are put successfully, the MetadataPutChunk is created to put
 * the metadata chunk consisting of the blob ids of all the data chunks, and the successful put of the metadata
 * chunk successfully completes the operation.
 */
class PutOperation {
  // Operation arguments.
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private final AccountService accountService;
  private final BlobProperties passedInBlobProperties;
  private final byte[] userMetadata;
  private final String partitionClass;
  private final PutBlobOptions options;
  // provided for direct uploads only
  private final ReadableStreamChannel channel;
  // provided for stitched uploads
  private final List<ChunkInfo> chunksToStitch;
  private final ByteBufferAsyncWritableChannel chunkFillerChannel;
  private final FutureResult<String> futureResult;
  private final Callback<String> callback;
  private final RouterCallback routerCallback;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
  private final Time time;
  private BlobProperties finalBlobProperties;
  private boolean isEncryptionEnabled;
  private final QuotaChargeCallback quotaChargeCallback;

  // Whether the entire blob (all chunks) are compressible.  Default is null for undecided.
  // The value is set based on blobProperties and compression service configuration.
  // For example, if BlobProperties.ContentEncoding specified, then the entire blob will
  // not be compressed by setting this field to FALSE.
  // This field is set once a PutOperation constructor based on blob properties.
  // All chunk compression will check this field and skip compression if this value FALSE.
  private boolean isBlobCompressible;

  // Parameters associated with the state.

  // the list of PutChunks that will be used to hold chunks that are sent out. A PutChunk will only hold one chunk at
  // any time, but will be reused as and when the operation on the chunk is complete.
  final ConcurrentLinkedQueue<PutChunk> putChunks;
  // the total size of the object (the overall blob). This will be initialized to -1 indicating that the value is
  // not yet determined. Once the chunk filling is complete, this will have the actual size of the data read
  // from the channel.
  private long blobSize = -1;
  // total bytes of this object that has been filled so far by the ChunkFillerThread.
  private long bytesFilledSoFar;
  // the reference to the chunk in putChunks that was most recently filled or became eligible for getting filled.
  private PutChunk chunkToFill;
  // counter for tracking the chunks being filled.
  private int chunkCounter;
  // the current ByteBuf/position in the chunkFillerChannel.
  private ByteBuf channelReadBuf;
  // indicates whether chunk filling is complete and successful.
  private volatile boolean chunkFillingCompletedSuccessfully = false;
  // the metadata chunk for this operation. There will always be a metadata chunk that tracks the data chunks.
  // However, if the operation completes and results in only one data chunk, then the metadata chunk will not be sent
  // out.
  private final MetadataPutChunk metadataPutChunk;
  // denotes whether the operation is complete.
  private volatile boolean operationCompleted = false;
  // the blob id of the overall blob. This will be set if and when the operation is successful.
  private BlobId blobId;

  private String putBlobMetaInfo = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // To find the PutChunk to hand over the response quickly.
  private final Map<Integer, PutChunk> correlationIdToPutChunk = new HashMap<Integer, PutChunk>();
  // The time at which the operation was submitted.
  private final long submissionTimeMs;
  // The point in time at which the most recent wait for free chunk availability started.
  private long startTimeForChunkAvailabilityWaitMs;
  // The point in time at which the most recent wait for channel data availability started.
  private long startTimeForChannelDataAvailabilityMs;
  // The time spent in waiting for a chunk to become available to be filled when the channel had data.
  private long waitTimeForCurrentChunkAvailabilityMs;
  // The time spent by a chunk for data to be available in the channel.
  private long waitTimeForChannelDataAvailabilityMs;
  // The set of slipped blob ids generated during the put operation.
  private Set<BlobId> slippedPutBlobIds = new HashSet<>();

  // Variables to keep track of netty chunks
  private final RestRequest restRequest;
  private final String loggingContext;
  private final CompressionService compressionService;
  private final ReservedMetadataIdMetrics reservedMetadataIdMetrics;
  private boolean isSimpleBlob;

  private static final Logger logger = LoggerFactory.getLogger(PutOperation.class);

  /**
   * Construct a PutOperation with the given parameters. For any operation, based on the max chunk size for puts,
   * an object contained within the {@link ReadableStreamChannel} will either be put as a single blob if its size is
   * less than the max chunk size; or will be split into as many chunks as required each of which is no longer in
   * size than the max chunk put size, and a single metadata blob containing the information about each of these
   * chunks.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param notificationSystem the {@link NotificationSystem} to use for blob creation notifications.
   * @param accountService the {@link AccountService} used for account/container id and name mapping.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param channel the {@link ReadableStreamChannel} containing the blob data. This cannot be null.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param writableChannelEventListener a callback to be called on certain chunk filler channel events.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param partitionClass the partition class to choose partitions from. Can be {@code null} if no affinity is required
   * @param quotaChargeCallback {@link QuotaChargeCallback} to listen to events that should result in charging quota.
   * @param compressionService The compression service use to compress data chunks.
   * @return the {@link PutOperation}.
   */
  static PutOperation forUpload(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      ClusterMap clusterMap, NotificationSystem notificationSystem, AccountService accountService, byte[] userMetadata,
      ReadableStreamChannel channel, PutBlobOptions options, FutureResult<String> futureResult,
      Callback<String> callback, RouterCallback routerCallback,
      ByteBufferAsyncWritableChannel.ChannelEventListener writableChannelEventListener, KeyManagementService kms,
      CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time, BlobProperties blobProperties,
      String partitionClass, QuotaChargeCallback quotaChargeCallback, CompressionService compressionService) {
    return new PutOperation(routerConfig, routerMetrics, clusterMap, notificationSystem, accountService, userMetadata,
        channel, null, options, futureResult, callback, routerCallback, writableChannelEventListener, kms,
        cryptoService, cryptoJobHandler, time, blobProperties, partitionClass, quotaChargeCallback, compressionService);
  }

  /**
   * Construct a PutOperation for composite blob stitching with the given parameters. This will upload a single metadata
   * blob containing information about each of the chunks in {@code chunksToStitch}. The metadata blob created will
   * behave like a normal composite blob where the data chunks are the blobs listed in {@code chunksToStitch}.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param notificationSystem the {@link NotificationSystem} to use for blob creation notifications.
   * @param accountService the {@link AccountService} used for account/container id and name mapping.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param chunksToStitch the list of chunks to stitch together. The put operation business logic will treat the
   *                       metadata in the {@link ChunkInfo} object as a source of truth when validating that the
   *                       chunks can be stitched.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param partitionClass the partition class to choose partitions from. Can be {@code null} if no affinity is required
   * @param quotaChargeCallback {@link QuotaChargeCallback} to listen to events that should result in charging quota.
   * @param compressionService The compression service use to compress data chunks.
   * @return the {@link PutOperation}.
   */
  static PutOperation forStitching(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      ClusterMap clusterMap, NotificationSystem notificationSystem, AccountService accountService, byte[] userMetadata,
      List<ChunkInfo> chunksToStitch, FutureResult<String> futureResult, Callback<String> callback,
      RouterCallback routerCallback, KeyManagementService kms, CryptoService cryptoService,
      CryptoJobHandler cryptoJobHandler, Time time, BlobProperties blobProperties, String partitionClass,
      QuotaChargeCallback quotaChargeCallback, CompressionService compressionService) {
    return new PutOperation(routerConfig, routerMetrics, clusterMap, notificationSystem, accountService, userMetadata,
        null, chunksToStitch, PutBlobOptions.DEFAULT, futureResult, callback, routerCallback, null, kms, cryptoService,
        cryptoJobHandler, time, blobProperties, partitionClass, quotaChargeCallback, compressionService);
  }

  /**
   * Construct a PutOperation for composite blob stitching with the given parameters. This will upload a single metadata
   * blob containing information about each of the chunks in {@code chunksToStitch}. The metadata blob created will
   * behave like a normal composite blob where the data chunks are the blobs listed in {@code chunksToStitch}.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param notificationSystem the {@link NotificationSystem} to use for blob creation notifications.
   * @param accountService the {@link AccountService} used for account/container id and name mapping.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param chunksToStitch the list of chunks to stitch together. The put operation business logic will treat the
   *                       metadata in the {@link ChunkInfo} object as a source of truth when validating that the
   *                       chunks can be stitched.
   * @param options the {@link PutBlobOptions}.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param partitionClass the partition class to choose partitions from. Can be {@code null} if no affinity is required
   * @param quotaChargeCallback {@link QuotaChargeCallback} to listen to events that should result in charging quota.
   * @param compressionService The compression service use to compress data chunks.
   * @return the {@link PutOperation}.
   */
  static PutOperation forStitching(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      ClusterMap clusterMap, NotificationSystem notificationSystem, AccountService accountService, byte[] userMetadata,
      List<ChunkInfo> chunksToStitch, PutBlobOptions options, FutureResult<String> futureResult,
      Callback<String> callback, RouterCallback routerCallback, KeyManagementService kms, CryptoService cryptoService,
      CryptoJobHandler cryptoJobHandler, Time time, BlobProperties blobProperties, String partitionClass,
      QuotaChargeCallback quotaChargeCallback, CompressionService compressionService) {
    return new PutOperation(routerConfig, routerMetrics, clusterMap, notificationSystem, accountService, userMetadata,
        null, chunksToStitch, options == null ? PutBlobOptions.DEFAULT : options, futureResult, callback,
        routerCallback, null, kms, cryptoService, cryptoJobHandler, time, blobProperties, partitionClass,
        quotaChargeCallback, compressionService);
  }
  /**
   * Construct a PutOperation with the given parameters. This private constructor is used for both blob uploads and
   * post-upload stitching.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param notificationSystem the {@link NotificationSystem} to use for blob creation notifications.
   * @param accountService the {@link AccountService} used for account/container id and name mapping.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param channel the {@link ReadableStreamChannel} containing the blob data, or null for stitch requests.
   * @param chunksToStitch the list of chunks to stitch together, or null for data upload put operations.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param writableChannelEventListener a callback to be called on certain chunk filler channel events.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param partitionClass the partition class to choose partitions from. Can be {@code null} if no affinity is required
   * @param quotaChargeCallback {@link QuotaChargeCallback} to listen to events that should result in charging quota.
   * @param compressionService The compression service use to compress data chunks.
   */
  private PutOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      NotificationSystem notificationSystem, AccountService accountService, byte[] userMetadata,
      ReadableStreamChannel channel, List<ChunkInfo> chunksToStitch, PutBlobOptions options,
      FutureResult<String> futureResult, Callback<String> callback, RouterCallback routerCallback,
      ByteBufferAsyncWritableChannel.ChannelEventListener writableChannelEventListener, KeyManagementService kms,
      CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time, BlobProperties blobProperties,
      String partitionClass, QuotaChargeCallback quotaChargeCallback, CompressionService compressionService) {
    submissionTimeMs = time.milliseconds();
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    this.accountService = accountService;
    this.passedInBlobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.partitionClass = Objects.requireNonNull(partitionClass, "The provided partitionClass is null");
    this.channel = channel;
    this.options = options;
    this.isSimpleBlob = !options.isChunkUpload()
        && !options.skipCompositeChunk(); // if it is a chunked upload, then it should not be considered as simple blob.
    this.chunksToStitch = chunksToStitch;
    this.futureResult = futureResult;
    this.callback = callback;
    this.routerCallback = routerCallback;
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.time = time;
    this.quotaChargeCallback = quotaChargeCallback;
    this.compressionService = compressionService;
    this.isBlobCompressible = compressionService.isBlobCompressible(blobProperties);
    bytesFilledSoFar = 0;
    chunkCounter = -1;
    putChunks = new ConcurrentLinkedQueue<>();
    reservedMetadataIdMetrics =
        ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(routerMetrics.getMetricRegistry());
    chunkFillerChannel = new ByteBufferAsyncWritableChannel(writableChannelEventListener);
    isEncryptionEnabled = passedInBlobProperties.isEncrypted();
    restRequest = options.getRestRequest();
    loggingContext = makeLoggingContext();
    if (chunksToStitch != null && chunksToStitch.isEmpty()) {
      // chunksToStitch is null for non stitch requests.
      // For stitch requests, the current metadata format does not support empty chunk lists
      setOperationExceptionAndComplete(new RouterException("Must provide at least one chunk for stitchBlob call",
          RouterErrorCode.InvalidPutArgument));
    }
    MetadataPutChunk metadataPutChunk = null;
    try {
      metadataPutChunk = new MetadataPutChunk(chunksToStitch);
    } catch (RouterException routerException) {
      setOperationExceptionAndComplete(routerException);
    }
    this.metadataPutChunk = metadataPutChunk;
  }

  /**
   * @return {@code true} if blob needs to be encrypted. {@code false} otherwise
   */
  boolean isEncryptionEnabled() {
    return isEncryptionEnabled;
  }

  /**
   * Either start reading from the channel containing the blob data for normal put requests or assemble the metadata
   * chunk for stitch requests.
   */
  void startOperation() {
    if (operationCompleted) {
      // The operation has already completed before start, which means there are some invalid input arguments.
      return;
    }
    Exception exception = null;
    try {
      if (options.isChunkUpload() && options.getMaxUploadSize() > routerConfig.routerMaxPutChunkSizeBytes) {
        exception = new RouterException("Invalid max upload size for chunk upload: " + options.getMaxUploadSize(),
            RouterErrorCode.InvalidPutArgument);
      } else if (isStitchOperation()) {
        processChunksToStitch();
      } else {
        startReadingFromChannel();
      }
    } catch (Exception e) {
      exception = e;
    } finally {
      if (exception != null) {
        setOperationExceptionAndComplete(exception);
        routerCallback.onPollReady();
      }
    }
  }

  /**
   * Start reading from the channel containing the data for this operation.
   */
  private void startReadingFromChannel() {
    // The callback will be passed to NettyRequest and will be invoked in the ByteBufferAsyncWritableChannel's
    // resolveOldestChunk method, when it's the last http content chunk. Therefore, when this callback is invoked,
    // the chunk filling process for this PutOperation is finished.
    channel.readInto(chunkFillerChannel, (result, exception) -> {
      if (exception != null) {
        logger.info("{}: ChannelRead has exception, will terminate the operation", loggingContext, exception);
        setOperationExceptionAndComplete(exception);
        routerCallback.onPollReady();
      } else {
        blobSize = result;
        chunkFillingCompletedSuccessfully = true;
      }
      chunkFillerChannel.close();
    });
  }

  /**
   * Process the list of data chunks provided and prepare a metadata chunk with the provided chunk IDs.
   * @throws RouterException if validation of the chunks to stitch failed.
   */
  private void processChunksToStitch() throws RouterException {
    long totalSize = 0;
    long intermediateChunkSize =
        routerConfig.routerMetadataContentVersion == MessageFormatRecord.Metadata_Content_Version_V2
            ? chunksToStitch.get(0).getChunkSizeInBytes() : routerConfig.routerMaxPutChunkSizeBytes;
    metadataPutChunk.setIntermediateChunkSize(intermediateChunkSize);
    for (ListIterator<ChunkInfo> iter = chunksToStitch.listIterator(); iter.hasNext(); ) {
      int chunkIndex = iter.nextIndex();
      ChunkInfo chunkInfo = iter.next();
      BlobId chunkId = unwrapChunkInfo(chunkInfo, intermediateChunkSize, !iter.hasNext());
      metadataPutChunk.addChunkId(chunkId, chunkInfo.getChunkSizeInBytes(), chunkIndex);
      totalSize += chunkInfo.getChunkSizeInBytes();
    }
    blobSize = totalSize;
    // Update blobSize in passedInBlobProperties. This can be used by Handlers after Router finishes stitching the blob
    passedInBlobProperties.setBlobSize(blobSize);
    chunkFillingCompletedSuccessfully = true;
  }

  /**
   * Extract the {@link BlobId} from a {@link ChunkInfo} object and check the metadata in the object to ensure that this
   * is a valid chunk for stitching.
   * @param chunkInfo the {@link ChunkInfo} containing the blob ID and extra metadata.
   * @param intermediateChunkSize the size in bytes that intermediate chunks must be with the current metadata format.
   * @param lastChunk {@code true} if this is the last chunk in the list of blobs to stitch.
   * @return the {@link BlobId}.
   * @throws RouterException if validation of the chunk or blob ID parsing failed.
   */
  private BlobId unwrapChunkInfo(ChunkInfo chunkInfo, long intermediateChunkSize, boolean lastChunk)
      throws RouterException {
    long chunkSize = chunkInfo.getChunkSizeInBytes();
    long chunkExpirationTimeInMs = chunkInfo.getExpirationTimeInMs();

    if (chunkSize == 0 || chunkSize > intermediateChunkSize || (
        routerConfig.routerMetadataContentVersion == MessageFormatRecord.Metadata_Content_Version_V2 && !lastChunk
            && chunkSize < intermediateChunkSize)) {
      throw new RouterException(
          "Invalid chunkSize for " + (lastChunk ? "last" : "intermediate") + " chunk: " + chunkSize
              + "; intermediateChunkSize: " + intermediateChunkSize, RouterErrorCode.InvalidPutArgument);
    }

    long metadataExpirationTimeInMs = Utils.addSecondsToEpochTime(passedInBlobProperties.getCreationTimeInMs(),
        passedInBlobProperties.getTimeToLiveInSeconds());
    // the chunk TTL must be at least as long as the metadata blob to ensure that they do not become inaccessible before
    // the metadata chunk does.
    if (Utils.compareTimes(chunkExpirationTimeInMs, metadataExpirationTimeInMs) < 0) {
      throw new RouterException("Chunk expiration time (" + chunkExpirationTimeInMs + ") < metadata expiration time ("
          + metadataExpirationTimeInMs + ")", RouterErrorCode.InvalidPutArgument);
    }

    BlobId blobId = RouterUtils.getBlobIdFromString(chunkInfo.getBlobId(), clusterMap);
    // the metadata blob format does not support nested composite blobs.
    if (blobId.getBlobDataType() != BlobDataType.DATACHUNK) {
      throw new RouterException("Cannot stitch together multiple composite blobs", RouterErrorCode.InvalidBlobId);
    }

    return blobId;
  }

  /**
   * returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * Notify for overall blob creation if the operation is complete and the blob was put successfully. Also ensure that
   * notifications have been sent out for all successfully put data chunks.
   */
  void maybeNotifyForBlobCreation() {
    if (isOperationComplete()) {
      // only notify for data chunk creation on direct uploads.
      if (isComposite() && !isStitchOperation()) {
        metadataPutChunk.maybeNotifyForFirstChunkCreation();
      }
      if (blobId != null) {
        Pair<Account, Container> accountContainer =
            RouterUtils.getAccountContainer(accountService, getBlobProperties().getAccountId(),
                getBlobProperties().getContainerId());
        NotificationBlobType blobType = isComposite() ? NotificationBlobType.Composite
            : options.isChunkUpload() || options.skipCompositeChunk() ? NotificationBlobType.DataChunk
                : NotificationBlobType.Simple;
        notificationSystem.onBlobCreated(getBlobIdString(), getBlobProperties(), accountContainer.getFirst(),
            accountContainer.getSecond(), blobType);
      }
    }
  }

  /**
   * release all the chunk {@link ByteBuf} resource when the operation is done.
   */
  private synchronized void clearReadyChunks() {
    for (PutChunk chunk : putChunks) {
      logger.debug("{}: Chunk {} state: {}", loggingContext, chunk.getChunkIndex(), chunk.getState());
      // Only release the chunk in ready or complete mode. Filler thread will release the chunk in building mode
      // and the encryption thread will release the chunk in encrypting mode.
      if (chunk.isReady() || chunk.isComplete()) {
        chunk.clear();
      }
    }
  }

  /**
   * Release the blob content of chunks, including building chunk, excluding encrypted chunks since encrypted chunks have
   * other thread to release the data.
   */
  private synchronized void releaseDataForAllChunks() {
    for (PutChunk chunk : putChunks) {
      if (!(chunk.isFree() || chunk.isEncrypting())) {
        logger.info("{}: Clear unfinished chunk {} {} since operation is completed", loggingContext,
            chunk.getChunkIndex(), chunk.getState());
        chunk.releaseBlobContent();
      }
    }
  }

  void setOperationCompleted() {
    operationCompleted = true;
    clearReadyChunks();
  }

  /**
   * Clean up the chunks to release any data buffer. This should be invoked when terminating the operation with
   * an exception.
   */
  public void cleanupChunks() {
    releaseDataForAllChunks();
  }

  /**
   * For this operation, create and populate put requests for chunks (in the form of {@link RequestInfo}) to
   * send out.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
    if (operationCompleted) {
      return;
    }
    metadataPutChunk.poll(requestRegistrationCallback);
    if (metadataPutChunk.isComplete()) {
      if (getNumDataChunks() > 1) {
        onChunkOperationComplete(metadataPutChunk);
      }
    } else if (!metadataPutChunk.isReady()) {
      for (PutChunk chunk : putChunks) {
        if (chunk.isReady()) {
          chunk.poll(requestRegistrationCallback);
          if (chunk.isComplete()) {
            onChunkOperationComplete(chunk);
            // After each chunk is processed, check whether the operation itself has completed
            if (operationCompleted) {
              return;
            }
          }
        }
      }
    }
  }

  /**
   * Handle the given {@link ResponseInfo} by handing it over to the correct {@link PutChunk} that issued the request.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   * @param putResponse the {@link PutResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, PutResponse putResponse) {
    int correlationId = responseInfo.getRequestInfo().getRequest().getCorrelationId();
    PutChunk putChunk = correlationIdToPutChunk.remove(correlationId);
    logger.debug("{}: Handling response for {}", loggingContext, correlationId);
    putChunk.handleResponse(responseInfo, putResponse);
    if (putChunk.isComplete()) {
      onChunkOperationComplete(putChunk);
    }
  }

  /**
   * Called when the operation on a {@link PutChunk} is complete: That is, the chunk is successfully put or there was
   * an irrecoverable error in doing so. The {@link PutChunk} guarantees that in the former case,
   * the blobId of the chunk is set; and in the latter case, it is null.
   * @param chunk the {@link PutChunk} that has completed its operation.
   */
  private void onChunkOperationComplete(PutChunk chunk) {
    if (chunk.getChunkBlobId() == null) {
      // the overall operation has failed if any of the chunk fails.
      if (chunk.getChunkException() == null) {
        logger.error("{}: Operation on chunk failed, but no exception was set", loggingContext);
      }
      logger.error("{}: Failed putting chunk at index: {}, failing the entire operation", loggingContext,
          chunk.getChunkIndex());
      setOperationCompleted();
    } else if (chunk != metadataPutChunk) {
      // a data chunk has succeeded.
      metadataPutChunk.addChunkId(chunk.chunkBlobId, chunk.chunkBlobSize, chunk.chunkIndex);
      metadataPutChunk.maybeNotifyForChunkCreation(chunk);
      maybeUpdateSlippedPutSuccessMetrics(chunk);
    } else {
      blobId = chunk.getChunkBlobId();
      maybeUpdateSlippedPutSuccessMetrics(chunk);
      setOperationCompleted();
    }
    long operationLatencyMs = time.milliseconds() - chunk.chunkFillCompleteAtMs;
    if (chunk.chunkBlobProperties != null) {
      if (chunk.chunkBlobProperties.isEncrypted()) {
        routerMetrics.putEncryptedChunkOperationLatencyMs.update(operationLatencyMs);
      } else {
        routerMetrics.putChunkOperationLatencyMs.update(operationLatencyMs, TimeUnit.MILLISECONDS);
      }
    }
    chunk.clear();
  }

  /**
   * Returns whether chunk filling is complete (successfully or otherwise).
   * @return true if chunk filling is complete, false otherwise.
   */
  boolean isChunkFillingDone() {
    return isStitchOperation() || chunkFillingCompletedSuccessfully || operationCompleted;
  }

  /**
   * This method runs in the context of the ChunkFiller thread. As long as there are chunks available to
   * be written to, it gets the chunk that is to be filled and keeps filling it with the data from the
   * chunkFillerChannel, if there is any.
   * @throws InterruptedException if the call to get a chunk from the chunkFillerChannel is interrupted.
   */
  void fillChunks() {
    try {
      PutChunk chunkToFill;
      while (!isChunkFillingDone()) {
        // Attempt to fill a chunk
        if (channelReadBuf == null) {
          channelReadBuf = chunkFillerChannel.getNextByteBuf(0);
        }
        if (channelReadBuf != null) {
          if (channelReadBuf.readableBytes() > 0 && isChunkAwaitingResolution()) {
            // If first chunk is awaiting resolution, and there are more bytes in the channel prepare it first as a data chunk.
            // Also, this makes the blob a composite blob.
            isSimpleBlob = false;
            maybeResolveAwaitingChunk();
          }
          maybeStopTrackingWaitForChannelDataTime();
          chunkToFill = getChunkToFill();
          if (chunkToFill == null) {
            // channel has data, but no chunks are free to be filled yet.
            maybeStartTrackingWaitForChunkTime();
            break;
          } else {
            // channel has data, and there is a chunk that can be filled.
            maybeStopTrackingWaitForChunkTime();
            bytesFilledSoFar += chunkToFill.fillFrom(channelReadBuf);
            enforceMaxUploadSize();

            if (chunkToFill.isReady() && !chunkToFill.chunkBlobProperties.isEncrypted()) {
              routerCallback.onPollReady();
            }
            if (!channelReadBuf.isReadable()) {
              chunkFillerChannel.resolveOldestChunk(null);
              channelReadBuf = null;
            }
          }
        } else {
          // channel does not have more data yet.
          if (getFreeChunk() != null) {
            // this means there is a chunk available to be filled, but no data in the channel.
            maybeStartTrackingWaitForChannelDataTime();
          }
          break;
        }
      }
      if (chunkFillingCompletedSuccessfully) {
        // If the blob size is less than 4MB or the last chunk size is less than 4MB, than this lastChunk will be
        // the chunk above.
        PutChunk lastChunk = getChunkInState(ChunkState.Building);

        // The last chunk could be the second chunk, if the blob size is less than 8MB. We need to check if any chunk is
        // awaiting resolution, and if so, resolve it first.
        maybeResolveAwaitingChunk();

        if (lastChunk != null) {
          if (chunkCounter != 0 && lastChunk.buf.readableBytes() == 0) {
            logger.trace("{}: The last buffer(s) received from chunkFillerChannel have no data, discarding them.",
                loggingContext);
            lastChunk.releaseBlobContent();
            lastChunk.state = ChunkState.Free;
          } else {
            lastChunk.onFillComplete(true);
            updateChunkFillerWaitTimeMetrics();
          }
          if (lastChunk.isReady()) {
            routerCallback.onPollReady();
          }
        }
      }
      if (operationCompleted) {
        logger.debug("{}: Operation terminated, now try to clean up chunks", loggingContext);
        // Go over the put chunk list and release building, ready and complete chunks. This is because the multi-threading.
        // We have callback in chunk.readInto to release all the ready and complete chunks but that's in different
        // thread. There might a chance when callback runs, the last chunk is still building and then change to complete.
        releaseDataForAllChunks();
      }
    } catch (Exception e) {
      RouterException routerException = e instanceof RouterException ? (RouterException) e
          : new RouterException("PutOperation fillChunks encountered unexpected error", e,
              RouterErrorCode.UnexpectedInternalError);
      PutChunk lastChunk = getChunkInState(ChunkState.Building);
      if (lastChunk != null) {
        // call release blob content, not clear, since clear should only be used in the main thread.
        lastChunk.releaseBlobContent();
      }
      PutChunk resolutionAwaitingChunk = getChunkInState(ChunkState.AwaitingBlobTypeResolution);
      if (resolutionAwaitingChunk != null) {
        resolutionAwaitingChunk.releaseBlobContent();
      }
      routerMetrics.chunkFillerUnexpectedErrorCount.inc();
      setOperationExceptionAndComplete(routerException);
      routerCallback.onPollReady();
    }
  }

  /**
   * Prepare the awaiting chunks to be sent to server.
   * Note that there can be only one awaiting chunk, and that chunk will always be the first data chunk of a blob.
   */
  private void maybeResolveAwaitingChunk() {
    PutChunk resolutionAwaitingChunk = getChunkInState(ChunkState.AwaitingBlobTypeResolution);
    if (resolutionAwaitingChunk != null) {
      resolutionAwaitingChunk.onFillComplete(true);
      if (resolutionAwaitingChunk.isReady()) {
        routerCallback.onPollReady();
      }
    }
  }

  /**
   * @return {@code true} if there is any chunk in ChunkState.AwaitingBlobTypeResolution, {@code false} otherwise.
   */
  private boolean isChunkAwaitingResolution() {
    return getChunkInState(ChunkState.AwaitingBlobTypeResolution) != null;
  }

  /**
   * @return a snapshot of current put chunks
   */
  List<PutChunk> getPutChunks() {
    return new ArrayList<>(putChunks);
  }

  /**
   * Appends the given slipped chunk blob id onto the {@code slippedPutBlobIds}.
   * @param chunkBlobId The slipped chunk blob id.
   */
  synchronized private void appendSlippedPutBlobId(BlobId chunkBlobId) {
    slippedPutBlobIds.add(chunkBlobId);
  }

  /**
   *
   * @return the set of slipped chunk blob ids.
   */
  Set<BlobId> getSlippedPutBlobIds() {
    return slippedPutBlobIds;
  }

  /**
   * @return QuotaChargeCallback associated with this request.
   */
  public QuotaChargeCallback getQuotaChargeCallback() {
    return quotaChargeCallback;
  }

  /**
   * Update the metrics for slipped puts if slipped puts were attempted.
   */
  private void maybeUpdateSlippedPutSuccessMetrics(PutChunk chunk) {
    if (chunk.failedAttempts > 0) {
      logger.trace("{}: Slipped put succeeded for chunk: {}", loggingContext, chunk.getChunkBlobId());
      routerMetrics.slippedPutSuccessCount.inc();
      if (chunk instanceof MetadataPutChunk) {
        routerMetrics.metadataSlippedPutSuccessCount.inc();
      }
    } else {
      logger.trace("{}: Successfully put chunk: {} ", loggingContext, chunk.getChunkBlobId());
    }
  }

  /**
   * Called whenever the channel has data but no free or building chunk is available to be filled.
   */
  private void maybeStartTrackingWaitForChunkTime() {
    if (startTimeForChunkAvailabilityWaitMs == 0) {
      // this is the first point in time after the last chunk filling (if any) when the filling was blocked due to
      // chunk unavailability, so mark this time.
      startTimeForChunkAvailabilityWaitMs = time.milliseconds();
    } else {
      // the wait was already initiated, so do nothing.
    }
  }

  /**
   * Called whenever the channel has data and there is a free or building chunk available to be filled.
   */
  private void maybeStopTrackingWaitForChunkTime() {
    if (startTimeForChunkAvailabilityWaitMs != 0) {
      // this is the first point in time since the last wait that a chunk became available for filling.
      waitTimeForCurrentChunkAvailabilityMs = time.milliseconds() - startTimeForChunkAvailabilityWaitMs;
      startTimeForChunkAvailabilityWaitMs = 0;
    }
  }

  /**
   * Called whenever a chunk is available to be filled but there is no data available in the channel.
   */
  private void maybeStartTrackingWaitForChannelDataTime() {
    if (startTimeForChannelDataAvailabilityMs == 0) {
      // this is the first point in time after the last time data was read from the channel that data became
      // unavailable in the channel, so mark this time.
      startTimeForChannelDataAvailabilityMs = time.milliseconds();
    } else {
      // the wait was already initiated, so do nothing.
    }
  }

  /**
   * Called whenever data becomes available in the channel.
   */
  private void maybeStopTrackingWaitForChannelDataTime() {
    if (startTimeForChannelDataAvailabilityMs != 0) {
      // this is the first point in time since the last wait that data became available in the channel.
      waitTimeForChannelDataAvailabilityMs += time.milliseconds() - startTimeForChannelDataAvailabilityMs;
      startTimeForChannelDataAvailabilityMs = 0;
    }
  }

  /**
   * Update metrics related to how long a channel had to wait for a chunk to become available for filling, and
   * how long the chunk had to wait for data to become available in the channel.
   */
  private void updateChunkFillerWaitTimeMetrics() {
    routerMetrics.waitTimeForFreeChunkAvailabilityMs.update(waitTimeForCurrentChunkAvailabilityMs);
    routerMetrics.waitTimeForChannelDataAvailabilityMs.update(waitTimeForChannelDataAvailabilityMs);
    waitTimeForCurrentChunkAvailabilityMs = 0;
    waitTimeForChannelDataAvailabilityMs = 0;
  }

  /**
   * Enforce that the bytes filled so far is less than or equal to the max blob size in the {@link PutBlobOptions}.
   * @throws RouterException
   */
  private void enforceMaxUploadSize() throws RouterException {
    if (bytesFilledSoFar > options.getMaxUploadSize()) {
      throw new RouterException("Blob is larger than max upload size argument: " + options.getMaxUploadSize(),
          RouterErrorCode.BlobTooLarge);
    }
  }

  /**
   * Get the chunk to be filled. At most one chunk for an operation will ever be in Building state. If there is such
   * a chunk, that is returned. If not, if there is a Free chunk, that is returned. If no Free chunks are available
   * either, then null is returned.
   * @return the chunk to fill, or null if there are no chunks eligible for filling.
   */
  private PutChunk getChunkToFill() throws RouterException {
    if (chunkToFill == null || !chunkToFill.isBuilding()) {
      chunkToFill = getFreeChunk();
      if (chunkToFill != null) {
        if (chunkCounter == Integer.MAX_VALUE) {
          throw new RouterException("Blob is too large", RouterErrorCode.BlobTooLarge);
        }
        chunkCounter++;
        chunkToFill.prepareForBuilding(chunkCounter);
      }
    }
    return chunkToFill;
  }

  /**
   * @return A free chunk, if one is available; null otherwise.
   */
  private PutChunk getFreeChunk() {
    PutChunk chunkToReturn = null;
    for (PutChunk chunk : putChunks) {
      if (chunk.isFree()) {
        chunkToReturn = chunk;
        break;
      }
    }
    if (chunkToReturn == null && putChunks.size() < routerConfig.routerMaxInMemPutChunks) {
      chunkToReturn = new PutChunk();
      putChunks.add(chunkToReturn);
    }
    return chunkToReturn;
  }

  /**
   * Get the PutChunk that is in specified state. Note that if there are multiple chunks in the specified state, this
   * method will return the first one encountered.
   * @return the PutChunk that is in the specified state; null if no PutChunk is in the specified state.
   */
  private PutChunk getChunkInState(ChunkState state) {
    PutChunk chunkToReturn = null;
    for (PutChunk chunk : putChunks) {
      if (chunk.state == state) {
        chunkToReturn = chunk;
        break;
      }
    }
    return chunkToReturn;
  }

  /**
   * Return the number of data chunks that this operation should result in. For composite direct uploads, this method
   * calculates the expected number of chunks using the blob size and the configured max put chunk size. For stitched
   * uploads, this returns the number of chunks provided in the {@code chunksToStitch} list. This method should only be
   * called once the chunk filling has completed (which is when the final size is determined).
   * @return the number of data chunks that this operation resulted in.
   * @throws IllegalStateException if the chunk filling has not yet completed.
   */
  int getNumDataChunks() {
    return isStitchOperation() ? chunksToStitch.size()
        : RouterUtils.getNumChunksForBlobAndChunkSize(getBlobSize(), routerConfig.routerMaxPutChunkSizeBytes);
  }

  /**
   * @return the size of the blob in this operation. This method should only be called once the chunk filling has
   * completed (which is when the final size is determined).
   * @throws IllegalStateException if the chunk filling has not yet completed.
   */
  long getBlobSize() {
    if (!chunkFillingCompletedSuccessfully) {
      throw new IllegalStateException("Request for blob size before chunk fill completion");
    }
    return blobSize;
  }

  /**
   * Return the blobId string associated with this operation if it was successful, or null if it failed. This should
   * (obviously) only be called once the operation is complete.
   * @return the blobId if the operation is successful; null otherwise.
   */
  String getBlobIdString() {
    // we call getBlobIdString to set the return result of FutureResult<String>
    // when skipCompositeChunk is true, we don't generate composite blob, we return {@link PutBlobMetaInfo}
    if (options.skipCompositeChunk()) {
      return putBlobMetaInfo;
    } else {
      return blobId == null ? null : blobId.getID();
    }
  }

  /**
   * Return the {@link BlobProperties} associated with this operation.
   * @return the {@link BlobProperties} associated with this operation.
   */
  BlobProperties getBlobProperties() {
    if (finalBlobProperties == null) {
      throw new IllegalStateException("blob properties has not yet been finalized");
    }
    return finalBlobProperties;
  }

  /**
   * Return the userMetadata associated with this operation.
   * @return the userMetadata associated with this operation.
   */
  byte[] getUserMetadata() {
    return userMetadata;
  }

  /**
   * Return the {@link Callback} associated with this operation.
   * @return the {@link Callback} associated with this operation.
   */
  Callback<String> getCallback() {
    return callback;
  }

  /**
   * Return the {@link FutureResult} associated with this operation.
   * @return the {@link FutureResult} associated with this operation.
   */
  FutureResult<String> getFuture() {
    return futureResult;
  }

  /**
   * The exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * The time at which this operation was submitted.
   * @return the time at which the operation was submitted.
   */
  long getSubmissionTimeMs() {
    return submissionTimeMs;
  }

  /**
   * @return the service ID for this put operation.
   */
  String getServiceId() {
    return passedInBlobProperties.getServiceId();
  }

  /**
   * This will return a view of the correlation IDs of requests that may still be in flight. This is not threadsafe
   * and should only be called from the main event loop.
   * @return the set of correlation IDs of requests that are still in flight.
   */
  Set<Integer> getInFlightCorrelationIds() {
    return Collections.unmodifiableSet(correlationIdToPutChunk.keySet());
  }

  /**
   * If this is a composite object, fill the list with chunk IDs successfully created by this operation.
   * This does not include IDs being stitched together by a stitchBlob call.
   * @return the list of successfully put chunk ids if this is a composite object and not a stitch operation, empty list
   *         otherwise.
   */
  List<StoreKey> getSuccessfullyPutChunkIdsIfCompositeDirectUpload() {
    boolean compositeDirectUpload = !isStitchOperation() && isComposite();
    return compositeDirectUpload ? metadataPutChunk.getSuccessfullyPutChunkIds() : Collections.emptyList();
  }

  /**
   * @return {@code true} if this is a composite blob upload, or if the operation failed. Stitch operations count as
   *         composite uploads since they upload a metadata chunk.
   */
  private boolean isComposite() {
    // If the overall operation failed, we treat the successfully put chunks as part of a composite blob.
    boolean operationFailed = blobId == null || getOperationException() != null;
    return operationFailed || metadataPutChunk.indexToChunkIdsAndChunkSizes.size() > 1 || isStitchOperation();
  }

  /**
   * @return {@code true} if this is a blob stitching operation instead of a standard upload.
   */
  boolean isStitchOperation() {
    return chunksToStitch != null;
  }

  /**
   * Set the exception associated with this operation.
   * First, if current operationException is null, directly set operationException as exception;
   * Second, if operationException exists, compare ErrorCodes of exception and existing operation Exception depending
   * on precedence level. An ErrorCode with a smaller precedence level overrides an ErrorCode with a larger precedence
   * level. Update the operationException if necessary.
   * @param exception the {@link RouterException} to possibly set.
   */
  void setOperationExceptionAndComplete(Exception exception) {
    if (exception instanceof RouterException) {
      RouterUtils.replaceOperationException(operationException, (RouterException) exception, this::getPrecedenceLevel);
    } else {
      operationException.set(exception);
    }
    setOperationCompleted();
  }

  /**
   * @return MetadataPutChunk associated with this operation.
   */
  MetadataPutChunk getMetadataPutChunk() {
    return metadataPutChunk;
  }

  /**
   * Gets the precedence level for a {@link RouterErrorCode}. A precedence level is a relative priority assigned
   * to a {@link RouterErrorCode}. If a {@link RouterErrorCode} has not been assigned a precedence level, a
   * {@code Integer.MIN_VALUE} will be returned.
   * @param routerErrorCode The {@link RouterErrorCode} for which to get its precedence level.
   * @return The precedence level of the {@link RouterErrorCode}.
   */
  private int getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case InsufficientCapacity:
        return 1;
      case TooManyRequests:
        return 2;
      case AmbryUnavailable:
        return 3;
      case UnexpectedInternalError:
        return 4;
      case OperationTimedOut:
        return 5;
      default:
        return Integer.MIN_VALUE;
    }
  }

  /**
   * PutChunk is responsible for storing chunks to be put, managing their state and completing the operation on the
   * chunks. A PutChunk object is not really associated with one single chunk of data. Instead, it acts a holder that
   * handles a chunk of data and takes it to completion, and once done, moves on to handle more chunks of data. This
   * why there is a reference to the "current chunk" in the comments.
   */
  class PutChunk {
    // the position of the current chunk in the overall blob.
    private int chunkIndex;
    // the blobId of the current chunk.
    protected BlobId chunkBlobId;
    // the size of raw chunk (prior encryption if applicable)
    protected long chunkBlobSize;
    // the BlobProperties to associate with this chunk.
    private BlobProperties chunkBlobProperties;
    // the userMetadata associated with the blob
    protected byte[] chunkUserMetadata;
    // the most recent time at which this chunk became Free.
    private long chunkFreeAtMs;
    // the time at which the chunk filling was complete
    private long chunkFillCompleteAtMs;
    // the most recent time at which this chunk became ready to be encrypted.
    private long chunkEncryptReadyAtMs;
    // the most recent time at which this chunk became ready.
    private long chunkReadyAtMs;
    // The exception encountered while putting the current chunk. Not all errors are irrecoverable. An error may or
    // may not get overridden by a subsequent error, and this variable is meant to store the most relevant error.
    private RouterException chunkException;
    // the state of the current chunk.
    protected volatile ChunkState state;
    // the ByteBuffer that has the data for the current chunk.
    protected volatile ByteBuf buf;
    // the ByteBuffer that has the encryptedPerBlobKey (encrypted using containerKey). Could be null if encryption is not required.
    protected ByteBuffer encryptedPerBlobKey;
    // the OperationTracker used to track the status of requests for the current chunk.
    protected OperationTracker operationTracker;
    // the number of times a put was attempted for the current chunk.
    private int failedAttempts;
    // the partitionId chosen for the current chunk.
    private PartitionId partitionId;
    // metrics tracker to track encrypt jobs
    private final CryptoJobMetricsTracker encryptJobMetricsTracker =
        new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics);
    // the list of partitions already attempted for this chunk.
    private List<PartitionId> attemptedPartitionIds = new ArrayList<PartitionId>();
    // map of correlation id to the request metadata for every request issued for the current chunk.
    private final Map<Integer, RequestInfo> correlationIdToChunkPutRequestInfo = new LinkedHashMap<>();
    // list of buffers that were once associated with this chunk and are not yet freed.
    private final Logger logger = LoggerFactory.getLogger(PutChunk.class);
    // Tracks quota charging for this chunk.
    private OperationQuotaCharger operationQuotaCharger;

    // Whether this chunk is compressed.  Default is false for not compressed.
    // This value is set after compression has completed.  It is used to create PutRequest.
    private boolean isChunkCompressed;
    private final Crc32 chunkCrc32 = new Crc32();
    private boolean isCrcVerified = false;

    /**
     * Construct a PutChunk
     */
    public PutChunk() {
      clear();
    }

    /**
     * Clear the state to make way for a new data chunk
     */
    void clear() {
      releaseBlobContent();
      chunkIndex = -1;
      chunkBlobSize = -1;
      chunkBlobId = null;
      chunkException = null;
      failedAttempts = 0;
      partitionId = null;
      attemptedPartitionIds.clear();
      correlationIdToChunkPutRequestInfo.clear();
      chunkUserMetadata = userMetadata;
      encryptedPerBlobKey = null;
      chunkFreeAtMs = time.milliseconds();
      // this assignment should be the last statement as this immediately makes this chunk available to the
      // ChunkFiller thread for filling.
      state = ChunkState.Free;
      operationQuotaCharger =
          new OperationQuotaCharger(quotaChargeCallback, PutOperation.class.getSimpleName(), routerMetrics);
      isChunkCompressed = false;
    }

    /**
     * This method might be called in main thread, encryption job thread or chunk filler thread, so it has to be protected.
     */
    synchronized void releaseBlobContent() {
      if (buf != null) {
        logger.trace("{}: releasing the chunk data for chunk {}", loggingContext, chunkIndex);
        ReferenceCountUtil.safeRelease(buf);
        buf = null;
        chunkCrc32.reset();
        isCrcVerified = false;
      }
    }

    /**
     * True is the data in this chunk is released.
     * @return
     */
    synchronized boolean isDataReleased() {
      return buf == null;
    }

    /**
     * @return {@code true} if chunk is a MetadataChunk. {@code false} otherwise. Since this is a regular chunk,
     * false is returned
     */
    boolean isMetadataChunk() {
      return false;
    }

    /**
     * @return the position of the current chunk in the overall blob.
     */
    int getChunkIndex() {
      return chunkIndex;
    }

    /**
     * @return the state of the PutChunk.
     */
    ChunkState getState() {
      return state;
    }

    /**
     * @return the {@link BlobId} of the current chunk - valid only after the chunk is successfully put.
     */
    BlobId getChunkBlobId() {
      return chunkBlobId;
    }

    /**
     * @return the {@link RouterException}, if any, encountered for the current chunk.
     */
    RouterException getChunkException() {
      return chunkException;
    }

    /**
     * @return true if this PutChunk is free so a chunk of the overall blob can be filled in.
     */
    boolean isFree() {
      return state == ChunkState.Free;
    }

    /**
     * @return true if this PutChunk is being built/filled with a chunk of the overall blob.
     */
    boolean isBuilding() {
      return state == ChunkState.Building;
    }

    /**
     * @return true if this PutChunk is ready with a chunk of the overall blob.
     */
    boolean isReady() {
      return state == ChunkState.Ready;
    }

    /**
     * @return true if the operation on the current chunk is complete.
     */
    boolean isComplete() {
      return state == ChunkState.Complete;
    }

    /**
     * @return true if the operation on the current chunk is encrypting.
     */
    boolean isEncrypting() {
      return state == ChunkState.Encrypting;
    }

    /**
     * @return operation tracker used by current put chunk
     */
    OperationTracker getOperationTrackerInUse() {
      return operationTracker;
    }

    /**
     * @return the partition id of the current chunk.
     */
    PartitionId getPartitionId() {
      return partitionId;
    }

    /**
     * Prepare this chunk for building, that is, for being filled with data from the channel.
     * @param chunkIndex the position in the overall blob that this chunk is going to  be in.
     */
    private void prepareForBuilding(int chunkIndex) {
      this.chunkIndex = chunkIndex;
      state = ChunkState.Building;
    }

    /**
     * Prepare this chunk (that is completely built), for puts.
     */
    private void prepareForSending() {
      try {
        BlobDataType blobDataType =
            isMetadataChunk() ? BlobDataType.METADATA : isSimpleBlob ? BlobDataType.SIMPLE : BlobDataType.DATACHUNK;

        int failedAttempt = 0;
        while (failedAttempt <= routerConfig.routerMaxSlippedPutAttempts) {
          if (shouldUseReservedMetadataId()) {
            partitionId = metadataPutChunk.reservedMetadataChunkId.getPartition();
            chunkBlobId = metadataPutChunk.reservedMetadataChunkId;
          } else {
            partitionId = getPartitionForPut(partitionClass, attemptedPartitionIds);
            chunkBlobId =
                new BlobId(routerConfig.routerBlobidCurrentVersion, BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
                    passedInBlobProperties.getAccountId(), passedInBlobProperties.getContainerId(), partitionId,
                    passedInBlobProperties.isEncrypted(), blobDataType);
          }

          // To ensure previously attempted partitions are not retried for this PUT after a failure.
          attemptedPartitionIds.add(partitionId);

          chunkBlobProperties = new BlobProperties(chunkBlobSize, passedInBlobProperties.getServiceId(),
              passedInBlobProperties.getOwnerId(), passedInBlobProperties.getContentType(),
              passedInBlobProperties.isPrivate(), passedInBlobProperties.getTimeToLiveInSeconds(),
              passedInBlobProperties.getCreationTimeInMs(), passedInBlobProperties.getAccountId(),
              passedInBlobProperties.getContainerId(), passedInBlobProperties.isEncrypted(),
              passedInBlobProperties.getExternalAssetTag(), passedInBlobProperties.getContentEncoding(),
              passedInBlobProperties.getFilename(), resolveReservedMetadataId());

            try {
              // Attempt to get the operation tracker
              operationTracker = getOperationTracker();
              break;
            } catch (Exception e) {
              failedAttempt++;
            }
        }

        if (failedAttempt > routerConfig.routerMaxSlippedPutAttempts) {
          throw new RouterException("Prepare for sending failed", RouterErrorCode.UnexpectedInternalError);
        }
        correlationIdToChunkPutRequestInfo.clear();
        logger.trace("{}: Chunk {} is ready for sending out to server", loggingContext, chunkIndex);
        state = ChunkState.Ready;
      } catch (RouterException e) {
        setOperationExceptionAndComplete(e);
      } catch (Exception e) {
        setOperationExceptionAndComplete(
            new RouterException("Prepare for sending failed", e, RouterErrorCode.UnexpectedInternalError));
      }
    }

    /**
     * Resolve the reserved metadata id to be used for data chunks.
     * For simple blobs or metadata chunks this will be {@code null}. For the data chunks of composite blobs, the reserved
     * metadata id of the metadata chunk will be used.
     * @return the blob id to use as the reserved metadata id. Can be {@code null}.
     */
    private String resolveReservedMetadataId() {
      if (!routerConfig.routerReservedMetadataEnabled || isSimpleBlob || isMetadataChunk()) {
        return null;
      }
      // S3 doesn't use reserved metadata chunk.
      // 1. For multipart upload, S3 doesn't use signed url. And for the data chunk, we don't reserve metadata chunk.
      //    Customer may upload part by part for days.
      // 2. For regular update, it's not chunked upload. It's uploaded with single PutObject.
      //    Don't reserve metadata chunk either. We manage the S3 life cycle right away.
      // 3. For multipart Complete request, it uses stitch command. Avoid the metadata chunk exceptions.
      if (RestUtils.isS3Request(restRequest)) {
        return null;
      }
      return metadataPutChunk.reservedMetadataChunkId.getID();
    }

    /**
     * Gets an {@link OperationTracker} based on the config;
     * @return an {@link OperationTracker} based on the config;
     */
    protected OperationTracker getOperationTracker() {
      OperationTracker operationTracker;
      String trackerType = routerConfig.routerPutOperationTrackerType;
      String originatingDcName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());

      Pair<Account, Container> accountContainer = RouterUtils.getAccountContainer(accountService,
          passedInBlobProperties.getAccountId(),
          passedInBlobProperties.getContainerId());

      if(routerConfig.routerParanoidDurabilityEnabled && accountContainer.getSecond().isParanoidDurabilityEnabled()) {
        operationTracker = new ParanoidDurabilityOperationTracker(routerConfig, partitionId, originatingDcName, routerMetrics);
      } else if (trackerType.equals(SimpleOperationTracker.class.getSimpleName())) {
        operationTracker =
            new SimpleOperationTracker(routerConfig, RouterOperation.PutOperation, partitionId, originatingDcName, true,
                routerMetrics);
      } else if (trackerType.equals(AdaptiveOperationTracker.class.getSimpleName())) {
        operationTracker =
            new AdaptiveOperationTracker(routerConfig, routerMetrics, RouterOperation.PutOperation, partitionId,
                originatingDcName, time);
      } else {
        throw new IllegalArgumentException("Unrecognized tracker type: " + trackerType);
      }
      return operationTracker;
    }

    /**
     * The callback method to handle the result and exception from an encryption job.
     * @param result The result of Encryption job.
     * @param exception The exception of Encryption job.
     */
    private void encryptionCallback(EncryptJob.EncryptJobResult result, Exception exception) {
      logger.trace("{}: Processing encrypt job callback for chunk at index {}", loggingContext, chunkIndex);

      if (exception == null && !verifyCRC()) {
        // Original content is corruption. Complete the operation
        logger.error("CRC of the chunk {} is different before and after encryption", chunkBlobId);
        releaseBlobContent();
        if (result != null) {
          result.release();
        }
        setOperationExceptionAndComplete(
            new RouterException("CRC of chunk {} is different before and after encryption" + chunkBlobId, exception,
                RouterErrorCode.BlobCorrupted));
        routerMetrics.encryptTimeMs.update(time.milliseconds() - chunkEncryptReadyAtMs);
        routerCallback.onPollReady();
        return;
      }

      if (!isMetadataChunk()) {
        // If this is a data blob, then release the content with or without exception.
        // When there is no exception, then the encrypted data will be used.
        // When there is an exception, then PutOperation will be terminated, and there is no use for this blob anymore.
        releaseBlobContent();
      }
      encryptJobMetricsTracker.onJobResultProcessingStart();
      if (exception == null && !isOperationComplete()) {
        if (!isMetadataChunk()) {
          buf = result.getEncryptedBlobContent();
          if (routerConfig.routerVerifyCrcForPutRequests) {
            for (ByteBuffer byteBuffer : buf.nioBuffers()) {
              chunkCrc32.update(byteBuffer);
            }
          }
        }
        encryptedPerBlobKey = result.getEncryptedKey();
        chunkUserMetadata = result.getEncryptedUserMetadata().array();
        logger.trace("{}: Completing encrypt job result for chunk at index {}", loggingContext, chunkIndex);
        prepareForSending();
        chunkReadyAtMs = time.milliseconds();
      } else {
        encryptJobMetricsTracker.incrementOperationError();
        if (!isOperationComplete()) {
          logger.trace("{}: Setting exception from encrypt of chunk at index {} ", loggingContext, chunkIndex,
              exception);
          // If we are here, then the result is null. no need to release it.
          setOperationExceptionAndComplete(
              new RouterException("Exception thrown on encrypting the content for chunk at index " + chunkIndex,
                  exception, RouterErrorCode.UnexpectedInternalError));
        } else {
          logger.trace(
              "{}: Ignoring exception from encrypt job for chunk at index {} as operation exception {} is set already",
              loggingContext, chunkIndex, getOperationException(), exception);
          // If we are here, then the operation is completed and the exception could be null, in this case,
          // we have to release the content in the result.
          if (result != null) {
            result.release();
          }
        }
      }
      routerMetrics.encryptTimeMs.update(time.milliseconds() - chunkEncryptReadyAtMs);
      encryptJobMetricsTracker.onJobResultProcessingComplete();
      routerCallback.onPollReady();
      // double check if the operation is not completed. If so, we have to release the buf here, since in
      // main thread, chunk might already be released.
      if (isOperationComplete()) {
        logger.info("{}: Release blob content for put chunk in encryption callback since operation is completed",
            loggingContext);
        releaseBlobContent();
      }
    }

    /**
     * Compress the buffer in "buf" field for data chunks.  It does not compress metadata chunks.
     * It also applies rules specified in config.
     * After compression, it replaces "buf" field and update isCompressed field in blob properties.
     * @param outputDirectMemory Whether the compression output should be direct memory (True) or heap memory (false).
     *                           If the next step is encryption, it should use Heap (since encryption takes an array).
     *                           If the next step is output to Netty, it should use direct memory.
     */
    private void compressChunk(boolean outputDirectMemory) {
      // Do not compress metadata chunk or already compressed chunk.
      if (isMetadataChunk() || isChunkCompressed || !isBlobCompressible) {
        return;
      }

      // Note: compress() returns null if it failed.
      boolean isFullChunk = (buf.readableBytes() == routerConfig.routerMaxPutChunkSizeBytes);
      ByteBuf newBuffer = compressionService.compressChunk(buf, isFullChunk, outputDirectMemory);
      if (newBuffer != null) {
        buf.release();
        buf = newBuffer;
        isChunkCompressed = true;
      }
    }

    /**
     * Submits encrypt job for the given {@link PutChunk} and processes the callback for the same
     */
    private void encryptChunk() {
      try {
        logger.trace("{}: Chunk at index {} moves to {} state", loggingContext, chunkIndex, ChunkState.Encrypting);
        state = ChunkState.Encrypting;
        chunkEncryptReadyAtMs = time.milliseconds();
        encryptJobMetricsTracker.onJobSubmission();
        logger.trace("{}: Submitting encrypt job for chunk at index {}", loggingContext, chunkIndex);
        cryptoJobHandler.submitJob(
            new EncryptJob(passedInBlobProperties.getAccountId(), passedInBlobProperties.getContainerId(),
                isMetadataChunk() ? null : buf.retainedDuplicate(), ByteBuffer.wrap(chunkUserMetadata),
                kms.getRandomKey(), cryptoService, kms, options, encryptJobMetricsTracker, this::encryptionCallback));
      } catch (GeneralSecurityException e) {
        encryptJobMetricsTracker.incrementOperationError();
        logger.trace("{}: Exception thrown while generating random key for chunk at index {}", loggingContext,
            chunkIndex, e);
        setOperationExceptionAndComplete(new RouterException(
            "GeneralSecurityException thrown while generating random key for chunk at index " + chunkIndex, e,
            RouterErrorCode.UnexpectedInternalError));
      }
    }

    /**
     * Do the actions required when the chunk has been completely built.
     * @param updateMetric whether chunk fill completion metrics should be updated.
     */
    void onFillComplete(boolean updateMetric) {
      chunkBlobSize = buf.readableBytes();
      chunkFillCompleteAtMs = time.milliseconds();
      if (updateMetric) {
        routerMetrics.chunkFillTimeMs.update(time.milliseconds() - chunkFreeAtMs);
      }

      // Attempt to apply compression on data chunk.
      // If encryption is next, use Heap to store compression output since encryption takes heap buffer.
      // If no encryption, use direct memory to store compression output since it sends to Netty.
      compressChunk(!passedInBlobProperties.isEncrypted());

      if (!passedInBlobProperties.isEncrypted()) {
        prepareForSending();
        chunkReadyAtMs = time.milliseconds();
      } else {
        encryptChunk();
      }
    }

    /**
     * Fill the buffer of the current chunk with the data from the given {@link ByteBuf}.
     * @param channelReadBuf the {@link ByteBuf} from which to read data.
     * @return the number of bytes transferred in this operation.
     */
    int fillFrom(ByteBuf channelReadBuf) {
      int toWrite;
      ByteBuf slice;
      if (buf == null) {
        // If current buf is null, then only read the up to routerMaxPutChunkSizeBytes.
        toWrite = Math.min(channelReadBuf.readableBytes(), routerConfig.routerMaxPutChunkSizeBytes);
        slice = channelReadBuf.readRetainedSlice(toWrite);
        buf = slice;
        buf.touch(loggingContext);
      } else {
        int remainingSize = routerConfig.routerMaxPutChunkSizeBytes - buf.readableBytes();
        toWrite = Math.min(channelReadBuf.readableBytes(), remainingSize);
        slice = channelReadBuf.readRetainedSlice(toWrite);
        slice.touch(loggingContext);
        // buf already has some bytes
        if (buf instanceof CompositeByteBuf) {
          // Buf is already a CompositeByteBuf, then just add the slice from
          ((CompositeByteBuf) buf).addComponent(true, slice);
        } else {
          int maxComponents = routerConfig.routerMaxPutChunkSizeBytes;
          CompositeByteBuf composite = buf.isDirect() ? buf.alloc().compositeDirectBuffer(maxComponents)
              : buf.alloc().compositeHeapBuffer(maxComponents);
          composite.addComponents(true, buf, slice);
          buf = composite;
          buf.touch(loggingContext);
        }
      }

      // Update crc for the chunk data
      if (routerConfig.routerVerifyCrcForPutRequests) {
        chunkCrc32.update(slice.nioBuffer());
      }

      if (buf.readableBytes() == routerConfig.routerMaxPutChunkSizeBytes) {
        if (chunkIndex == 0) {
          // If first put chunk is full, but not yet prepared then mark it awaiting resolution instead of completing it.
          // This chunk will be prepared as soon as either more bytes are read from the channel, or the chunk filling
          // is complete. At that point we will have enough information to mark this blob as simple or composite blob.
          state = ChunkState.AwaitingBlobTypeResolution;
        } else {
          onFillComplete(true);
        }
      }
      return toWrite;
    }

    /**
     * Check if the operation on the chunk is eligible for completion, if so complete it.
     */
    void checkAndMaybeComplete() {
      boolean done = false;
      // Now, check if this chunk is done.
      if (chunkException != null && chunkException.getErrorCode() == RouterErrorCode.BlobCorrupted) {
        logger.error("{} : Put chunk {} failed due to corruption. Failing entire operation", loggingContext,
            getChunkBlobId());
        // Append this blob to slipped put so that it is cleaned up later. If this is a composite blob, the rest of the
        // chunks will be cleaned up automatically as part of PUT operation clean up in PutManager#onComplete()
        appendSlippedPutBlobId(chunkBlobId);
        setOperationExceptionAndComplete(chunkException);
        return;
      }

      if (operationTracker.isDone() || (chunkException != null
          && chunkException.getErrorCode() == RouterErrorCode.TooManyRequests)) {
        if (!operationTracker.hasSucceeded()) {
          failedAttempts++;
          appendSlippedPutBlobId(chunkBlobId);
          if (failedAttempts <= routerConfig.routerMaxSlippedPutAttempts && !shouldUseReservedMetadataId()) {
            logger.trace("{}: Attempt to put chunk with id: {} failed, attempting slipped put ", loggingContext,
                chunkBlobId);
            routerMetrics.slippedPutAttemptCount.inc();
            if (this instanceof MetadataPutChunk) {
              routerMetrics.metadataSlippedPutAttemptCount.inc();
            }
            chunkException = null;
            prepareForSending();
          } else {
            // this chunk could not be successfully put. The whole operation has to fail.
            logger.error("{}: Slipped puts failed for {}", loggingContext, getChunkBlobId());
            chunkBlobId = null;
            setOperationExceptionAndComplete(chunkException);
            done = true;
          }
        } else {
          chunkException = null;
          done = true;
        }
      }
      if (done) {
        // the chunk is complete now. We can charge against quota for the chunk if its not a metadata chunk.
        if (QuotaUtils.postProcessCharge(quotaChargeCallback) && !(this instanceof MetadataPutChunk)
            && chunkException == null) {
          try {
            quotaChargeCallback.checkAndCharge(false, true, chunkBlobProperties.getBlobSize());
          } catch (QuotaException quotaException) {
            // For now we only log for quota charge exceptions for in progress requests.
            logger.info("{}: Exception {} while handling quota charge event", loggingContext,
                quotaException.toString());
          }
        }
        if (this instanceof MetadataPutChunk && chunkException == null) {
          // if this is a metadata chunk, and it was successful then increment the count of metadata chunk creation.
          routerMetrics.metadataChunkCreationCount.inc();
        }
        state = ChunkState.Complete;
      }
    }

    /**
     * @return {@code true} If this chunk is a metadata chunk and router reserved metadata is enabled. {@code false}
     * otherwise.
     */
    boolean shouldUseReservedMetadataId() {
      return isMetadataChunk() && routerConfig.routerReservedMetadataEnabled && !RestUtils.isS3Request(restRequest);
    }

    /**
     * This is one of two main entry points to this class, the other being
     * {@link #handleResponse(ResponseInfo, PutResponse)}.
     * Apart from fetching requests to send out, this also checks for timeouts of issued requests,
     * status of the operation and anything else that needs to be done within this PutChunk. The callers guarantee
     * that this method is called on all the PutChunks of an operation until either the operation,
     * or the chunk operation is completed.
     * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
     *                                    created as part of this poll operation.
     */
    void poll(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      cleanupExpiredInFlightRequests(requestRegistrationCallback);
      checkAndMaybeComplete();
      if (!isComplete()) {
        fetchRequests(requestRegistrationCallback);
      }
    }

    /**
     * Clean up requests sent out by this operation that have now timed out.
     * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
     */
    private void cleanupExpiredInFlightRequests(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      Iterator<Map.Entry<Integer, RequestInfo>> inFlightRequestsIterator =
          correlationIdToChunkPutRequestInfo.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, RequestInfo> entry = inFlightRequestsIterator.next();
        int correlationId = entry.getKey();
        RequestInfo requestInfo = entry.getValue();
        // If request times out due to no response from server or due to being stuck in router itself (due to bandwidth
        // throttling, etc) for long time, drop the request.
        long currentTimeInMs = time.milliseconds();
        RouterUtils.RouterRequestExpiryReason routerRequestExpiryReason =
            RouterUtils.isRequestExpired(requestInfo, currentTimeInMs);
        if (routerRequestExpiryReason != RouterUtils.RouterRequestExpiryReason.NO_TIMEOUT) {
          onErrorResponse(requestInfo.getReplicaId(), TrackedRequestFinalState.FAILURE);
          logger.warn("{}: PutRequest with correlationId {} in flight has expired for replica {} {} due to {} ",
              loggingContext, correlationId, requestInfo.getReplicaId().getDataNodeId(), requestInfo,
              routerRequestExpiryReason.name());
          // Do not notify this as a failure to the response handler, as this timeout could simply be due to
          // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
          // response and the response handler will be notified accordingly.
          setChunkException(RouterUtils.buildTimeoutException(correlationId, requestInfo.getReplicaId().getDataNodeId(),
              chunkBlobId));
          requestRegistrationCallback.registerRequestToDrop(correlationId);
          inFlightRequestsIterator.remove();
          RouterUtils.logTimeoutMetrics(routerRequestExpiryReason, routerMetrics, requestInfo);
        } else {
          // Note: Even though the requests are ordered by correlation id and their creation time, we cannot break out of
          // the while loop here. This is because time outs for all requests may not be equal now.

          // For example, request 1 in the map may have been assigned high time out since it might be sent at a
          // time when the load is high and request 2 may have been assigned lower time out value since the load might have
          // decreased by the time it is sent out. In this case, we should continue iterating the loop and clean up
          // request 2 in the map.

          // The cost of iterating all entries should be okay since the map contains outstanding requests whose number
          // should be small. The maximum outstanding requests possible would be equal to the operation parallelism value
          // and may be few more if adaptive operation tracker is used.
        }
      }
    }

    /**
     * Fetch {@link PutRequest}s to send for the current data chunk.
     */
    private void fetchRequests(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
      while (replicaIterator.hasNext()) {
        ReplicaId replicaId = replicaIterator.next();
        String hostname = replicaId.getDataNodeId().getHostname();
        Port port = RouterUtils.getPortToConnectTo(replicaId, routerConfig.routerEnableHttp2NetworkClient);
        PutRequest putRequest = createPutRequest();
        RequestInfo requestInfo =
            new RequestInfo(hostname, port, putRequest, replicaId, prepareQuotaCharger(), time.milliseconds(),
                routerConfig.routerRequestNetworkTimeoutMs, routerConfig.routerRequestTimeoutMs);
        int correlationId = putRequest.getCorrelationId();
        correlationIdToChunkPutRequestInfo.put(correlationId, requestInfo);
        correlationIdToPutChunk.put(correlationId, this);
        requestRegistrationCallback.registerRequestToSend(PutOperation.this, requestInfo);
        replicaIterator.remove();
        if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
          logger.debug("{}: Making request with correlationId {} to a remote replica {} in {}", loggingContext,
              correlationId, replicaId.getDataNodeId(), replicaId.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoRequestCount.inc();
        } else {
          logger.trace("{}: Making request with correlationId {} to a local replica {}", loggingContext, correlationId,
              replicaId.getDataNodeId());
        }
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).putRequestRate.mark();
        routerMetrics.routerPutRequestRate.mark();
      }
    }

    /**
     * Create and return the {@link PutRequest} associated with the current chunk. This method should only be called
     * when the chunk is in ready state.
     * @return the crated {@link PutRequest}.
     */
    protected PutRequest createPutRequest() {
      return new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
          chunkBlobId, chunkBlobProperties, ByteBuffer.wrap(chunkUserMetadata), buf.retainedDuplicate(),
          buf.readableBytes(), BlobType.DataBlob, encryptedPerBlobKey != null ? encryptedPerBlobKey.duplicate() : null,
          isChunkCompressed);
    }

    /**
     * Choose a random {@link PartitionId} for putting the current chunk and return it.
     * @param partitionClass the partition class to choose partitions from.
     * @param partitionIdsToExclude the list of {@link PartitionId}s that should be excluded from consideration.
     * @return the chosen {@link PartitionId}
     * @throws RouterException
     */
    protected PartitionId getPartitionForPut(String partitionClass, List<PartitionId> partitionIdsToExclude)
        throws RouterException {
      PartitionId selected = clusterMap.getRandomWritablePartition(partitionClass, partitionIdsToExclude);
      if (selected == null) {
        throw new RouterException("No writable partitions of class '" + partitionClass + "' or default available.",
            RouterErrorCode.AmbryUnavailable);
      }
      if (!partitionClass.equals(selected.getPartitionClass())) {
        logger.warn(
            "{}: No partitions for partitionClass='{}' found, partitionClass='{}' used instead. blobProperties={}",
            loggingContext, partitionClass, selected.getPartitionClass(), passedInBlobProperties);
        routerMetrics.unknownPartitionClassCount.inc();
      }
      return selected;
    }

    /**
     * This method is the entry point for handling responses received for requests sent out on behalf of this chunk.
     * For puts, processing involves determining whether the request was successful, notifying the operation
     * tracker so it can track the status of the operation, and notifying the response handler for failure detection.
     * Finally, a check is done to determine whether the operation on the chunk is eligible for completion,
     * if so the operation is completed right away.
     * @param responseInfo the response received for a request sent out on behalf of this chunk.
     * @param putResponse the {@link PutResponse} associated with this response.
     */
    void handleResponse(ResponseInfo responseInfo, PutResponse putResponse) {
      int correlationId = responseInfo.getRequestInfo().getRequest().getCorrelationId();
      RequestInfo requestInfo = correlationIdToChunkPutRequestInfo.remove(correlationId);
      if (requestInfo == null) {
        // Ignore right away. This could mean:
        // - the response is valid for this chunk, but was timed out and removed from the map.
        // - the response is for an earlier attempt of this chunk (slipped put scenario). And the map was cleared
        // before attempting the slipped put.
        // - the response is for an earlier chunk held by this PutChunk.
        logger.debug("{}: No matching request found for {}", loggingContext, correlationId);
        return;
      }
      if (responseInfo.isQuotaRejected()) {
        processQuotaRejectedResponse(correlationId, requestInfo.getReplicaId());
        return;
      }
      // Track the over all time taken for the response since the creation of the request.
      long requestLatencyMs = time.milliseconds() - requestInfo.getRequestCreateTime();
      routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
      routerMetrics.getDataNodeBasedMetrics(requestInfo.getReplicaId().getDataNodeId()).putRequestLatencyMs.update(
          requestLatencyMs);
      boolean isSuccessful;
      TrackedRequestFinalState putRequestFinalState = null;
      if (responseInfo.getError() != null) {
        logger.debug("{}: PutRequest with response correlationId {} timed out for replica {} ", loggingContext,
            correlationId, requestInfo.getReplicaId().getDataNodeId());
        setChunkException(new RouterException(
            "Operation timed out because of " + responseInfo.getError() + " at DataNode " + responseInfo.getDataNode(),
            RouterErrorCode.OperationTimedOut));
        isSuccessful = false;
        putRequestFinalState = TrackedRequestFinalState.FAILURE;
      } else {
        if (putResponse == null) {
          logger.debug(
              "{}: PutRequest with response correlationId {} received an unexpected error on response deserialization from replica {} ",
              loggingContext, correlationId, requestInfo.getReplicaId().getDataNodeId());
          setChunkException(new RouterException("Response deserialization received an unexpected error",
              RouterErrorCode.UnexpectedInternalError));
          isSuccessful = false;
          putRequestFinalState = TrackedRequestFinalState.FAILURE;
        } else {
          if (putResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            routerMetrics.unknownReplicaResponseError.inc();
            logger.error(
                "{}: The correlation id in the PutResponse {} is not the same as the correlation id in the associated PutRequest: {}",
                loggingContext, putResponse.getCorrelationId(), correlationId);
            setChunkException(
                new RouterException("Unexpected internal error", RouterErrorCode.UnexpectedInternalError));
            isSuccessful = false;
            putRequestFinalState = TrackedRequestFinalState.FAILURE;
            // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
          } else {
            ServerErrorCode putError = putResponse.getError();
            if (putError == ServerErrorCode.NoError) {
              if (!verifyCRC()) {
                logger.error("{}: PutRequest with response correlationId {}, blob id {} has a mismatch in crc {} ",
                    loggingContext, correlationId, chunkBlobId, requestInfo.getReplicaId().getDataNodeId());
                setChunkException(
                    new RouterException("CRC mismatch of chunk content before and after writing to server",
                        RouterErrorCode.BlobCorrupted));
                isSuccessful = false;
                putRequestFinalState = TrackedRequestFinalState.FAILURE;
              } else {
                logger.trace("{}: The putRequest was successful for chunk {}", loggingContext, chunkIndex);
                isSuccessful = true;
              }
            } else {
              // chunkException will be set within processServerError.
              logger.trace(
                  "{}: Replica {} returned an error {} for a PutRequest with response correlationId : {} and blobId {}",
                  loggingContext, requestInfo.getReplicaId(), putResponse.getError(), putResponse.getCorrelationId(),
                  blobId);
              processServerError(putResponse.getError());
              isSuccessful = false;
              putRequestFinalState =
                  putError == ServerErrorCode.TemporarilyDisabled ? TrackedRequestFinalState.REQUEST_DISABLED
                      : TrackedRequestFinalState.FAILURE;
            }
          }
        }
      }
      if (isSuccessful) {
        operationTracker.onResponse(requestInfo.getReplicaId(), TrackedRequestFinalState.SUCCESS);
        if (RouterUtils.isRemoteReplica(routerConfig, requestInfo.getReplicaId())) {
          logger.trace("{}: Cross colo request successful for remote replica in {} ", loggingContext,
              requestInfo.getReplicaId().getDataNodeId().getDatacenterName());
          routerMetrics.crossColoSuccessCount.inc();

          // We use a separate metric to the latency of remote vs local writes (such requests are routed through
          // ParanoidDurabilityOperationTracker).
          routerMetrics.routerPutRequestRemoteLatencyMs.update(requestLatencyMs);
        } else {
          routerMetrics.routerPutRequestLocalLatencyMs.update(requestLatencyMs);
        }
      } else {
        onErrorResponse(requestInfo.getReplicaId(), putRequestFinalState);
      }
      checkAndMaybeComplete();
    }

    /**
     * @return {@code true} if CRC of the chunk buffer is same as one calculated in chunk filler thread
     */
    boolean verifyCRC() {
      if (!routerConfig.routerVerifyCrcForPutRequests || isMetadataChunk() || isCrcVerified) {
        return true;
      }
      Crc32 crc32 = new Crc32();
      for (ByteBuffer byteBuffer : buf.nioBuffers()) {
        crc32.update(byteBuffer);
      }
      isCrcVerified = true;
      logger.trace("Chunk Id {}, state {}, Original CRC {}, current CRC {}", chunkBlobId, state.name(),
          this.chunkCrc32.getValue(), crc32.getValue());
      return this.chunkCrc32.getValue() == crc32.getValue();
    }

    /**
     * Process response if it was rejected due to quota compliance.
     * @param correlationId correlation id of the request.
     * @param replicaId {@link ReplicaId} of the request.
     */
    private void processQuotaRejectedResponse(int correlationId, ReplicaId replicaId) {
      logger.debug("{}: PutRequest with response correlationId {} was rejected because quota was exceeded.",
          loggingContext, correlationId);
      setChunkException(new RouterException("QuotaExceeded", RouterErrorCode.TooManyRequests));
      onErrorResponse(replicaId, TrackedRequestFinalState.FAILURE, false);
      checkAndMaybeComplete();
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     * @param putRequestFinalState the {@link RouterErrorCode} associated with failed response.
     */
    private void onErrorResponse(ReplicaId replicaId, TrackedRequestFinalState putRequestFinalState) {
      onErrorResponse(replicaId, putRequestFinalState, true);
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     * @param putRequestFinalState the {@link RouterErrorCode} associated with failed response.
     * @param updateDataNodeMetrics {@code true} if data node metrics should be updated. {@code false} otherwise.
     */
    private void onErrorResponse(ReplicaId replicaId, TrackedRequestFinalState putRequestFinalState,
        boolean updateDataNodeMetrics) {
      // For Put, final state could be TIMED_OUT, REQUEST_DISABLED and FAILURE
      operationTracker.onResponse(replicaId, putRequestFinalState);
      routerMetrics.routerRequestErrorCount.inc();
      if (updateDataNodeMetrics) {
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).putRequestErrorCount.inc();
      }
    }

    /**
     * Possibly set the exception for this chunk using the given exception. Calling this method with an exception does
     * not necessarily result in that being set as the chunkException. The idea is to set the most relevant exception
     * in case of errors.
     * @param exception the exception that may be set as the chunkException.
     */
    private void setChunkException(RouterException exception) {
      chunkException = exception;
    }

    /**
     * Process an error received from the server. The idea is to convert from the ServerErrorCode to a RouterErrorCode.
     * @param error the ServerErrorCode received from a response to a request.
     */
    private void processServerError(ServerErrorCode error) {
      // for puts, all errors are effectively Ambry_Unavailable. One could argue that certain errors
      // are retryable and are eligible for slipped puts, but what could those be? Partition_ReadOnly and
      // BlobId_Already_Exists are outliers (should not really happen) that those should really
      // result in Ambry_Unavailable or UnexpectedInternalError.
      // However, for metrics, we will need to distinguish them here.
      setChunkException(new RouterException("Could not complete operation, server returned: " + error,
          RouterErrorCode.AmbryUnavailable));
    }

    /**
     * Set the blobId and chunkSize in the operationQuotaCharger, and return the {@link OperationQuotaCharger} object.
     * Note that this method might set the blobId and chunkSize for the same chunk multiple times. This is ok because
     * these are idempotent operation for the same chunk.
     * @return OperationQuotaCharger object.
     */
    private OperationQuotaCharger prepareQuotaCharger() {
      operationQuotaCharger.setBlobId(chunkBlobId);
      operationQuotaCharger.setChunkSize(chunkBlobSize);
      return operationQuotaCharger;
    }
  }

  /**
   * MetadataPutChunk responsible for maintaining the state of the metadata chunk and completing the chunk operation
   * on it.
   */
  private class MetadataPutChunk extends PutChunk {
    private final TreeMap<Integer, Pair<StoreKey, Long>> indexToChunkIdsAndChunkSizes;
    private int intermediateChunkSize = routerConfig.routerMaxPutChunkSizeBytes;
    private Pair<? extends StoreKey, BlobProperties> firstChunkIdAndProperties = null;
    private BlobId reservedMetadataChunkId = null;

    /**
     * Initialize the MetadataPutChunk.
     * If {@link RouterConfig#routerReservedMetadataEnabled} is {@code true}, then this will initialize the reserved
     * metadata chunk id. There are three cases:
     * 1. If this is a chunked upload, then the reserved metadata chunk id is passed in the blob properties.
     * 2. If this is a stitched blob request, then the reserved metadata is obtained from the specified chunksToStitch.
     * 3. If this is a regular upload, then a new reserved metadata chunk id is generated. Note that this reserved metadata
     *    id will be used only for composite blobs.
     *
     * @param chunksToStitch {@link List} of {@link ChunkInfo} objects to stitch for a stitchBlob request.
     *                       Will be {@code null} for all other put requests.
     * @throws RouterException In case of any error.
     */
    MetadataPutChunk(List<ChunkInfo> chunksToStitch) throws RouterException {
      indexToChunkIdsAndChunkSizes = new TreeMap<>();
      // metadata blob is in building state.
      state = ChunkState.Building;
      try {
        validateReservedMetadataChunkId();
        if (options.isChunkUpload()) {
          setReservedMetadataChunkId(passedInBlobProperties.getReservedMetadataBlobId());
        } else if (chunksToStitch == null) {
          // if it's not chunked upload, and chunksToStitch is null, then it's a regular upload.
          reservedMetadataChunkId =
              ClusterMapUtils.reserveMetadataBlobId(partitionClass, Collections.emptyList(), reservedMetadataIdMetrics,
                  clusterMap, passedInBlobProperties.getAccountId(), passedInBlobProperties.getContainerId(),
                  passedInBlobProperties.isEncrypted(), routerConfig);
        } else if (!chunksToStitch.isEmpty() && !RestUtils.isS3Request(restRequest)) {
          // Empty chunksToStitch list is already handled in PutOperation constructor by now.
          setReservedMetadataChunkId(chunksToStitch.get(0).getReservedMetadataId());
          for (ChunkInfo chunkInfo : chunksToStitch) {
            verifyStitchedBlobReservedMetadataChunkId(chunkInfo.getReservedMetadataId());
          }
        }
      } catch (RouterException routerException) {
        if (routerConfig.routerReservedMetadataEnabled) {
          reservedMetadataIdMetrics.metadataIdNotReservedCount.inc();
          throw routerException;
        }
      }
      if (reservedMetadataChunkId == null) {
        reservedMetadataIdMetrics.metadataIdNotReservedCount.inc();
      }
    }

    @Override
    boolean isMetadataChunk() {
      return true;
    }

    /**
     * Change the max chunk size option to support stitching chunks where the intermediate chunk sizes differ from
     * the current router chunking configuration. This setter is temporary and will be removed once a new metadata
     * blob format is introduced that supports intermediate chunks of arbitrary size.
     * @param intermediateChunkSize the chunk size for the intermediate (not last) data chunks listed in the metadata
     *                              blob.
     * @throws RouterException if the intermediate chunk size is invalid.
     */
    void setIntermediateChunkSize(long intermediateChunkSize) throws RouterException {
      if (intermediateChunkSize <= 0 || intermediateChunkSize > routerConfig.routerMaxPutChunkSizeBytes) {
        throw new RouterException("Invalid intermediate chunk size: " + intermediateChunkSize,
            RouterErrorCode.InvalidPutArgument);
      }
      this.intermediateChunkSize = (int) intermediateChunkSize;
    }

    /**
     * Add the given blobId of a successfully put data chunk to the metadata at its position in the overall blob.
     * @param storeKey the blobId of the associated data chunk
     * @param chunkSize size of the data chunk
     * @param chunkIndex the position of the associated data chunk in the overall blob.
     */
    void addChunkId(StoreKey storeKey, long chunkSize, int chunkIndex) {
      indexToChunkIdsAndChunkSizes.put(chunkIndex, new Pair<>(storeKey, chunkSize));
    }

    /**
     * Validate the reserved metadata chunk id is passed in correctly where it needs to be specified.
     * @throws RouterException in case of any errors.
     */
    private void validateReservedMetadataChunkId() throws RouterException {
      if (!options.isChunkUpload() && Objects.nonNull(passedInBlobProperties.getReservedMetadataBlobId())) {
        reservedMetadataIdMetrics.reservedMetadataPassedInForNonChunkedUploadCount.inc();
        throw new RouterException("Only chunked uploads can have reserved metadata blob id passed in.",
            RouterErrorCode.InvalidOrMismatchedStitchBlobReservedMetadataChunkId);
      }
      if (options.isChunkUpload() && passedInBlobProperties.getReservedMetadataBlobId() == null) {
        reservedMetadataIdMetrics.noReservedMetadataForChunkedUploadCount.inc();
        throw new RouterException("No reserved metadata passed in for chunked upload",
            RouterErrorCode.InvalidOrMismatchedStitchBlobReservedMetadataChunkId);
      }
    }

    /**
     * Validate and set the specified chunkId as the reserved metadata chunk id.
     * @param chunkId The chunk id to set.
     * @throws RouterException In case of any issue during validation.
     */
    private void setReservedMetadataChunkId(String chunkId) throws RouterException {
      try {
        reservedMetadataChunkId = new BlobId(chunkId, clusterMap);
      } catch (Exception exception) {
        reservedMetadataIdMetrics.stitchedBlobMetadataIdDeserErrorCount.inc();
        throw new RouterException("Invalid metadata chunk id of chunks in the stitched operation.",
            RouterErrorCode.InvalidOrMismatchedStitchBlobReservedMetadataChunkId);
      }
      if (reservedMetadataChunkId.getBlobDataType() != BlobDataType.METADATA) {
        reservedMetadataIdMetrics.stitchedBlobMetadataIdDeserErrorCount.inc();
        throw new RouterException("The blob data type of the metadata blob should be BlobIdType.METADATA",
            RouterErrorCode.InvalidOrMismatchedStitchBlobReservedMetadataChunkId);
      }
    }

    /**
     * Verify that the reserved metadata chunk id matches the specified chunk id.
     * @param chunkId chunk id to be matched.
     * @throws RouterException In case of a mismatch.
     */
    private void verifyStitchedBlobReservedMetadataChunkId(String chunkId) throws RouterException {
      String reservedMetadataChunkIdStr = (reservedMetadataChunkId == null) ? null : reservedMetadataChunkId.getID();
      if (!Objects.equals(reservedMetadataChunkIdStr, chunkId)) {
        reservedMetadataChunkId = null;
        reservedMetadataIdMetrics.stitchedBlobMetadataIdMismatchCount.inc();
        throw new RouterException("Mismatch in metadata chunk ids of chunks in stitched operation.",
            RouterErrorCode.InvalidOrMismatchedStitchBlobReservedMetadataChunkId);
      }
    }

    /**
     * Call {@link NotificationSystem#onBlobCreated(String, BlobProperties, Account, Container, NotificationBlobType)}
     * for this chunk, unless it is the first chunk, in which case it might be an entire simple blob. In that case, save
     * the {@link BlobProperties} from the first chunk.
     * @param chunk the {@link PutChunk} created.
     */
    void maybeNotifyForChunkCreation(PutChunk chunk) {
      if (chunk.chunkIndex == 0) {
        firstChunkIdAndProperties = new Pair<>(chunk.chunkBlobId, chunk.chunkBlobProperties);
      } else {
        Pair<Account, Container> accountContainer =
            RouterUtils.getAccountContainer(accountService, chunk.chunkBlobProperties.getAccountId(),
                chunk.chunkBlobProperties.getContainerId());
        notificationSystem.onBlobCreated(chunk.chunkBlobId.getID(), chunk.chunkBlobProperties,
            accountContainer.getFirst(), accountContainer.getSecond(), NotificationBlobType.DataChunk);
      }
    }

    /**
     * Notify for the creation of the first chunk. To be called after the overall operation is completed if the overall
     * blob is composite. If no first chunk was put successfully, this will do nothing.
     */
    void maybeNotifyForFirstChunkCreation() {
      if (indexToChunkIdsAndChunkSizes.get(0) != null) {
        // reason to check for not null: there are chances that 2nd chunk would completes before the first chunk and
        // the first chunk failed later. In such cases, even though metadata chunk might return some successfully
        // completed chunkIds, the first chunk may be null
        String chunkId = firstChunkIdAndProperties.getFirst().getID();
        BlobProperties chunkProperties = firstChunkIdAndProperties.getSecond();
        Pair<Account, Container> accountContainer =
            RouterUtils.getAccountContainer(accountService, chunkProperties.getAccountId(),
                chunkProperties.getContainerId());
        notificationSystem.onBlobCreated(chunkId, chunkProperties, accountContainer.getFirst(),
            accountContainer.getSecond(), NotificationBlobType.DataChunk);
      }
    }

    @Override
    void poll(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      if (isBuilding() && chunkFillingCompletedSuccessfully
          && indexToChunkIdsAndChunkSizes.size() == getNumDataChunks()) {
        finalizeMetadataChunk();
      }
      if (isReady()) {
        super.poll(requestRegistrationCallback);
      }
    }

    /**
     * To be called when chunk filling completes successfully. Finalizing involves preparing the metadata chunk
     * for sending if this blob is composite, or marking the operation complete if this is a simple blob.
     */
    private void finalizeMetadataChunk() {
      finalBlobProperties =
          new BlobProperties(getBlobSize(), passedInBlobProperties.getServiceId(), passedInBlobProperties.getOwnerId(),
              passedInBlobProperties.getContentType(), passedInBlobProperties.isPrivate(),
              passedInBlobProperties.getTimeToLiveInSeconds(), passedInBlobProperties.getCreationTimeInMs(),
              passedInBlobProperties.getAccountId(), passedInBlobProperties.getContainerId(),
              passedInBlobProperties.isEncrypted(), passedInBlobProperties.getExternalAssetTag(),
              passedInBlobProperties.getContentEncoding(), passedInBlobProperties.getFilename(), null);

      if (options.skipCompositeChunk()) {
        // close the request as the single blob. we don't generate the composite blob.
        // we set first chunk id to the blobid since it's not supposed to be null. But we return putBlobMetaInfo only.
        blobId = (BlobId) indexToChunkIdsAndChunkSizes.get(0).getFirst();
        state = ChunkState.Complete;
        setOperationCompleted();

        // now generate the PutBlobMetaInfo which includes the data chunk list
        // values returned are in the right order as TreeMap returns them in key-order.
        // .stream.map.collect is supposed to return in order
        List<Pair<String, Long>> orderedChunkList = new ArrayList<>(indexToChunkIdsAndChunkSizes.values()
            .stream()
            .map(p -> new Pair<>(p.getFirst().toString(), p.getSecond()))
            .collect(Collectors.toList()));
        // S3 Part Upload, we don't reserve metadata chunk id for each data chunk. Refer to resolveReservedMetadataId.
        PutBlobMetaInfo putBlobMetaInfoObj = new PutBlobMetaInfo(orderedChunkList, null);
        putBlobMetaInfo = PutBlobMetaInfo.serialize(putBlobMetaInfoObj);
        logger.debug("S3 5MB : finalizeMetadataChunk orderedChunkIdList is {} {}", indexToChunkIdsAndChunkSizes,
            putBlobMetaInfo);
      } else if (isStitchOperation() || getNumDataChunks() > 1) {
        ByteBuffer serialized = null;
        // values returned are in the right order as TreeMap returns them in key-order.
        if (routerConfig.routerMetadataContentVersion == MessageFormatRecord.Metadata_Content_Version_V2) {
          serialized = MetadataContentSerDe.serializeMetadataContentV2(intermediateChunkSize, getBlobSize(),
              getSuccessfullyPutChunkIds());
        } else if (routerConfig.routerMetadataContentVersion == MessageFormatRecord.Metadata_Content_Version_V3) {
          List<Pair<StoreKey, Long>> orderedChunkIdList = new ArrayList<>(indexToChunkIdsAndChunkSizes.values());
          serialized = MetadataContentSerDe.serializeMetadataContentV3(getBlobSize(), orderedChunkIdList);
        } else {
          throw new IllegalStateException(
              "Unexpected metadata content version: " + routerConfig.routerMetadataContentVersion);
        }
        serialized.flip();
        buf = Unpooled.wrappedBuffer(serialized);
        onFillComplete(false);
        checkReservedPartitionState();
      } else {
        // if there is only one chunk
        blobId = (BlobId) indexToChunkIdsAndChunkSizes.get(0).getFirst();
        state = ChunkState.Complete;
        setOperationCompleted();
      }
    }

    /**
     * @return a list of all of the successfully put chunk ids associated with this blob
     */
    List<StoreKey> getSuccessfullyPutChunkIds() {
      return indexToChunkIdsAndChunkSizes.values().stream().map(Pair::getFirst).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * In constructing the put request for the metadata blob, MetadataPutChunk serializes the metadata.
     * @return the created {@link PutRequest}.
     */
    @Override
    protected PutRequest createPutRequest() {
      return new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
          chunkBlobId, finalBlobProperties, ByteBuffer.wrap(chunkUserMetadata), buf.retainedDuplicate(),
          buf.readableBytes(), BlobType.MetadataBlob,
          encryptedPerBlobKey != null ? encryptedPerBlobKey.duplicate() : null);
    }

    /**
     * Check if the partition of the reserved metadata chunk is available to take write.
     */
    private void checkReservedPartitionState() {
      if (reservedMetadataChunkId == null) {
        reservedMetadataIdMetrics.metadataIdNotReservedCount.inc();
        return;
      }
      PartitionId reservedPartitionId = reservedMetadataChunkId.getPartition();
      if (reservedPartitionId.getPartitionState() == PartitionState.READ_ONLY) {
        reservedMetadataIdMetrics.numReservedPartitionFoundReadOnlyCount.inc();
      }
      if (!clusterMap.hasEnoughEligibleReplicasAvailableForPut(reservedPartitionId, routerConfig.routerPutSuccessTarget,
          true)) {
        reservedMetadataIdMetrics.numReservedPartitionFoundUnavailableInLocalDcCount.inc();
      }
      if (!clusterMap.hasEnoughEligibleReplicasAvailableForPut(reservedPartitionId, routerConfig.routerPutSuccessTarget,
          false)) {
        reservedMetadataIdMetrics.numReservedPartitionFoundUnavailableInAllDcCount.inc();
      }
    }
  }

  /**
   * @return the reserved metadata {@link BlobId} associated with this operation.
   */
  BlobId getReservedMetadataId() {
    return (metadataPutChunk == null) ? null : metadataPutChunk.reservedMetadataChunkId;
  }

  /**
   * Return a string message that will be used as a context for logging;
   * @return
   */
  private String makeLoggingContext() {
    if (restRequest == null || restRequest.getRestRequestContext() == null) {
      return "";
    }
    String channelStr =
        ((NettyRequest.NettyRequestContext) restRequest.getRestRequestContext()).getChannel().toString();
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    sb.append(channelStr);
    if (isEncryptionEnabled) {
      sb.append(", Encrypted");
    }
    return sb.append("]").toString();
  }

  /**
   * Different states of a PutChunk.
   */
  enum ChunkState {
    /**
     * The Chunk is free and can be filled with data.
     */
    Free,
    /**
     * The Chunk is being built. It may have some data but is not yet ready to be sent.
     */
    Building,
    /**
     * The Chunk is waiting for resolution of the blob type.
     * Only the first chunk of a blob can be in this state.
     */
    AwaitingBlobTypeResolution,
    /**
     * The Chunk is being encrypted.
     */
    Encrypting,
    /**
     * The Chunk is ready to be sent out.
     */
    Ready,
    /**
     * The Chunk is complete.
     */
    Complete,
  }
}

