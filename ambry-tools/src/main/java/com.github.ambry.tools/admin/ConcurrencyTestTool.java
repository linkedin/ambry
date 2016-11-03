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
package com.github.ambry.tools.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.getRandomLong;


/**
 * Concurrency Test Tool is used to test any end point (server/frontend) for concurrent GETs and PUTs in a mixed type of
 * workload(puts and gets). Also, it issues GET of the same blob N no of times and ensures we get the same blob everytime.
 * This tool supports testing of Ambry Server for now. Possible to use the same tool to test our frontends as well
 * if need be with another PutGetHelper pertaining to frontends.
 *
 * Caution: We might need to be cautious about OOM errors as this tool holds all the blobs(that were injected) in memory
 * injected until the GETs have reached the threshold for the a particular blob.
 *
 * Supported features
 * - Parallel Puts and Gets
 * - Concurrent GETs for the same blobs
 * - This also means that we will get occasional bursts of GETs
 *
 * TODO: Metrics
 *
 * Future work:
 * - Adding delay between concurrent GETs for same blobs
 * - Adding delay for GETs to start so that the file cache will only have recently uploaded blobs(and not all)
 *
 * Example :
 *  java -cp ambry.jar com.github.ambry.tools.admin.ConcurrencyTestTool --hardwareLayout [HardwareLayoutFile]
 *  --partitionLayout [PartitionLayoutFile] --putGetHelperFactory com.github.ambry.tools.admin.ServerPutGetHelperFactory
 *  --maxParallelPutCount 2 --parallelGetCount 4 --burstCountForGet 4 --maxGetCountPerBlob 10
 *  --totalPutBlobCount 1000 --maxBlobSizeInBytes 1000 --minBlobSizeInBytes 100 --hostName [HostName] --port [PortNo]
 *  --deleteOnExit true --enableVerboseLogging true
 *
 *  Few other options that might be useful:
 * --outFile [FilePath]                 : Output file to redirect the output to
 * --sleepTimeBetweenBatchGetsInMs long : Time to sleep in ms between batch Gets per Consumer thread
 * --sleepTimeBetweenBatchPutsInMs long : Time to sleep in ms between batch Puts per Producer thread
 * --burstThreadCountForPut int         : No of concurrent PUTs by each producer thread
 */
public class ConcurrencyTestTool {

  private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTestTool.class);

  public static void main(String args[])
      throws Exception {
    InvocationOptions options = new InvocationOptions(args);
    ClusterMap clusterMap = new ClusterMapManager(options.hardwareLayoutFilePath, options.partitionLayoutFilePath,
        new ClusterMapConfig(new VerifiableProperties(new Properties())));

    PutGetHelperFactory putGetHelperFactory =
        Utils.getObj(options.putGetHelperFactoryStr, new Properties(), options.hostName, options.port, clusterMap,
            options.maxBlobSizeInBytes, options.minBlobSizeInBytes, options.enabledVerboseLogging);

    PutGetHelper putGetHelper = putGetHelperFactory.getPutGetHelper();
    ConcurrencyTestTool concurrencyTestTool = new ConcurrencyTestTool();
    concurrencyTestTool.startTest(putGetHelper, options.maxParallelPutCount, options.parallelGetCount,
        options.totalPutBlobCount, options.maxGetCountPerBlob, options.burstCountForGet, options.deleteOnExit,
        options.enabledVerboseLogging, options.sleepTimeBetweenBatchPutsInMs, options.sleepTimeBetweenBatchGetsInMs);
  }

  /**
   * Initializes and starts the concurrent test tool.
   * @param putGetHelper the {@link PutGetHelper} to be used for PUTs and GETs
   * @param maxParallelPutCount maximum number of parallel puts per phase
   * @param parallelGetCount the number of parallel Gets per phase
   * @param maxPutCount total number of blobs to be uploaded
   * @param maxGetCountPerBlob total number of times a particular blob can be fetched before its deleted
   * @param burstCountGet Burst count for GETs. Each GET consumer thread will fetch a blob these many no of
   *                         times simultaneously
   * @param deleteOnExit Deletes the blobs once GETs have reached a threshold on every blob
   * @param enableVerboseLogging Enables verbose logging
   * @param sleepTimeBetweenBatchPutsInMs Time to sleep in ms between batch Puts per Producer thread
   * @param sleepTimeBetweenBatchGetsInMs Time to sleep in ms between batch Gets per Consumer thread
   * @throws InterruptedException
   */
  public void startTest(PutGetHelper putGetHelper, int maxParallelPutCount, int parallelGetCount, int maxPutCount,
      int maxGetCountPerBlob, int burstCountGet, boolean deleteOnExit, boolean enableVerboseLogging,
      long sleepTimeBetweenBatchPutsInMs, long sleepTimeBetweenBatchGetsInMs)
      throws InterruptedException {

    AtomicInteger currentPutCount = new AtomicInteger(0);
    AtomicInteger getCompletedCount = new AtomicInteger(0);
    // Creating shared queue for producer and consumer
    ArrayList<BlobAccessInfo> sharedQueue = new ArrayList<>();
    BlockingQueue<String> deletedBlobIds = new LinkedBlockingQueue<>();

    final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    logger.trace("Initiating Producer thread ");
    // Creating PutThread
    Thread putThread = new Thread(
        new PutThread("Producer", putGetHelper, sharedQueue, maxPutCount, currentPutCount, maxParallelPutCount,
            sleepTimeBetweenBatchPutsInMs, enableVerboseLogging, threadLocalRandom));

    logger.trace("Initiating Consumer thread ");
    // Creating Get thread
    Thread getThread = new Thread(
        new GetThread("Consumer", putGetHelper, sharedQueue, maxGetCountPerBlob, maxPutCount, getCompletedCount,
            burstCountGet, parallelGetCount, sleepTimeBetweenBatchGetsInMs, deleteOnExit, enableVerboseLogging,
            threadLocalRandom, deletedBlobIds));

    Thread deleteThread = null;
    if (deleteOnExit) {
      logger.trace("Initiating Delete thread ");
      DeleteThread deleteBlobThread =
          new DeleteThread("DeleteThread", putGetHelper, maxPutCount, deletedBlobIds, enableVerboseLogging);
      deleteThread = new Thread(deleteBlobThread);
      deleteThread.start();
    }

    logger.trace("Starting Producer thread ");
    putThread.start();
    logger.trace("Starting Consumer thread ");
    getThread.start();

    putThread.join();
    getThread.join();

    if (deleteThread != null) {
      logger.trace("Starting Delete thread ");
      deleteThread.join();
    }

    logger.trace("Invoking shutdown after completing the test suite ");
    // shutting down the PutGetHelper class to release any resources held up
    putGetHelper.shutdown();
  }

  /**
   * Put threads to assist in producing blobs or uploading blobs to ambry
   */
  class PutThread implements Runnable {
    private final String threadName;
    private final PutGetHelper producerJob;
    private final ArrayList<BlobAccessInfo> blobInfos;
    private final int maxPutCount;
    private final AtomicInteger currentPutCount;
    private final int maxParallelRequest;
    private final long sleepTimeBetweenBatchPutsInMs;
    private final boolean enableVerboseLogging;
    private final ThreadLocalRandom threadLocalRandom;

    public PutThread(String threadName, PutGetHelper producerJob, ArrayList<BlobAccessInfo> blobInfos,
        final int maxPutCount, AtomicInteger currentPutCount, int maxParallelRequest,
        long sleepTimeBetweenBatchPutsInMs, boolean enableVerboseLogging, ThreadLocalRandom threadLocalRandom) {
      this.threadName = threadName;
      this.producerJob = producerJob;
      this.blobInfos = blobInfos;
      this.maxPutCount = maxPutCount;
      this.currentPutCount = currentPutCount;
      this.maxParallelRequest = maxParallelRequest;
      this.sleepTimeBetweenBatchPutsInMs = sleepTimeBetweenBatchPutsInMs;
      this.enableVerboseLogging = enableVerboseLogging;
      this.threadLocalRandom = threadLocalRandom;
    }

    @Override
    public void run() {
      while (currentPutCount.get() < maxPutCount) {
        try {
          int burstCount = threadLocalRandom.nextInt(maxParallelRequest) + 1;
          logger.trace(threadName + " Producing " + burstCount + " times ");
          CountDownLatch countDownLatch = new CountDownLatch(burstCount);
          for (int j = 0; j < burstCount; j++) {
            Callback callback = new Callback() {
              @Override
              public void onCompletion(Object result, Exception exception) {
                if (exception == null) {
                  Pair<String, byte[]> response = (Pair<String, byte[]>) result;
                  if (currentPutCount.get() < maxPutCount) {
                    blobInfos.add(new BlobAccessInfo(response.getFirst(), response.getSecond()));
                    currentPutCount.incrementAndGet();
                  }
                }
                countDownLatch.countDown();
              }
            };
            FutureResult futureResult = new FutureResult();
            producerJob.produce(futureResult, callback);
          }
          countDownLatch.await();
          Thread.sleep(threadLocalRandom.nextLong(sleepTimeBetweenBatchPutsInMs));
        } catch (InterruptedException ex) {
          logger.error("InterruptedException in " + threadName + " " + ex.getStackTrace());
        }
      }
      logger.info("Exiting Producer " + threadName + ": current Put Count " + currentPutCount.get() + ", max Put Count "
          + maxPutCount);
    }
  }

  /**
   * Record to hold information about a blob along with its content and its reference count
   */
  class BlobAccessInfo {
    String blobId;
    AtomicInteger referenceCount;
    AtomicInteger burstGetReferenceCount;
    byte[] blobContent;

    public BlobAccessInfo(String blobId, byte[] blobContent) {
      this.blobId = blobId;
      this.referenceCount = new AtomicInteger(0);
      this.burstGetReferenceCount = new AtomicInteger(0);
      this.blobContent = blobContent;
    }
  }

  /**
   * Get threads to assist in consuming blobs from ambry and verifying its content
   */
  class GetThread implements Runnable {
    private final String threadName;
    private final PutGetHelper getConsumerJob;
    private final ArrayList<BlobAccessInfo> blobInfos;
    private final int maxGetCountPerBlob;
    private final AtomicInteger getCompletedCount;
    private final int maxPutCount;
    private final int maxBurstCountPerblob;
    private final int parallelGetCount;
    private final boolean deleteOnExit;
    private final boolean enableVerboseLogging;
    private final long sleepTimeBetweenBatchGetsInMs;
    private final ThreadLocalRandom threadLocalRandom;
    private final BlockingQueue<String> deleteBlobIds;

    public GetThread(String threadName, PutGetHelper getConsumerJob, ArrayList<BlobAccessInfo> blobInfos,
        int maxGetCountPerBlob, final int maxPutCount, AtomicInteger getCompletedCount, int maxBurstCountPerBlob,
        int parallelGetCount, long sleepTimeBetweenBatchGetsInMs, boolean deleteOnExit, boolean enableVerboseLogging,
        ThreadLocalRandom threadLocalRandom, BlockingQueue<String> deleteBlobIds) {
      this.threadName = threadName;
      this.getConsumerJob = getConsumerJob;
      this.blobInfos = blobInfos;
      this.maxGetCountPerBlob = maxGetCountPerBlob;
      this.maxPutCount = maxPutCount;
      this.parallelGetCount = parallelGetCount;
      this.getCompletedCount = getCompletedCount;
      this.maxBurstCountPerblob = maxBurstCountPerBlob;
      this.deleteOnExit = deleteOnExit;
      this.sleepTimeBetweenBatchGetsInMs = sleepTimeBetweenBatchGetsInMs;
      this.enableVerboseLogging = enableVerboseLogging;
      this.threadLocalRandom = threadLocalRandom;
      this.deleteBlobIds = deleteBlobIds;
    }

    @Override
    public void run() {
      while (getCompletedCount.get() < maxPutCount) {
        try {
          int size = blobInfos.size();
          if (size > 0) {
            List<Pair<BlobAccessInfo, Integer>> blobInfoBurstCountPairList = new ArrayList<>();
            List<String> toBeDeletedBlobsPerBatch = new ArrayList<>();
            int blobCountPerBatch = Math.min(size, parallelGetCount);
            for (int i = 0; i < blobCountPerBatch; i++) {
              size = blobInfos.size();
              BlobAccessInfo blobAccessInfo = blobInfos.get(threadLocalRandom.nextInt(size));
              if (blobAccessInfo != null) {
                int burstCount = threadLocalRandom.nextInt(maxBurstCountPerblob) + 1;
                blobInfoBurstCountPairList.add(new Pair(blobAccessInfo, burstCount));
                blobAccessInfo.referenceCount.addAndGet(burstCount);
                if (blobAccessInfo.referenceCount.get() >= maxGetCountPerBlob) {
                  blobInfos.remove(blobAccessInfo);
                  getCompletedCount.incrementAndGet();
                  toBeDeletedBlobsPerBatch.add(blobAccessInfo.blobId);
                }
              }
            }

            CountDownLatch countDownLatchForBatch = new CountDownLatch(blobCountPerBatch);
            for (Pair<BlobAccessInfo, Integer> blobAccessInfoBurstCountPair : blobInfoBurstCountPairList) {
              int burstCount = blobAccessInfoBurstCountPair.getSecond();
              BlobAccessInfo blobAccessInfo = blobAccessInfoBurstCountPair.getFirst();
              logger.trace(threadName + " fetching " + blobAccessInfo.blobId + " " + burstCount + " times ");
              CountDownLatch countDownLatch = new CountDownLatch(burstCount);
              for (int j = 0; j < burstCount; j++) {
                Callback callback = new Callback() {
                  @Override
                  public void onCompletion(Object result, Exception exception) {
                    countDownLatch.countDown();
                    blobAccessInfo.burstGetReferenceCount.decrementAndGet();
                    if (countDownLatch.getCount() == 0) {
                      countDownLatchForBatch.countDown();
                    }
                  }
                };
                FutureResult futureResult = new FutureResult();
                blobAccessInfo.burstGetReferenceCount.incrementAndGet();
                getConsumerJob.consumeAndValidate(blobAccessInfo.blobId, blobAccessInfo.blobContent,
                    getConsumerJob.getErrorCodeForNoError(), futureResult, callback);
              }
            }
            countDownLatchForBatch.await();
            if (deleteOnExit) {
              deleteBlobIds.addAll(toBeDeletedBlobsPerBatch);
            }
          }
          Thread.sleep(threadLocalRandom.nextLong(sleepTimeBetweenBatchGetsInMs));
        } catch (InterruptedException ex) {
          logger.error("InterruptedException in " + threadName + ex.getStackTrace());
        }
      }
      logger.info("Exiting GetConsumer " + threadName);
    }
  }

  /**
   * Thread used for deleting blobs from ambry
   */
  class DeleteThread implements Runnable {
    private final String threadName;
    private final PutGetHelper deleteConsumerJob;
    private final boolean enableVerboseLogging;
    private final int putCount;
    private AtomicInteger deletedCount = new AtomicInteger(0);
    private BlockingQueue<String> deleteBlobIds;
    private CountDownLatch countDownLatch;

    public DeleteThread(String threadName, PutGetHelper deleteConsumerJob, int putCount,
        BlockingQueue<String> deleteBlobIds, boolean enableVerboseLogging) {
      this.threadName = threadName;
      this.putCount = putCount;
      this.deleteConsumerJob = deleteConsumerJob;
      this.enableVerboseLogging = enableVerboseLogging;
      this.deleteBlobIds = deleteBlobIds;
      this.countDownLatch = new CountDownLatch(putCount);
    }

    public void run() {
      while (deletedCount.get() < putCount) {
        try {
          String blobIdStr = deleteBlobIds.poll(50, TimeUnit.MILLISECONDS);
          if (blobIdStr != null) {
            deletedCount.incrementAndGet();
            logger.trace(threadName + " Deleting blob " + blobIdStr);
            FutureResult futureResult = new FutureResult();
            Callback callback = new Callback() {
              @Override
              public void onCompletion(Object result, Exception exception) {
                logger.trace("Deletion completed for " + blobIdStr);
                countDownLatch.countDown();
              }
            };
            deleteConsumerJob.deleteAndValidate(blobIdStr, deleteConsumerJob.getErrorCodeForNoError(), futureResult,
                callback);
          }
        } catch (InterruptedException e) {
          logger.error(" Interrupted Exception thrown in " + threadName + ", exception " + e.getStackTrace());
        }
      }
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        logger.error(
            " Interrupted Exception thrown while waiting for deletion to complete " + threadName + ", exception "
                + e.getStackTrace());
      }
    }
  }

  /**
   * An interface for assisting in producing and consuming blobs
   */
  interface PutGetHelper<T> {
    /**
     * Request to produce a blob
     * @return a {@link Pair<String, byte[]>} which contains the blobId and the content
     */
    FutureResult produce(FutureResult futureResult, Callback callback);

    /**
     * Request to fetch the blob pertaining to the {@code blobId} passed in and verify for its content
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param blobContent the content of the blob that needs to be verified against
     * @param expectedErrorCode the expected error code for the Get call
     */
    void consumeAndValidate(String blobId, byte[] blobContent, T expectedErrorCode, FutureResult futureResult,
        Callback callback);

    /**
     * Deletes a blob from ambry pertaining to the {@code blobId} passed in and verifies that subsequent Get fails
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param expectedErrorCode the expected error code for the Delete call
     */
    void deleteAndValidate(String blobId, T expectedErrorCode, FutureResult futureResult, Callback callback);

    /**
     * Returns the default Error code that is expected for a Get or a Delete call
     * @return the default error code that is expected for a Get or a Delete call
     */
    T getErrorCodeForNoError();

    void shutdown()
        throws InterruptedException;
  }

  /**
   * An implementation of {@link PutGetHelper} for an Ambry Server
   */
  static class ServerPutGetHelper implements PutGetHelper<ServerErrorCode> {
    private final String hostName;
    private final int port;
    private final ClusterMap clusterMap;
    private final int maxBlobSize;
    private final int minBlobSize;
    private final boolean enableVerboseLogging;
    private final ThreadLocalRandom localRandom;
    private final NetworkClientUtils networkClientUtils;
    private final AtomicInteger correlationIdGenerator = new AtomicInteger(0);

    public ServerPutGetHelper(Properties properties, String hostName, int port, ClusterMap clusterMap, int maxBlobSize,
        int minBlobSize, Boolean enableVerboseLogging)
        throws Exception {
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      NetworkClientFactory networkClientFactory =
          new NetworkClientFactory(new NetworkMetrics(new MetricRegistry()), new NetworkConfig(verifiableProperties),
              null, 20, 20, 10000, SystemTime.getInstance());
      NetworkClient networkClient = networkClientFactory.getNetworkClient();
      this.networkClientUtils = new NetworkClientUtils(networkClient, 50);
      this.hostName = hostName;
      this.port = port;
      this.clusterMap = clusterMap;
      this.maxBlobSize = maxBlobSize;
      this.minBlobSize = minBlobSize;
      this.enableVerboseLogging = enableVerboseLogging;
      this.localRandom = ThreadLocalRandom.current();
    }

    /**
     * {@inheritDoc}
     * Uploads a blob to the server directly and returns the blobId along with the content
     * @return a {@link Pair<String, byte[]>} which contains the blobId and the content
     * @throws IOException
     */
    @Override
    public FutureResult produce(FutureResult futureResult, Callback callback) {
      int randomNum = localRandom.nextInt((maxBlobSize - minBlobSize) + 1) + minBlobSize;
      byte[] blob = new byte[randomNum];
      byte[] usermetadata = new byte[new Random().nextInt(1024)];
      BlobProperties props = new BlobProperties(randomNum, "test");
      int correlationId = -1;
      try {
        List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
        int index = (int) getRandomLong(localRandom, partitionIds.size());
        PartitionId partitionId = partitionIds.get(index);
        BlobId blobId = new BlobId(partitionId);
        correlationId = correlationIdGenerator.incrementAndGet();
        PutRequest putRequest =
            new PutRequest(correlationId, "ConcurrencyTest", blobId, props, ByteBuffer.wrap(usermetadata),
                ByteBuffer.wrap(blob), props.getBlobSize(), BlobType.DataBlob);
        long startTime = SystemTime.getInstance().nanoseconds();
        List<NetworkClientUtils.RequestMetadata> requestMetadataList = new ArrayList<>();
        NetworkClientUtils.RequestMetadata requestMetadata = new NetworkClientUtils.RequestMetadata<>();
        requestMetadata.requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), putRequest);
        requestMetadata.futureResult = new FutureResult<ByteBuffer>();
        requestMetadata.correlationId = correlationId;
        requestMetadata.callback = new Callback() {
          @Override
          public void onCompletion(Object result, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTime;
            logger.trace(requestMetadata.correlationId + " Time taken to put blob id " + blobId + " in ms "
                + latencyPerBlob / SystemTime.NsPerMs + " for blob of size " + blob.length);
            Exception exceptionToReturn = null;
            Pair<String, byte[]> toReturn = null;
            if (result != null) {
              try {
                ByteBuffer response = (ByteBuffer) result;
                PutResponse putResponse =
                    PutResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)));
                if (putResponse.getError() != ServerErrorCode.No_Error) {
                  exceptionToReturn = new UnexpectedException("error " + putResponse.getError());
                }
                toReturn = new Pair<>(blobId.getID(), blob);
              } catch (IOException e) {
                exceptionToReturn = e;
              }
            } else {
              exceptionToReturn = exception;
            }
            requestMetadata.futureResult.done(toReturn, exceptionToReturn);
            if (callback != null) {
              callback.onCompletion(toReturn, exceptionToReturn);
            }
          }
        };
        requestMetadataList.add(requestMetadata);
        networkClientUtils.poll(requestMetadataList);
        return requestMetadata.futureResult;
      } catch (Exception e) {
        logger.error(correlationId + " Unknown Exception thrown when putting blob " + e.getStackTrace());
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
      return null;
    }

    /**
     * {@inheritDoc}
     * Request to fetch the blob from the server directly pertaining to the {@code blobId} passed in and verify for
     * its content
     * @param blobIdStr the blobId the of that blob that needs to be fetched
     * @param blobContent the content of the blob that needs to be verified against
     * @param expectedErrorCode the expected error code for the Get call
     * @throws IOException
     */
    @Override
    public void consumeAndValidate(String blobIdStr, byte[] blobContent, ServerErrorCode expectedErrorCode,
        FutureResult futureResult, Callback callback) {
      final BlobData[] blobData = {null};
      BlobId blobId = null;
      int correlationId = -1;
      try {
        blobId = new BlobId(blobIdStr, clusterMap);
        ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
        blobIds.add(blobId);
        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
        partitionRequestInfoList.add(partitionRequestInfo);
        correlationId = correlationIdGenerator.incrementAndGet();
        GetRequest getRequest =
            new GetRequest(correlationId, "ConcurrencyTest", MessageFormatFlags.Blob, partitionRequestInfoList,
                GetOptions.None);
        Long startTimeGetBlob = SystemTime.getInstance().nanoseconds();
        List<NetworkClientUtils.RequestMetadata> requestInfoList = new ArrayList<>();
        NetworkClientUtils.RequestMetadata requestMetadata = new NetworkClientUtils.RequestMetadata<>();
        requestMetadata.requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), getRequest);
        requestMetadata.futureResult = new FutureResult<ByteBuffer>();
        requestMetadata.correlationId = correlationId;
        requestMetadata.callback = new Callback() {
          @Override
          public void onCompletion(Object result, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            Exception exceptionToReturn = null;
            if (result != null) {
              try {
                ByteBuffer response = (ByteBuffer) result;
                GetResponse getResponse =
                    GetResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)), clusterMap);
                if (getResponse.getError() == ServerErrorCode.No_Error) {
                  ServerErrorCode serverErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
                  if (serverErrorCode == ServerErrorCode.No_Error) {
                    blobData[0] = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
                    long sizeRead = 0;
                    byte[] outputBuffer = new byte[(int) blobData[0].getSize()];
                    ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
                    while (sizeRead < blobData[0].getSize()) {
                      streamOut.write(blobData[0].getStream().read());
                      sizeRead++;
                    }
                    boolean blobcontentMatched = Arrays.equals(blobContent, outputBuffer);
                    if (!blobcontentMatched) {
                      logger.error(requestMetadata.correlationId + " Blob content mismatch for " + blobIdStr + " from "
                          + hostName + ":" + port);
                    }
                  } else if (expectedErrorCode.equals(serverErrorCode)) {
                    logger.trace(
                        requestMetadata.correlationId + " Get of blob " + blobIdStr + " throws " + expectedErrorCode
                            + " as expected ");
                  } else {
                    logger.error(requestMetadata.correlationId + " Get of blob " + blobIdStr
                        + " failed with unexpected Store level error code " + serverErrorCode + ", expected "
                        + expectedErrorCode);
                    exceptionToReturn = new IllegalStateException(
                        requestMetadata.correlationId + " Get of " + blobIdStr + " throws " + serverErrorCode
                            + " different from what is expected  " + expectedErrorCode);
                  }
                } else {
                  logger.error(requestMetadata.correlationId + " Get of blob " + blobIdStr
                      + " failed with unexpected Server Level error code " + getResponse.getError() + ", expected "
                      + expectedErrorCode);
                  exceptionToReturn = new IllegalStateException(
                      requestMetadata.correlationId + " Get of " + blobIdStr + " throws " + getResponse.getError()
                          + " different from what is expected  " + expectedErrorCode);
                }
                logger.trace(requestMetadata.correlationId + " Time taken to get blob " + blobIdStr + " in ms "
                    + latencyPerBlob / SystemTime.NsPerMs);
              } catch (IOException e) {
                exceptionToReturn = e;
              } catch (MessageFormatException e) {
                exceptionToReturn = e;
              }
            } else {
              exceptionToReturn = exception;
            }
            requestMetadata.futureResult.done(null, exceptionToReturn);
            if (callback != null) {
              callback.onCompletion(null, exceptionToReturn);
            }
          }
        };
        requestInfoList.add(requestMetadata);
        networkClientUtils.poll(requestInfoList);
      } catch (Exception e) {
        logger.error(
            correlationId + " Unknown Exception thrown when getting blob " + blobId + ", " + e.getStackTrace());
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
    }

    /**
     * {@inheritDoc}
     * Request to delete the blob from the server directly pertaining to the {@code blobId} passed in and verify that
     * the subsequent get fails with {@link ServerErrorCode#Blob_Deleted}
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param expectedErrorCode the expected error code for the Delete call
     * @throws IOException
     */
    @Override
    public void deleteAndValidate(String blobId, ServerErrorCode expectedErrorCode, FutureResult futureResult,
        Callback callback) {
      int correlationId = -1;
      try {
        correlationId = correlationIdGenerator.incrementAndGet();
        DeleteRequest deleteRequest =
            new DeleteRequest(correlationId, "ConcurrencyTest", new BlobId(blobId, clusterMap));
        Long startTimeDeleteBlob = SystemTime.getInstance().nanoseconds();
        List<NetworkClientUtils.RequestMetadata> requestMetadataList = new ArrayList<>();
        NetworkClientUtils.RequestMetadata requestMetadata = new NetworkClientUtils.RequestMetadata<>();
        requestMetadata.requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), deleteRequest);
        requestMetadata.futureResult = new FutureResult<ByteBuffer>();
        requestMetadata.correlationId = correlationId;
        requestMetadata.callback = new Callback() {
          @Override
          public void onCompletion(Object result, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeDeleteBlob;
            logger.trace(
                requestMetadata.correlationId + " Delete of  " + blobId + " took " + latencyPerBlob / SystemTime.NsPerMs
                    + " ms");
            Exception exceptionToReturn = null;
            if (result != null) {
              try {
                ByteBuffer response = (ByteBuffer) result;
                DeleteResponse deleteResponse =
                    DeleteResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)));
                if (deleteResponse.getError() != ServerErrorCode.No_Error) {
                  logger.error(requestMetadata.correlationId + " Deletion of " + blobId + " failed with an exception "
                      + deleteResponse.getError());
                  exceptionToReturn = new IllegalStateException(
                      requestMetadata.correlationId + " Deletion of " + blobId + " failed with an exception "
                          + deleteResponse.getError());
                } else {
                  logger.trace(requestMetadata.correlationId + " Deletion of " + blobId + " succeeded ");
                  FutureResult futureResultForGet = new FutureResult();
                  Callback callbackForGet = new Callback() {
                    @Override
                    public void onCompletion(Object result, Exception exception) {
                      futureResult.done(null, exception);
                      if (callback != null) {
                        callback.onCompletion(null, exception);
                      }
                    }
                  };
                  consumeAndValidate(blobId, null, ServerErrorCode.Blob_Deleted, futureResultForGet, null);
                }
              } catch (IOException e) {
                exceptionToReturn = e;
              }
            } else {
              exceptionToReturn = exception;
            }
            futureResult.done(null, exceptionToReturn);
            if (callback != null) {
              callback.onCompletion(null, exceptionToReturn);
            }
          }
        };
        requestMetadataList.add(requestMetadata);
        networkClientUtils.poll(requestMetadataList);
      } catch (Exception e) {
        logger.error(correlationId + " Unknown Exception thrown when deleting " + blobId + ", " + e.getStackTrace());
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
    }

    /**
     * {@inheritDoc}
     * Returns the default Error code that is expected for a Get call or a delete call
     * @return the default error code that is expected for a Get call or a delete call
     */
    @Override
    public ServerErrorCode getErrorCodeForNoError() {
      return ServerErrorCode.No_Error;
    }

    /**
     * {@inheritDoc}
     * Request to clean up resources used by this helper class, i.e. NetworkClient in this context
     */
    @Override
    public void shutdown()
        throws InterruptedException {
      networkClientUtils.close();
    }
  }

  static class InvocationOptions {
    public final String hardwareLayoutFilePath;
    public final String partitionLayoutFilePath;
    public final int maxParallelPutCount;
    public final int parallelGetCount;
    public final int burstCountForGet;
    public final int totalPutBlobCount;
    public final int maxGetCountPerBlob;
    public final int maxBlobSizeInBytes;
    public final int minBlobSizeInBytes;
    public final long sleepTimeBetweenBatchPutsInMs;
    public final long sleepTimeBetweenBatchGetsInMs;

    public final String hostName;
    public final int port;
    public final String putGetHelperFactoryStr;
    public final boolean deleteOnExit;
    public final boolean enabledVerboseLogging;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args the command line argument list.
     * @throws InstantiationException if all required arguments were not provided.
     * @throws IOException if help text could not be printed.
     */
    public InvocationOptions(String args[])
        throws InstantiationException, IOException {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> hardwareLayoutFilePath =
          parser.accepts("hardwareLayoutFilePath", "Path to hardware layout file")
              .withRequiredArg()
              .describedAs("hardwareLayoutFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> partitionLayoutFilePath =
          parser.accepts("partitionLayoutFilePath", "Path to partition layout file")
              .withRequiredArg()
              .describedAs("partitionLayoutFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> putGetHelperFactoryOpt =
          parser.accepts("putGetHelperFactory", "The reference to PutGetHelperFactory class")
              .withOptionalArg()
              .defaultsTo("com.github.ambry.tools.admin.ServerPutGetHelperFactory")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<Integer> maxParallelPutCountOpt =
          parser.accepts("maxParallelPutCountOpt", "Maximum number of parallel puts")
              .withOptionalArg()
              .describedAs("maxParallelPutCountOpt")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> parallelGetCountOpt =
          parser.accepts("parallelGetCountOpt", "Total number of parallel gets")
              .withOptionalArg()
              .describedAs("parallelGetCountOpt")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> burstCountForGetOpt =
          parser.accepts("burstCountForGet", "Total number of burst gets per blob")
              .withOptionalArg()
              .describedAs("burstCountForGet")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> totalPutBlobCountOpt =
          parser.accepts("totalPutBlobCount", "Total number of blobs to be produced")
              .withOptionalArg()
              .describedAs("totalPutBlobCount")
              .ofType(Integer.class)
              .defaultsTo(10);
      ArgumentAcceptingOptionSpec<Integer> maxGetCountPerBlobOpt =
          parser.accepts("maxGetCountPerBlob", "Max number of times a blob has to be fetched and verified")
              .withOptionalArg()
              .describedAs("maxGetCountPerBlob")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> maxBlobSizeInBytesOpt =
          parser.accepts("maxBlobSizeInBytes", "Maximum size of blobs to be uploaded")
              .withOptionalArg()
              .describedAs("maxBlobSizeInBytes")
              .ofType(Integer.class)
              .defaultsTo(100);
      ArgumentAcceptingOptionSpec<Integer> minBlobSizeInBytesOpt =
          parser.accepts("minBlobSizeInBytes", "Minimum size of blobs to be uploaded")
              .withOptionalArg()
              .describedAs("minBlobSizeInBytes")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> sleepTimeBetweenBatchPutsInMsOpt =
          parser.accepts("sleepTimeBetweenBatchPutsInMs", "Time to sleep in ms between batch puts")
              .withOptionalArg()
              .describedAs("sleepTimeBetweenBatchPutsInMs")
              .ofType(Integer.class)
              .defaultsTo(50);
      ArgumentAcceptingOptionSpec<Integer> sleepTimeBetweenBatchGetsInMsOpt =
          parser.accepts("sleepTimeBetweenBatchGetsInMs", "Time to sleep in ms between batch Gets")
              .withOptionalArg()
              .describedAs("sleepTimeBetweenBatchGetsInMs")
              .ofType(Integer.class)
              .defaultsTo(100);
      ArgumentAcceptingOptionSpec<String> hostNameOpt =
          parser.accepts("hostName", "The hostname against which requests are to be made")
              .withOptionalArg()
              .describedAs("hostName")
              .defaultsTo("localhost")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<Integer> portNumberOpt =
          parser.accepts("port", "The port number to be used while contacing the host")
              .withOptionalArg()
              .describedAs("port")
              .ofType(Integer.class)
              .defaultsTo(6667);
      ArgumentAcceptingOptionSpec<Boolean> deleteOnExitOpt =
          parser.accepts("deleteOnExit", "Delete blobs on exiting the test")
              .withOptionalArg()
              .describedAs("deleteOnExit")
              .ofType(Boolean.class)
              .defaultsTo(true);
      ArgumentAcceptingOptionSpec<Boolean> enableVerboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging if set to true")
              .withOptionalArg()
              .describedAs("enableVerboseLogging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
      requiredArgs.add(hardwareLayoutFilePath);
      requiredArgs.add(partitionLayoutFilePath);

      OptionSet options = parser.parse(args);
      if (hasRequiredOptions(requiredArgs, options)) {
        this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
        logger.trace("Hardware layout file path: {}", this.hardwareLayoutFilePath);
        this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
        logger.trace("Partition layout file path: {}", this.partitionLayoutFilePath);
      } else {
        parser.printHelpOn(System.err);
        throw new InstantiationException("Did not receive all required arguments for starting RestServer");
      }
      this.maxParallelPutCount = options.valueOf(maxParallelPutCountOpt);
      this.parallelGetCount = options.valueOf(parallelGetCountOpt);
      this.burstCountForGet = options.valueOf(burstCountForGetOpt);
      this.totalPutBlobCount = options.valueOf(totalPutBlobCountOpt);
      this.maxGetCountPerBlob = options.valueOf(maxGetCountPerBlobOpt);
      this.maxBlobSizeInBytes = options.valueOf(maxBlobSizeInBytesOpt);
      this.minBlobSizeInBytes = options.valueOf(minBlobSizeInBytesOpt);
      this.sleepTimeBetweenBatchPutsInMs = options.valueOf(sleepTimeBetweenBatchPutsInMsOpt);
      this.sleepTimeBetweenBatchGetsInMs = options.valueOf(sleepTimeBetweenBatchGetsInMsOpt);
      this.hostName = options.valueOf(hostNameOpt);
      this.port = options.valueOf(portNumberOpt);
      this.putGetHelperFactoryStr = options.valueOf(putGetHelperFactoryOpt);
      this.deleteOnExit = options.valueOf(deleteOnExitOpt);
      this.enabledVerboseLogging = options.valueOf(enableVerboseLoggingOpt);
    }

    /**
     * Checks if all required arguments are present. Prints the ones that are not.
     * @param requiredArgs the list of required arguments.
     * @param options the list of received options.
     * @return whether required options are present.
     */
    private boolean hasRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options) {
      boolean haveAll = true;
      for (OptionSpec opt : requiredArgs) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument " + opt);
          haveAll = false;
        }
      }
      return haveAll;
    }
  }
}
