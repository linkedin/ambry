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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.router.RouterFactory;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Concurrency Test Tool is used to test Ambry Router/Server for concurrent GETs and PUTs in a mixed type of
 * workload(puts and gets). Also, it issues GET of the same blob N no of times and ensures we get the same blob everytime.
 *
 * Caution: We might need to be cautious about OOM errors as this tool holds all the blobs(that were injected) in memory
 * injected until the GETs have reached the threshold for the a particular blob. Its possible to run this tool for thousands
 * of Puts, but ensure that maxGetCountPerBlob is minimum, so that blobs are garbage collected as we continue running the
 * tests
 *
 * Supported features
 * - Parallel Puts and Gets
 * - Concurrent GETs for the same blobs
 *
 * Future work:
 * - Adding delay between concurrent GETs for same blobs
 * - Adding delay for GETs to start so that the file cache will only have recently uploaded blobs(and not all)
 *
 * Example :
 * Router
 *  java -cp ambry.jar com.github.ambry.tools.admin.ConcurrencyTestTool --hardwareLayout [HardwareLayoutFile]
 *  --partitionLayout [PartitionLayoutFile] --putGetHelperFactory com.github.ambry.tools.admin.RouterPutGetHelperFactory
 *  --maxParallelPutCount 2 --parallelGetCount 4 --burstCountForGet 4 --maxGetCountPerBlob 10
 *  --totalPutBlobCount 1000 --maxBlobSizeInBytes 1000 --minBlobSizeInBytes 100 --routerPropsFilePath [Path to router props
 *  file] --deleteAndValidate true
 *
 *  Few other options that might be useful:
 * --sleepTimeBetweenBatchGetsInMs long : Time to sleep in ms between batch Gets
 * --sleepTimeBetweenBatchPutsInMs long : Time to sleep in ms between batch Puts
 * --burstThreadCountForPut int         : No of concurrent PUTs by each producer thread
 */
public class ConcurrencyTestTool {
  private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTestTool.class);
  private static final String ROUTER_PUT_GET_HELPER = "com.github.ambry.tools.admin.RouterPutGetHelperFactory";
  private static final String SERVER_PUT_GET_HELPER = "com.github.ambry.tools.admin.ServerPutGetHelperFactory";
  private static final AtomicBoolean putComplete = new AtomicBoolean(false);
  private static final Random random = new Random();

  public static void main(String args[]) throws Exception {
    InvocationOptions options = new InvocationOptions(args);
    Properties properties = Utils.loadProps(options.routerPropsFilePath);
    ToolUtils.addClusterMapProperties(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            options.hardwareLayoutFilePath, options.partitionLayoutFilePath)).getClusterMap();
    PutGetHelperFactory putGetHelperFactory = null;
    if (options.putGetHelperFactoryStr.equals(ROUTER_PUT_GET_HELPER)) {
      putGetHelperFactory =
          Utils.getObj(options.putGetHelperFactoryStr, properties, clusterMap, options.maxBlobSizeInBytes,
              options.minBlobSizeInBytes);
    } else if (options.putGetHelperFactoryStr.equals(SERVER_PUT_GET_HELPER)) {
      putGetHelperFactory =
          Utils.getObj(options.putGetHelperFactoryStr, properties, options.hostName, options.port, clusterMap,
              options.maxBlobSizeInBytes, options.minBlobSizeInBytes);
    }
    PutGetHelper putGetHelper = putGetHelperFactory.getPutGetHelper();
    ConcurrencyTestTool concurrencyTestTool = new ConcurrencyTestTool();
    concurrencyTestTool.startTest(putGetHelper, options.maxParallelPutCount, options.parallelGetCount,
        options.totalPutBlobCount, options.maxGetCountPerBlob, options.burstCountForGet,
        options.maxFailuresPerPutBatchToStopPuts, options.maxFailuresPerGetBatchToStopGets, options.deleteAndValidate,
        options.sleepTimeBetweenBatchPutsInMs, options.sleepTimeBetweenBatchGetsInMs,
        options.measurementIntervalInSecs);
  }

  /**
   * Initializes and starts the concurrent test tool.
   * @param putGetHelper the {@link PutGetHelper} to be used for PUTs and GETs
   * @param maxParallelPutCount maximum number of parallel puts per phase
   * @param parallelGetCount the number of parallel Gets per phase
   * @param totalPutCount total number of blobs to be uploaded
   * @param maxGetCountPerBlob total number of times a particular blob can be fetched before its deleted
   * @param burstCountGet Burst count for GETs. Each GET consumer thread will fetch a blob these many no of
   *                         times simultaneously
   * @param maxFailuresPerPutBatchToStopPuts Maximum allowable failures per Put batch in order to continue with PUTs
   * @param maxFailuresPerGetBatchToStopGets Maximum allowable failures per Get batch in order to continue with GETs
   * @param deleteAndValidate Deletes the blobs once GETs have reached a threshold on every blob
   * @param sleepTimeBetweenBatchPutsInMs Time to sleep in ms between batch Puts per Producer thread
   * @param sleepTimeBetweenBatchGetsInMs Time to sleep in ms between batch Gets per Consumer thread
   * @param measurementIntervalInSecs The interval in seconds to report performance result
   * @throws InterruptedException
   */
  public void startTest(PutGetHelper putGetHelper, int maxParallelPutCount, int parallelGetCount, int totalPutCount,
      int maxGetCountPerBlob, int burstCountGet, int maxFailuresPerPutBatchToStopPuts,
      int maxFailuresPerGetBatchToStopGets, boolean deleteAndValidate, long sleepTimeBetweenBatchPutsInMs,
      long sleepTimeBetweenBatchGetsInMs, long measurementIntervalInSecs) throws InterruptedException {
    AtomicInteger currentPutCount = new AtomicInteger(0);
    AtomicInteger getCompletedCount = new AtomicInteger(0);
    // Creating shared queue for put Thread and get Thread
    CopyOnWriteArrayList<BlobAccessInfo> generatedBlobAccessInfos = new CopyOnWriteArrayList<BlobAccessInfo>();
    BlockingQueue<String> deletedBlobIds = new LinkedBlockingQueue<>();
    final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    // Creating PutThread
    Thread putThread = new Thread(
        new PutThread(putGetHelper, generatedBlobAccessInfos, totalPutCount, currentPutCount, maxParallelPutCount,
            maxFailuresPerPutBatchToStopPuts, sleepTimeBetweenBatchPutsInMs, threadLocalRandom,
            measurementIntervalInSecs));

    // Creating Get thread
    Thread getThread = new Thread(
        new GetThread(putGetHelper, generatedBlobAccessInfos, maxGetCountPerBlob, currentPutCount, getCompletedCount,
            burstCountGet, parallelGetCount, maxFailuresPerGetBatchToStopGets, sleepTimeBetweenBatchGetsInMs,
            deleteAndValidate, threadLocalRandom, deletedBlobIds, measurementIntervalInSecs));

    Thread deleteThread = null;
    if (deleteAndValidate) {
      logger.trace("Initiating Delete thread ");
      DeleteThread deleteBlobThread =
          new DeleteThread(putGetHelper, totalPutCount, deletedBlobIds, measurementIntervalInSecs);
      deleteThread = new Thread(deleteBlobThread);
      deleteThread.start();
    }

    putThread.start();
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
   * Put threads proceeds in multiple phases until {@code totalPutCount} is reached.
   * During each phase, N random uploads(max of {@code maxParallelRequest} are done to ambry with the help of
   * {@link PutGetHelper}. On completion of one phase, the thread proceeds onto the next phase after sleeping for max of
   * {@code sleepTimeBetweenBatchPutsInMs} ms.
   */
  class PutThread implements Runnable {
    private final PutGetHelper putHelper;
    private final CopyOnWriteArrayList<BlobAccessInfo> generatedBlobAccessInfos;
    private final int totalPutCount;
    private final AtomicInteger currentPutCount;
    private final int maxParallelRequest;
    private final int maxFailuresPerPutBatchToStopPuts;
    private final long sleepTimeBetweenBatchPutsInMs;
    private final ThreadLocalRandom threadLocalRandom;
    private MetricsCollector metricsCollector;

    public PutThread(PutGetHelper putHelper, CopyOnWriteArrayList<BlobAccessInfo> generatedBlobAccessInfos,
        final int totalPutCount, AtomicInteger currentPutCount, int maxParallelRequest,
        int maxFailuresPerPutBatchToStopPuts, long sleepTimeBetweenBatchPutsInMs, ThreadLocalRandom threadLocalRandom,
        long measurementIntervalInSecs) {
      this.putHelper = putHelper;
      this.generatedBlobAccessInfos = generatedBlobAccessInfos;
      this.totalPutCount = totalPutCount;
      this.currentPutCount = currentPutCount;
      this.maxParallelRequest = maxParallelRequest;
      this.maxFailuresPerPutBatchToStopPuts = maxFailuresPerPutBatchToStopPuts;
      this.sleepTimeBetweenBatchPutsInMs = sleepTimeBetweenBatchPutsInMs;
      this.threadLocalRandom = threadLocalRandom;
      this.metricsCollector = new MetricsCollector(measurementIntervalInSecs, "PUTs");
    }

    @Override
    public void run() {
      try {
        while (currentPutCount.get() < totalPutCount) {
          long startTimeInMs = SystemTime.getInstance().milliseconds();
          int burstCount = threadLocalRandom.nextInt(maxParallelRequest) + 1;
          logger.info("PutThread producing " + burstCount + " times ");
          final CountDownLatch countDownLatch = new CountDownLatch(burstCount);
          final AtomicInteger failureCount = new AtomicInteger(0);
          for (int j = 0; j < burstCount; j++) {
            Callback<Pair<String, byte[]>> callback = new Callback<Pair<String, byte[]>>() {

              @Override
              public void onCompletion(Pair<String, byte[]> result, Exception exception) {
                if (exception == null) {
                  if (currentPutCount.get() < totalPutCount) {
                    generatedBlobAccessInfos.add(new BlobAccessInfo(result.getFirst(), result.getSecond()));
                    currentPutCount.incrementAndGet();
                  }
                } else {
                  logger.error("PutBlob failed with ", exception);
                  failureCount.incrementAndGet();
                }
                countDownLatch.countDown();
              }
            };
            putHelper.putBlob(callback, metricsCollector);
          }
          countDownLatch.await(5, TimeUnit.MINUTES);
          metricsCollector.updateTimePassedSoFar(SystemTime.getInstance().milliseconds() - startTimeInMs);
          if (failureCount.get() > maxFailuresPerPutBatchToStopPuts) {
            logger.error(failureCount.get() + "  failures during this batch, hence exiting the Put Thread");
            break;
          }
          Thread.sleep(threadLocalRandom.nextLong(sleepTimeBetweenBatchPutsInMs));
        }
      } catch (InterruptedException ex) {
        logger.error("InterruptedException in putThread ", ex);
      } finally {
        putComplete.set(true);
        metricsCollector.reportMetrics();
      }
      logger.info(
          "Exiting Producer. Current Put Count " + currentPutCount.get() + ", total Put Count " + totalPutCount);
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
   * Get thread also proceeds in multiple phases until {@code totalPutCount} number of blobs have been fetched and
   * verified. Blobs and its metadata information are fetched from {@code generatedBlobAccessInfos}. On each phase,
   * the get thread fetches {@code parallelGetCount} number of blobs from the {@code generatedBlobAccessInfos} and
   * issues random (max of {@code maxBurstCountPerblob} number of burst GET calls(and validates) to Ambry via
   * {@link PutGetHelper#getBlobAndValidate(String, byte[], Object, Callback, MetricsCollector)}. Each blob is fetched for a maximum of
   * {@code maxGetCountPerBlob} times. On reaching the threshold, they are removed from the
   * {@code generatedBlobAccessInfos} and deletion is called via
   * {@link PutGetHelper#deleteBlobAndValidate(String, Object, Callback, MetricsCollector)} if {@code deleteOnExit} is set. On completion
   * of one phase, the thread proceeds onto the next phase after sleeping for max of {@code sleepTimeBetweenBatchGetsInMs} ms.
   */
  class GetThread implements Runnable {
    private final PutGetHelper getHelper;
    private final CopyOnWriteArrayList<BlobAccessInfo> generatedBlobAccessInfos;
    private final int maxGetCountPerBlob;
    private final AtomicInteger getCompletedCount;
    private final AtomicInteger totalPutCount;
    private final int maxBurstCountPerblob;
    private final int parallelGetCount;
    private final boolean deleteOnExit;
    private final int maxFailuresPerGetBatch;
    private final long sleepTimeBetweenBatchGetsInMs;
    private final ThreadLocalRandom threadLocalRandom;
    private final BlockingQueue<String> deleteBlobIds;
    private MetricsCollector metricsCollector;

    public GetThread(PutGetHelper getHelper, CopyOnWriteArrayList<BlobAccessInfo> generatedBlobAccessInfos,
        int maxGetCountPerBlob, final AtomicInteger totalPutCount, AtomicInteger getCompletedCount,
        int maxBurstCountPerBlob, int parallelGetCount, int maxFailuresPerGetBatch, long sleepTimeBetweenBatchGetsInMs,
        boolean deleteOnExit, ThreadLocalRandom threadLocalRandom, BlockingQueue<String> deleteBlobIds,
        long measurementIntervalInSecs) {
      this.getHelper = getHelper;
      this.generatedBlobAccessInfos = generatedBlobAccessInfos;
      this.maxGetCountPerBlob = maxGetCountPerBlob;
      this.totalPutCount = totalPutCount;
      this.parallelGetCount = parallelGetCount;
      this.getCompletedCount = getCompletedCount;
      this.maxBurstCountPerblob = maxBurstCountPerBlob;
      this.deleteOnExit = deleteOnExit;
      this.maxFailuresPerGetBatch = maxFailuresPerGetBatch;
      this.sleepTimeBetweenBatchGetsInMs = sleepTimeBetweenBatchGetsInMs;
      this.threadLocalRandom = threadLocalRandom;
      this.deleteBlobIds = deleteBlobIds;
      this.metricsCollector = new MetricsCollector(measurementIntervalInSecs, "GETs");
    }

    @Override
    public void run() {
      while (!putComplete.get() || getCompletedCount.get() < totalPutCount.get()) {
        try {
          int size = generatedBlobAccessInfos.size();
          if (size > 0) {
            long startTimeInMs = SystemTime.getInstance().milliseconds();
            List<Pair<BlobAccessInfo, Integer>> blobInfoBurstCountPairList = new ArrayList<>();
            List<String> toBeDeletedBlobsPerBatch = new ArrayList<>();
            int blobCountPerBatch = Math.min(size, parallelGetCount);
            for (int i = 0; i < blobCountPerBatch; i++) {
              size = generatedBlobAccessInfos.size();
              BlobAccessInfo blobAccessInfo = generatedBlobAccessInfos.get(threadLocalRandom.nextInt(size));
              if (blobAccessInfo != null) {
                int burstCount = threadLocalRandom.nextInt(maxBurstCountPerblob) + 1;
                blobInfoBurstCountPairList.add(new Pair(blobAccessInfo, burstCount));
                blobAccessInfo.referenceCount.addAndGet(burstCount);
                if (blobAccessInfo.referenceCount.get() >= maxGetCountPerBlob) {
                  generatedBlobAccessInfos.remove(blobAccessInfo);
                  getCompletedCount.incrementAndGet();
                  toBeDeletedBlobsPerBatch.add(blobAccessInfo.blobId);
                }
              }
            }

            logger.info("Get thread fetching " + blobCountPerBatch + " blobs for this phase ");
            final CountDownLatch countDownLatchForBatch = new CountDownLatch(blobCountPerBatch);
            final AtomicInteger failureCount = new AtomicInteger(0);
            for (Pair<BlobAccessInfo, Integer> blobAccessInfoBurstCountPair : blobInfoBurstCountPairList) {
              int burstCount = blobAccessInfoBurstCountPair.getSecond();
              final BlobAccessInfo blobAccessInfo = blobAccessInfoBurstCountPair.getFirst();
              logger.trace("Get Thread fetching " + blobAccessInfo.blobId + " " + burstCount + " times ");
              final CountDownLatch countDownLatch = new CountDownLatch(burstCount);
              for (int j = 0; j < burstCount; j++) {
                Callback<Void> callback = new Callback<Void>() {

                  @Override
                  public void onCompletion(Void result, Exception exception) {
                    countDownLatch.countDown();
                    blobAccessInfo.burstGetReferenceCount.decrementAndGet();
                    if (countDownLatch.getCount() == 0) {
                      countDownLatchForBatch.countDown();
                    }
                    if (exception != null) {
                      logger.error("Get and validation of " + blobAccessInfo.blobId + " failed with ", exception);
                      failureCount.incrementAndGet();
                    }
                  }
                };
                blobAccessInfo.burstGetReferenceCount.incrementAndGet();
                getHelper.getBlobAndValidate(blobAccessInfo.blobId, blobAccessInfo.blobContent,
                    getHelper.getErrorCodeForNoError(), callback, metricsCollector);
              }
            }
            countDownLatchForBatch.await(5, TimeUnit.MINUTES);
            metricsCollector.updateTimePassedSoFar(SystemTime.getInstance().milliseconds() - startTimeInMs);
            if (deleteOnExit) {
              deleteBlobIds.addAll(toBeDeletedBlobsPerBatch);
            }
            if (failureCount.get() > maxFailuresPerGetBatch) {
              logger.error(failureCount.get() + "  failures during this batch, hence exiting the GetThread");
              break;
            }
          }
          Thread.sleep(threadLocalRandom.nextLong(sleepTimeBetweenBatchGetsInMs));
        } catch (InterruptedException ex) {
          logger.error("InterruptedException in GetThread ", ex);
        }
      }
      metricsCollector.reportMetrics();
      logger.info("Exiting GetThread ");
    }
  }

  /**
   * Thread used for deleting blobs from ambry
   */
  class DeleteThread implements Runnable {
    private final PutGetHelper deleteHelper;
    private final int putCount;
    private AtomicInteger deletedCount = new AtomicInteger(0);
    private BlockingQueue<String> deleteBlobIds;
    private CountDownLatch countDownLatch;
    private MetricsCollector metricsCollector;

    public DeleteThread(PutGetHelper deleteHelper, int putCount, BlockingQueue<String> deleteBlobIds,
        long measurementIntervalInSecs) {
      this.putCount = putCount;
      this.deleteHelper = deleteHelper;
      this.deleteBlobIds = deleteBlobIds;
      this.countDownLatch = new CountDownLatch(putCount);
      this.metricsCollector = new MetricsCollector(measurementIntervalInSecs, "DELETEs");
    }

    public void run() {
      while (deletedCount.get() < putCount) {
        try {
          final String blobIdStr = deleteBlobIds.poll(50, TimeUnit.MILLISECONDS);
          if (blobIdStr != null) {
            final long startTimeInMs = SystemTime.getInstance().milliseconds();
            deletedCount.incrementAndGet();
            logger.trace("Deleting blob " + blobIdStr);
            Callback<Void> callback = new Callback<Void>() {

              @Override
              public void onCompletion(Void result, Exception exception) {
                metricsCollector.updateTimePassedSoFar(SystemTime.getInstance().milliseconds() - startTimeInMs);
                logger.trace("Deletion completed for " + blobIdStr);
                countDownLatch.countDown();
                if (exception != null) {
                  logger.error("Deletion of " + blobIdStr + " failed with ", exception);
                }
              }
            };
            deleteHelper.deleteBlobAndValidate(blobIdStr, deleteHelper.getErrorCodeForNoError(), callback,
                metricsCollector);
          }
        } catch (InterruptedException e) {
          logger.error("Delete operation interrupted", e);
        }
      }
      try {
        countDownLatch.await();
        metricsCollector.reportMetrics();
      } catch (InterruptedException e) {
        logger.error("Waiting for delete operations to complete interrupted", e);
      }
    }
  }

  /**
   * An interface to assist in uploading blobs to ambry, fetching and deleting the same from ambry
   */
  interface PutGetHelper<T> {
    /**
     * Request to upload a blob to ambry
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     * @return a {@link FutureResult} which will contain the result eventually
     */
    Future<T> putBlob(Callback<T> callback, MetricsCollector metricsCollector);

    /**
     * Request to fetch the blob pertaining to the {@code blobId} passed in and verify for its content
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param blobContent the content of the blob that needs to be verified against
     * @param expectedErrorCode the expected error code for the Get call
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     * @return the {@link FutureResult} that will contain the result eventually
     */
    Future<T> getBlobAndValidate(String blobId, byte[] blobContent, T expectedErrorCode, Callback<T> callback,
        MetricsCollector metricsCollector);

    /**
     * Deletes a blob from ambry pertaining to the {@code blobId} passed in and verifies that subsequent Get fails
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param expectedErrorCode the expected error code for the Delete call
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     * @return the {@link FutureResult} that will contain the result eventually
     */
    Future<T> deleteBlobAndValidate(String blobId, T expectedErrorCode, Callback<T> callback,
        MetricsCollector metricsCollector);

    /**
     * Returns the default Error code that is expected for a Get or a Delete call
     * @return the default error code that is expected for a Get or a Delete call
     */
    T getErrorCodeForNoError();

    void shutdown() throws InterruptedException;
  }

  /**
   * Class to assist in collecting and reporting metrics
   */
  class MetricsCollector {
    private String operation;
    private final long measurementIntervalInSecs;
    private ArrayList<Long> requestLatencies;
    private AtomicLong timePassedInMs = new AtomicLong(0);
    private AtomicLong numberOfOperations = new AtomicLong(0);
    private AtomicLong maxLatencyInMs = new AtomicLong(0);
    private AtomicLong minLatencyInMs = new AtomicLong(Long.MAX_VALUE);
    private AtomicLong totalLatencyInMs = new AtomicLong(0);

    MetricsCollector(long measurementIntervalInSecs, String operation) {
      this.measurementIntervalInSecs = measurementIntervalInSecs;
      requestLatencies = new ArrayList<>();
      this.operation = operation;
    }

    /**
     * Updates the wall clock time spent so far. This is used for reporting purpose.
     * For every {@code measurementIntervalInSecs}, we report the metrics and clear the stats.
     * @param timePassedInMs the time spent so far in Millisecs
     */
    synchronized void updateTimePassedSoFar(long timePassedInMs) {
      this.timePassedInMs.addAndGet(timePassedInMs);
      if (this.timePassedInMs.get() >= measurementIntervalInSecs * SystemTime.MsPerSec) {
        reportMetrics();
        numberOfOperations.set(0);
        this.timePassedInMs.set(0);
        requestLatencies.clear();
        maxLatencyInMs.set(0);
        minLatencyInMs.set(Long.MAX_VALUE);
        totalLatencyInMs.set(0);
      }
    }

    /**
     * Reports the stats for latencies like avg, 95th, 99th, min and max
     */
    void reportMetrics() {
      if (requestLatencies.size() > 5) {
        Collections.sort(requestLatencies);
        int index99 = (int) (requestLatencies.size() * 0.99) - 1;
        int index95 = (int) (requestLatencies.size() * 0.95) - 1;
        StringBuilder message = new StringBuilder();
        message.append(
            "==================================================================================================\n");
        message.append(
            operation + ":totalOps=" + numberOfOperations + ", 99thInMs=" + (double) requestLatencies.get(index99)
                + ", 95thInMs=" + (double) requestLatencies.get(index95) + ", AvgInMs=" + (
                ((double) totalLatencyInMs.get()) / numberOfOperations.get()) + ", minInMs=" + minLatencyInMs.get()
                + ", maxInMs=" + (maxLatencyInMs.get()) + "\n");
        message.append(
            "==================================================================================================\n");
        logger.info(message.toString());
      }
    }

    /**
     * Updates the latency for the request
     * @param requestLatency latency taken by the request
     */
    void updateLatency(long requestLatency) {
      numberOfOperations.incrementAndGet();
      requestLatencies.add(requestLatency);
      if (maxLatencyInMs.get() < requestLatency) {
        maxLatencyInMs.set(requestLatency);
      }
      if (minLatencyInMs.get() > requestLatency) {
        minLatencyInMs.set(requestLatency);
      }
      totalLatencyInMs.addAndGet(requestLatency);
    }
  }

  /**
   * An implementation of {@link PutGetHelper} for an Ambry Router
   */
  static class RouterPutGetHelper implements PutGetHelper<RouterErrorCode> {
    private final int maxBlobSize;
    private final int minBlobSize;
    private final Router router;
    private final ThreadLocalRandom localRandom;

    public RouterPutGetHelper(Properties properties, ClusterMap clusterMap, String routerFactoryClass, int maxBlobSize,
        int minBlobSize) throws Exception {
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      RouterFactory routerFactory =
          Utils.getObj(routerFactoryClass, verifiableProperties, clusterMap, new LoggingNotificationSystem());
      router = routerFactory.getRouter();
      this.maxBlobSize = maxBlobSize;
      this.minBlobSize = minBlobSize;
      this.localRandom = ThreadLocalRandom.current();
    }

    /**
     * {@inheritDoc}
     * Uploads a blob to the server directly and returns the blobId along with the content
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     * @return a {@link Future} which will contain the result eventually
     */
    @Override
    public Future putBlob(final Callback callback, final MetricsCollector metricsCollector) {
      int randomNum = localRandom.nextInt((maxBlobSize - minBlobSize) + 1) + minBlobSize;
      final byte[] blob = new byte[randomNum];
      byte[] usermetadata = new byte[random.nextInt(1024)];
      BlobProperties props =
          new BlobProperties(randomNum, "test", Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false);
      final FutureResult futureResult = new FutureResult();
      try {
        final long startTimeInMs = SystemTime.getInstance().milliseconds();
        ByteBufferReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob));
        router.putBlob(props, usermetadata, putChannel, new PutBlobOptionsBuilder().build(), new Callback<String>() {
          @Override
          public void onCompletion(String result, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().milliseconds() - startTimeInMs;
            metricsCollector.updateLatency(latencyPerBlob);
            logger.trace(" Time taken to put blob id " + result + " in ms " + latencyPerBlob + " for blob of size "
                + blob.length);
            Exception exceptionToReturn = null;
            Pair<String, byte[]> toReturn = null;
            if (result != null) {
              toReturn = new Pair(result, blob);
            } else {
              exceptionToReturn = exception;
            }
            futureResult.done(toReturn, exceptionToReturn);
            if (callback != null) {
              callback.onCompletion(toReturn, exceptionToReturn);
            }
          }
        });
      } catch (Exception e) {
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
      return futureResult;
    }

    /**
     * {@inheritDoc}
     * Request to fetch the blob from the server directly pertaining to the {@code blobId} passed in and verify for
     * its content
     * @param blobIdStr the (string representation of the) blobId of that blob that needs to be fetched
     * @param blobContent the content of the blob that needs to be verified against
     * @param expectedErrorCode the expected error code for the Get call
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     */
    @Override
    public Future getBlobAndValidate(final String blobIdStr, final byte[] blobContent,
        final RouterErrorCode expectedErrorCode, final Callback callback, final MetricsCollector metricsCollector) {
      final FutureResult futureResult = new FutureResult();
      try {
        final Long startTimeGetBlobMs = SystemTime.getInstance().milliseconds();
        router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), new Callback<GetBlobResult>() {
          @Override
          public void onCompletion(GetBlobResult getBlobResult, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().milliseconds() - startTimeGetBlobMs;
            logger.trace("Time taken to get blob " + blobIdStr + " in ms " + latencyPerBlob);
            Exception exceptionToReturn = null;
            if (getBlobResult != null) {
              logger.trace("OnCompletion returned for blob " + blobIdStr + " with non null result ");
              Thread compareThread = new Thread(
                  new CompareRunnable(blobIdStr, blobContent, getBlobResult.getBlobDataChannel(), futureResult,
                      callback, metricsCollector, startTimeGetBlobMs));
              Utils.newThread(compareThread, true).start();
            } else {
              logger.trace("OnCompletion returned for blob " + blobIdStr + " with exception ");
              if (exception instanceof RouterException) {
                RouterException routerException = (RouterException) exception;
                if (expectedErrorCode != null) {
                  if (!expectedErrorCode.equals(routerException.getErrorCode())) {
                    logger.error("Error code mismatch. Expected " + expectedErrorCode + ", actual "
                        + routerException.getErrorCode());
                    exceptionToReturn = exception;
                  }
                } else {
                  logger.trace("RouterException thrown with error code " + routerException.getErrorCode() + " "
                      + "when no exception was expected for " + blobIdStr);
                  exceptionToReturn = exception;
                }
              } else {
                logger.trace("UnknownException thrown for " + blobIdStr);
                exceptionToReturn = exception;
              }
              futureResult.done(null, exceptionToReturn);
              if (callback != null) {
                callback.onCompletion(null, exceptionToReturn);
              }
            }
          }
        });
      } catch (Exception e) {
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
      return futureResult;
    }

    /**
     * Class to compare the getBlob output
     */
    class CompareRunnable implements Runnable {
      private final String blobId;
      private final byte[] putContent;
      private final ReadableStreamChannel readableStreamChannel;
      private final FutureResult futureResult;
      private final Callback callback;
      private final MetricsCollector metricsCollector;
      private final long startTimeInMs;

      CompareRunnable(String blobId, byte[] putContent, ReadableStreamChannel readableStreamChannel,
          FutureResult futureResult, Callback callback, MetricsCollector metricsCollector, long startTimeInMs) {
        this.blobId = blobId;
        this.putContent = putContent;
        this.readableStreamChannel = readableStreamChannel;
        this.futureResult = futureResult;
        this.callback = callback;
        this.metricsCollector = metricsCollector;
        this.startTimeInMs = startTimeInMs;
      }

      public void run() {
        ByteBuffer putContentBuf = ByteBuffer.wrap(putContent);
        ByteBufferAsyncWritableChannel getChannel = new ByteBufferAsyncWritableChannel();
        Future<Long> readIntoFuture = readableStreamChannel.readInto(getChannel, null);
        final int bytesToRead = putContentBuf.remaining();
        int readBytes = 0;
        Exception exceptionToReturn = null;
        try {
          do {
            ByteBuffer buf = null;
            buf = getChannel.getNextChunk();
            int bufLength = buf.remaining();
            if (readBytes + bufLength > bytesToRead) {
              throw new IllegalStateException(
                  "total content read" + (readBytes + bufLength) + " should not be greater than "
                      + "length of put content " + bytesToRead + " for " + blobId);
            }
            while (buf.hasRemaining()) {
              if (putContentBuf.get() != buf.get()) {
                exceptionToReturn = new IllegalStateException("Get and Put blob content mis-match for " + blobId);
              }
              readBytes++;
            }
            getChannel.resolveOldestChunk(null);
          } while (readBytes < bytesToRead);
          if (readBytes != readIntoFuture.get()) {
            throw new IllegalStateException(
                "The returned length in the future " + readIntoFuture.get() + " didn't match the "
                    + "length of data written " + readBytes + " for " + blobId);
          }
          if (getChannel.getNextChunk(0) != null) {
            throw new IllegalStateException(
                "More data found in the channel after reading all the required bytes for " + blobId);
          }
          readIntoFuture.get();
        } catch (InterruptedException e) {
          exceptionToReturn = e;
        } catch (ExecutionException e) {
          exceptionToReturn = e;
        } catch (IllegalStateException e) {
          exceptionToReturn = e;
        } finally {
          if (metricsCollector != null) {
            metricsCollector.updateLatency(SystemTime.getInstance().milliseconds() - startTimeInMs);
          }
          futureResult.done(null, exceptionToReturn);
          if (callback != null) {
            callback.onCompletion(null, exceptionToReturn);
          }
        }
      }
    }

    /**
     * {@inheritDoc}
     * Request to delete the blob from the server directly pertaining to the {@code blobId} passed in and verify that
     * the subsequent get fails with {@link RouterErrorCode#BlobDeleted}
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param expectedErrorCode the expected error code for the Delete call
     * @param callback the {@link Callback} that will be called on completion
     * @param metricsCollector the {@link MetricsCollector} to collect and report metrics
     */
    @Override
    public Future deleteBlobAndValidate(final String blobId, final RouterErrorCode expectedErrorCode,
        final Callback callback, final MetricsCollector metricsCollector) {
      final FutureResult futureResult = new FutureResult();
      try {
        final Long startTimeGetBlobInMs = SystemTime.getInstance().milliseconds();
        router.deleteBlob(blobId, null, new Callback<Void>() {
          @Override
          public void onCompletion(Void result, Exception exception) {
            long latencyPerBlob = SystemTime.getInstance().milliseconds() - startTimeGetBlobInMs;
            metricsCollector.updateLatency(latencyPerBlob);
            logger.trace(" Time taken to delete blob " + blobId + " in ms " + latencyPerBlob);
            final AtomicReference<Exception> exceptionToReturn = new AtomicReference<>();
            if (exception == null) {
              logger.trace("Deletion of " + blobId + " succeeded. Issuing Get to confirm the deletion ");
              getBlobAndValidate(blobId, null, RouterErrorCode.BlobDeleted, new Callback<GetBlobResult>() {
                @Override
                public void onCompletion(GetBlobResult getBlobResult, Exception exception) {
                  if (getBlobResult != null) {
                    exceptionToReturn.set(new IllegalStateException(
                        "Get of a deleted blob " + blobId + " should have failed with " + RouterErrorCode.BlobDeleted));
                  } else {
                    if (exception != null) {
                      exceptionToReturn.set(exception);
                    }
                  }
                }
              }, null);
            } else {
              if (exception instanceof RouterException) {
                RouterException routerException = (RouterException) exception;
                if (expectedErrorCode != null) {
                  if (!expectedErrorCode.equals(routerException.getErrorCode())) {
                    exceptionToReturn.set(routerException);
                  }
                } else {
                  exceptionToReturn.set(routerException);
                }
              } else {
                exceptionToReturn.set(
                    new IllegalStateException("UnknownException thrown for deletion of " + blobId, exception));
              }
            }
            futureResult.done(null, exceptionToReturn.get());
            if (callback != null) {
              callback.onCompletion(null, exceptionToReturn.get());
            }
          }
        });
      } catch (Exception e) {
        futureResult.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
      }
      return futureResult;
    }

    /**
     * {@inheritDoc}
     * Returns the default Error code that is expected for a Get call or a delete call
     * @return the default error code that is expected for a Get call or a delete call
     */
    @Override
    public RouterErrorCode getErrorCodeForNoError() {
      return null;
    }

    /**
     * {@inheritDoc}
     * Request to clean up resources used by this helper class, i.e. NetworkClient in this context
     */
    @Override
    public void shutdown() throws InterruptedException {
      try {
        long startTime = System.currentTimeMillis();
        router.close();
        logger.info("Router shutdown took " + (System.currentTimeMillis() - startTime) + " ms");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static class InvocationOptions {
    public final String hardwareLayoutFilePath;
    public final String partitionLayoutFilePath;
    public final String routerPropsFilePath;
    public final int maxParallelPutCount;
    public final int parallelGetCount;
    public final int burstCountForGet;
    public final int totalPutBlobCount;
    public final int maxGetCountPerBlob;
    public final int maxBlobSizeInBytes;
    public final int minBlobSizeInBytes;
    public final int maxFailuresPerPutBatchToStopPuts;
    public final int maxFailuresPerGetBatchToStopGets;
    public final long sleepTimeBetweenBatchPutsInMs;
    public final long sleepTimeBetweenBatchGetsInMs;

    public final String hostName;
    public final int port;
    public final String putGetHelperFactoryStr;
    public final boolean deleteAndValidate;
    public final long measurementIntervalInSecs;
    public final boolean enabledVerboseLogging;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args the command line argument list.
     * @throws InstantiationException if all required arguments were not provided.
     * @throws IOException if help text could not be printed.
     */
    public InvocationOptions(String args[]) throws InstantiationException, IOException {
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
      ArgumentAcceptingOptionSpec<String> routerPropsFilePathOpt =
          parser.accepts("routerPropsFilePath", "Path to router properties file")
              .withRequiredArg()
              .describedAs("routerPropsFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> putGetHelperFactoryOpt =
          parser.accepts("putGetHelperFactory", "The reference to PutGetHelperFactory class")
              .withOptionalArg()
              .defaultsTo("com.github.ambry.tools.admin.RouterPutGetHelperFactory")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<Integer> maxParallelPutCountOpt =
          parser.accepts("maxParallelPutCount", "Maximum number of parallel puts")
              .withOptionalArg()
              .describedAs("maxParallelPutCount")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> parallelGetCountOpt =
          parser.accepts("parallelGetCount", "Total number of parallel gets")
              .withOptionalArg()
              .describedAs("parallelGetCount")
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
      ArgumentAcceptingOptionSpec<Integer> maxFailuresPerPutBatchToStopPutsOpt =
          parser.accepts("maxFailuresPerPutBatchToStopPuts", "Maximum failures per Put batch to stop PUTs")
              .withOptionalArg()
              .describedAs("maxFailuresPerPutBatchToStopPuts")
              .ofType(Integer.class)
              .defaultsTo(2);
      ArgumentAcceptingOptionSpec<Integer> maxFailuresPerGetBatchToStopGetsOpt =
          parser.accepts("maxFailuresPerGetBatchToStopGets", "Maximum failures per Get batch to stop GETs")
              .withOptionalArg()
              .describedAs("maxFailuresPerGetBatchToStopGets")
              .ofType(Integer.class)
              .defaultsTo(2);
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
      ArgumentAcceptingOptionSpec<Boolean> deleteAndValidateOpt =
          parser.accepts("deleteAndValidate", "Delete blobs once Get threshold is reached")
              .withOptionalArg()
              .describedAs("deleteAndValidate")
              .ofType(Boolean.class)
              .defaultsTo(true);
      ArgumentAcceptingOptionSpec<Long> measurementIntervalOpt =
          parser.accepts("measurementIntervalInSecs", "The interval in seconds to report performance result")
              .withOptionalArg()
              .describedAs("The CPU time spent for putting/getting/deleting blobs, not wall time")
              .ofType(Long.class)
              .defaultsTo(60L);
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
      this.routerPropsFilePath = options.valueOf(routerPropsFilePathOpt);
      this.maxParallelPutCount = options.valueOf(maxParallelPutCountOpt);
      this.parallelGetCount = options.valueOf(parallelGetCountOpt);
      this.burstCountForGet = options.valueOf(burstCountForGetOpt);
      this.totalPutBlobCount = options.valueOf(totalPutBlobCountOpt);
      this.maxGetCountPerBlob = options.valueOf(maxGetCountPerBlobOpt);
      this.maxBlobSizeInBytes = options.valueOf(maxBlobSizeInBytesOpt);
      this.minBlobSizeInBytes = options.valueOf(minBlobSizeInBytesOpt);
      this.maxFailuresPerPutBatchToStopPuts = options.valueOf(maxFailuresPerPutBatchToStopPutsOpt);
      this.maxFailuresPerGetBatchToStopGets = options.valueOf(maxFailuresPerGetBatchToStopGetsOpt);
      this.sleepTimeBetweenBatchPutsInMs = options.valueOf(sleepTimeBetweenBatchPutsInMsOpt);
      this.sleepTimeBetweenBatchGetsInMs = options.valueOf(sleepTimeBetweenBatchGetsInMsOpt);
      this.hostName = options.valueOf(hostNameOpt);
      this.port = options.valueOf(portNumberOpt);
      this.putGetHelperFactoryStr = options.valueOf(putGetHelperFactoryOpt);
      this.deleteAndValidate = options.valueOf(deleteAndValidateOpt);
      this.measurementIntervalInSecs = options.valueOf(measurementIntervalOpt);
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
