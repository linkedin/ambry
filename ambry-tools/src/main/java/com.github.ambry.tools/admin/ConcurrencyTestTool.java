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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
 * Caution: We might need to be cautious about OOM erorrs as this tool holds all the blobs(that were injected) in memory
 * injected until the GETs have reached the threshold for the a particular blob.
 *
 * Supported features
 * - Multiple producers and consumers
 * - Concurrent GETs for the same blobs
 * - This also means that we will get occasional bursts of GETs
 * - Apart from having N no of producer and consumer threads, each thread is capable of generating occasional bursts of
 * requests(including PUTs)
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
 *  --producerThreadCount 2 --getConsumerThreadCount 4 --burstThreadCountForGet 4 --maxGetCountPerBlob 10
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

  static String outFile;
  static FileWriter fileWriter;

  public static void main(String args[])
      throws Exception {
    InvocationOptions options = new InvocationOptions(args);
    ClusterMap clusterMap = new ClusterMapManager(options.hardwareLayoutFilePath, options.partitionLayoutFilePath,
        new ClusterMapConfig(new VerifiableProperties(new Properties())));

    PutGetHelperFactory putGetHelperFactory =
        Utils.getObj(options.putGetHelperFactoryStr, new Properties(), options.hostName, options.port, clusterMap,
            options.maxBlobSizeInBytes, options.minBlobSizeInBytes, options.enabledVerboseLogging);

    ConcurrencyTestTool.setOutFile(options.outFile);
    PutGetHelper putGetHelper = putGetHelperFactory.getPutGetHelper();
    ConcurrencyTestTool concurrencyTestTool = new ConcurrencyTestTool();
    concurrencyTestTool.startTest(putGetHelper, options.producerThreadCount, options.getConsumerThreadCount,
        options.totalPutBlobCount, options.maxGetCountPerBlob, options.burstThreadCountForPut,
        options.burstThreadCountForGet, options.deleteOnExit, options.enabledVerboseLogging,
        options.sleepTimeBetweenBatchPutsInMs, options.sleepTimeBetweenBatchGetsInMs);
    concurrencyTestTool.shutdown();
  }

  /**
   * Sets the output file to redirect the output to
   * @param outFile
   */
  static void setOutFile(String outFile) {
    try {
      if (outFile != null) {
        ConcurrencyTestTool.outFile = outFile;
        ConcurrencyTestTool.fileWriter = new FileWriter(new File(outFile));
      }
    } catch (java.io.IOException IOException) {
      System.out.println("IOException while trying to create File " + outFile);
    }
  }

  /**
   * Log a message to the standard out or to the file as per configs
   * @param msg the message that needs to be logged
   */
  static synchronized void logOutput(String msg) {
    try {
      if (fileWriter == null) {
        System.out.println(msg);
      } else {
        fileWriter.write(msg + "\n");
      }
    } catch (IOException e) {
      System.out.println("IOException while trying to write to File");
    }
  }

  /**
   * Flushes and closes the outfile if need be
   */
  void shutdown() {
    try {
      if (fileWriter != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to close File " + outFile);
    }
  }

  /**
   * Initializes and starts the concurrent test tool.
   * @param putGetHelper the {@link PutGetHelper} to be used for PUTs and GETs
   * @param putThreadCount the number of producer threads to be used for uploads
   * @param getThreadCount the number of consumer threads to be used for GETs
   * @param maxPutCount total number of blobs to be uploaded
   * @param maxGetCountPerBlob total number of times a particular blob can be fetched before its deleted
   * @param maxBurstCountPut Maximum burst count for PUTs. Each producer thread will generate at max this many no of
   *                         uploads
   * @param maxBurstCountGet Maximum burst count for GETs. Each GET consumer thread will fetch a blob these many no of
   *                         times
   * @param deleteOnExit Deletes the blobs once GETs have reached a threshold on every blob
   * @param enableVerboseLogging Enables verbose logging
   * @param sleepTimeBetweenBatchPutsInMs Time to sleep in ms between batch Puts per Producer thread
   * @param sleepTimeBetweenBatchGetsInMs Time to sleep in ms between batch Gets per Consumer thread
   * @throws InterruptedException
   */
  public void startTest(PutGetHelper putGetHelper, int putThreadCount, int getThreadCount, int maxPutCount,
      int maxGetCountPerBlob, int maxBurstCountPut, int maxBurstCountGet, boolean deleteOnExit,
      boolean enableVerboseLogging, long sleepTimeBetweenBatchPutsInMs, long sleepTimeBetweenBatchGetsInMs)
      throws InterruptedException {

    AtomicInteger currentPutCount = new AtomicInteger(0);
    AtomicInteger getCompletedCount = new AtomicInteger(0);
    // Creating shared queue for producer and consumer
    ArrayList<BlobAccessInfo> sharedQueue = new ArrayList<>();
    ArrayList<Thread> deleteThreads = new ArrayList<>();

    final Lock lock = new ReentrantLock();
    final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    // Creating PutManager threads
    List<Thread> putManagerThreads = new ArrayList<>();
    for (int i = 0; i < putThreadCount; i++) {
      putManagerThreads.add(new Thread(
          new PutManager("Producer_" + i, putGetHelper, sharedQueue, maxPutCount, currentPutCount, lock,
              maxBurstCountPut, sleepTimeBetweenBatchPutsInMs, enableVerboseLogging, threadLocalRandom)));
    }
    // Creating GetConsumer threads
    List<Thread> getManagerThreads = new ArrayList<>();
    for (int i = 0; i < getThreadCount; i++) {
      getManagerThreads.add(new Thread(
          new GetManager("Consumer_" + i, putGetHelper, sharedQueue, maxGetCountPerBlob, lock, maxPutCount,
              getCompletedCount, maxBurstCountGet, sleepTimeBetweenBatchGetsInMs, deleteOnExit, enableVerboseLogging,
              threadLocalRandom, deleteThreads)));
    }

    // Starting PutManager and GetManager thread
    for (Thread putManagerThread : putManagerThreads) {
      putManagerThread.start();
    }
    for (Thread getManagerThread : getManagerThreads) {
      getManagerThread.start();
    }
    // Waiting for PutManager and GetManager threads to complete
    for (Thread putManagerThread : putManagerThreads) {
      putManagerThread.join();
    }
    for (Thread getManagerThread : getManagerThreads) {
      getManagerThread.join();
    }
    // Waiting for delete threads to complete
    for (Thread deleteThread : deleteThreads) {
      deleteThread.join();
    }

    // shutting down the PutGetHelper class to release any resources held up
    putGetHelper.shutdown();
  }

  /**
   * Put Manager threads to assist in producing blobs or uploading blobs to ambry
   */
  class PutManager implements Runnable {
    private final String threadName;
    private final PutGetHelper producerJob;
    private final ArrayList<BlobAccessInfo> blobInfos;
    private final int maxPutCount;
    private final AtomicInteger currentPutCount;
    private final Lock lock;
    private final int maxBurstCount;
    private final long sleepTimeBetweenBatchPutsInMs;
    private final boolean enableVerboseLogging;
    private final ThreadLocalRandom threadLocalRandom;

    public PutManager(String threadName, PutGetHelper producerJob, ArrayList<BlobAccessInfo> blobInfos,
        final int maxPutCount, AtomicInteger currentPutCount, Lock lock, int maxBurstCount,
        long sleepTimeBetweenBatchPutsInMs, boolean enableVerboseLogging, ThreadLocalRandom threadLocalRandom) {
      this.threadName = threadName;
      this.producerJob = producerJob;
      this.blobInfos = blobInfos;
      this.maxPutCount = maxPutCount;
      this.currentPutCount = currentPutCount;
      this.lock = lock;
      this.maxBurstCount = maxBurstCount;
      this.sleepTimeBetweenBatchPutsInMs = sleepTimeBetweenBatchPutsInMs;
      this.enableVerboseLogging = enableVerboseLogging;
      this.threadLocalRandom = threadLocalRandom;
    }

    @Override
    public void run() {
      while (currentPutCount.get() < maxPutCount) {
        try {
          List<Thread> putThreads = new ArrayList<>();
          int burstCount = threadLocalRandom.nextInt(maxBurstCount) + 1;
          if (enableVerboseLogging) {
            logOutput(threadName + " Producing " + burstCount + " times ");
          }
          for (int j = 0; j < burstCount; j++) {
            putThreads.add(new Thread(
                new PutBlobThread(threadName + "_" + j, producerJob, blobInfos, lock, maxPutCount, currentPutCount,
                    enableVerboseLogging)));
          }
          for (int j = 0; j < putThreads.size(); j++) {
            putThreads.get(j).start();
          }
          for (int j = 0; j < putThreads.size(); j++) {
            putThreads.get(j).join();
          }
          Thread.sleep(sleepTimeBetweenBatchPutsInMs);
        } catch (InterruptedException ex) {
          logOutput("InterruptedException in " + threadName + " " + ex.getStackTrace());
        }
      }
      logOutput("Exiting Producer " + threadName + ": current Put Count " + currentPutCount.get() + ", max Put Count "
          + maxPutCount);
    }
  }

  /**
   * Record to hold information about a blob along with its content and its reference count
   */
  class BlobAccessInfo {
    String blobId;
    AtomicInteger referenceCount;
    byte[] blobContent;

    public BlobAccessInfo(String blobId, byte[] blobContent) {
      this.blobId = blobId;
      this.referenceCount = new AtomicInteger(0);
      this.blobContent = blobContent;
    }
  }

  /**
   * Thread which are used to upload blobs to Ambry. All threads created of such nature are alive only for a very short
   * time of just one upload
   */
  class PutBlobThread implements Runnable {
    private final String threadName;
    private final PutGetHelper producerJob;
    private final int maxPutCount;
    private final AtomicInteger currentPutCount;
    private final ArrayList<BlobAccessInfo> blobInfos;
    private final Lock lock;
    private final boolean enableVerboseLogging;

    public PutBlobThread(String threadName, PutGetHelper producerJob, ArrayList<BlobAccessInfo> blobInfos, Lock lock,
        final int maxPutCount, AtomicInteger currentPutCount, boolean enableVerboseLogging) {
      this.threadName = threadName;
      this.producerJob = producerJob;
      this.blobInfos = blobInfos;
      this.lock = lock;
      this.maxPutCount = maxPutCount;
      this.currentPutCount = currentPutCount;
      this.enableVerboseLogging = enableVerboseLogging;
    }

    public void run() {
      lock.lock();
      if (currentPutCount.get() < maxPutCount) {
        Pair<String, byte[]> blob = producerJob.produce();
        if (enableVerboseLogging) {
          logOutput(threadName + " : Producing : " + blob.getFirst() + ", currentPutCount " + currentPutCount.get()
              + ", queue Size " + blobInfos.size());
        }
        blobInfos.add(new BlobAccessInfo(blob.getFirst(), blob.getSecond()));
        currentPutCount.incrementAndGet();
      }
      lock.unlock();
    }
  }

  /**
   * GetManager threads to assist in consuming blobs from ambry and verifying its content
   */
  class GetManager implements Runnable {
    private final String threadName;
    private final PutGetHelper getConsumerJob;
    private final ArrayList<BlobAccessInfo> blobInfos;
    private final int maxGetCountPerBlob;
    private final Lock lock;
    private final AtomicInteger getCompletedCount;
    private final int maxPutCount;
    private final int maxBurstCountPerblob;
    private final boolean deleteOnExit;
    private final boolean enableVerboseLogging;
    private final long sleepTimeBetweenBatchGetsInMs;
    private final ThreadLocalRandom threadLocalRandom;
    private final ArrayList<Thread> deleteThreads;

    public GetManager(String threadName, PutGetHelper getConsumerJob, ArrayList<BlobAccessInfo> blobInfos,
        int maxGetCountPerBlob, Lock lock, final int maxPutCount, AtomicInteger getCompletedCount,
        int maxBurstCountPerBlob, long sleepTimeBetweenBatchGetsInMs, boolean deleteOnExit,
        boolean enableVerboseLogging, ThreadLocalRandom threadLocalRandom, ArrayList<Thread> deleteThreads) {
      this.threadName = threadName;
      this.getConsumerJob = getConsumerJob;
      this.blobInfos = blobInfos;
      this.maxGetCountPerBlob = maxGetCountPerBlob;
      this.lock = lock;
      this.maxPutCount = maxPutCount;
      this.getCompletedCount = getCompletedCount;
      this.maxBurstCountPerblob = maxBurstCountPerBlob;
      this.deleteOnExit = deleteOnExit;
      this.sleepTimeBetweenBatchGetsInMs = sleepTimeBetweenBatchGetsInMs;
      this.enableVerboseLogging = enableVerboseLogging;
      this.threadLocalRandom = threadLocalRandom;
      this.deleteThreads = deleteThreads;
    }

    @Override
    public void run() {
      while (getCompletedCount.get() < maxPutCount) {
        try {
          lock.lock();
          int size = blobInfos.size();
          if (size > 0) {
            BlobAccessInfo blobAccessInfo = blobInfos.get(threadLocalRandom.nextInt(size));
            if (blobAccessInfo != null) {
              blobAccessInfo.referenceCount.incrementAndGet();
              boolean getBlobVerificationComplete = false;
              if (blobAccessInfo.referenceCount.get() >= maxGetCountPerBlob) {
                if (enableVerboseLogging) {
                  logOutput(threadName + ": Removing " + blobAccessInfo.blobId
                      + " from the queue as we have reached maximum GET count");
                }
                blobInfos.remove(blobAccessInfo);
                getCompletedCount.incrementAndGet();
                getBlobVerificationComplete = true;
              }
              lock.unlock();
              List<Thread> getThreads = new ArrayList<>();
              int burstCount = threadLocalRandom.nextInt(maxBurstCountPerblob) + 1;
              if (enableVerboseLogging) {
                logOutput(threadName + " fetching " + blobAccessInfo.blobId + " " + burstCount + " times ");
              }
              for (int i = 0; i < burstCount; i++) {
                getThreads.add(new Thread(new GetBlobThread(blobAccessInfo, getConsumerJob)));
              }
              for (int i = 0; i < getThreads.size(); i++) {
                getThreads.get(i).start();
              }
              for (int i = 0; i < getThreads.size(); i++) {
                getThreads.get(i).join();
              }

              if (getBlobVerificationComplete && deleteOnExit) {
                if (enableVerboseLogging) {
                  logOutput(threadName + " GetBlob Verification completed for " + blobAccessInfo.blobId
                      + ", hence issuing deletes ");
                }
                Thread deleteThread =
                    new Thread(new DeleteBlobThread(threadName, blobAccessInfo, getConsumerJob, enableVerboseLogging));
                deleteThread.start();
                deleteThreads.add(deleteThread);
              }
            }
          } else {
            lock.unlock();
          }
          Thread.sleep(sleepTimeBetweenBatchGetsInMs);
        } catch (InterruptedException ex) {
          logOutput("InterruptedException in " + threadName + ex.getStackTrace());
        }
      }
      logOutput("Exiting GetConsumer " + threadName);
    }
  }

  /**
   * Threads used for consuming blobs from ambry. All threads created of such nature are alive only for a very short
   * time of just one Get and verify
   */
  class GetBlobThread implements Runnable {
    private final BlobAccessInfo blobAccessInfo;
    private final PutGetHelper getConsumerJob;

    public GetBlobThread(BlobAccessInfo blobAccessInfo, PutGetHelper getConsumerJob) {
      this.blobAccessInfo = blobAccessInfo;
      this.getConsumerJob = getConsumerJob;
    }

    public void run() {
      getConsumerJob.consumeAndValidate(blobAccessInfo.blobId, blobAccessInfo.blobContent,
          getConsumerJob.getErrorCodeForNoError());
    }
  }

  /**
   * Threads used for deleting blobs from ambry. All threads created of such nature are alive only for a very short
   * time of just one Delete and verify
   */
  class DeleteBlobThread implements Runnable {
    private final String threadName;
    private final BlobAccessInfo blobAccessInfo;
    private final PutGetHelper deleteConsumerJob;
    private final boolean enableVerboseLogging;

    public DeleteBlobThread(String threadName, BlobAccessInfo blobAccessInfo, PutGetHelper deleteConsumerJob,
        boolean enableVerboseLogging) {
      this.threadName = threadName;
      this.blobAccessInfo = blobAccessInfo;
      this.deleteConsumerJob = deleteConsumerJob;
      this.enableVerboseLogging = enableVerboseLogging;
    }

    public void run() {
      if (enableVerboseLogging) {
        logOutput(threadName + " Deleting blob " + blobAccessInfo.blobId);
      }
      deleteConsumerJob.deleteAndValidate(blobAccessInfo.blobId, deleteConsumerJob.getErrorCodeForNoError());
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
    Pair<String, byte[]> produce();

    /**
     * Request to fetch the blob pertaining to the {@code blobId} passed in and verify for its content
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param blobContent the content of the blob that needs to be verified against
     * @param expectedErrorCode the expected error code for the Get call
     */
    void consumeAndValidate(String blobId, byte[] blobContent, T expectedErrorCode);

    /**
     * Deletes a blob from ambry pertaining to the {@code blobId} passed in and verifies that subsequent Get fails
     * @param blobId the blobId the of that blob that needs to be fetched
     * @param expectedErrorCode the expected error code for the Delete call
     */
    void deleteAndValidate(String blobId, T expectedErrorCode);

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
    private final NetworkClientUtils _networkClientUtils;
    private final AtomicInteger correlationIdGenerator = new AtomicInteger(0);

    public ServerPutGetHelper(Properties properties, String hostName, int port, ClusterMap clusterMap, int maxBlobSize,
        int minBlobSize, Boolean enableVerboseLogging)
        throws Exception {
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      NetworkClientFactory networkClientFactory =
          new NetworkClientFactory(new NetworkMetrics(new MetricRegistry()), new NetworkConfig(verifiableProperties),
              null, 20, 20, 10000, SystemTime.getInstance());
      NetworkClient networkClient = networkClientFactory.getNetworkClient();
      this._networkClientUtils = new NetworkClientUtils(networkClient, 50, 100);
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
    public Pair<String, byte[]> produce() {
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
        List<RequestMetadata> requestMetadataList = new ArrayList<>();
        RequestMetadata requestInfo = new RequestMetadata<>();
        requestInfo._requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), putRequest);
        requestInfo.futureResult = new FutureResult<ByteBuffer>();
        requestInfo.correlationId = correlationId;
        requestMetadataList.add(requestInfo);
        _networkClientUtils.poll(requestMetadataList);
        ByteBuffer response = (ByteBuffer) requestInfo.futureResult.get();
        PutResponse putResponse = PutResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)));
        if (putResponse.getError() != ServerErrorCode.No_Error) {
          throw new UnexpectedException("error " + putResponse.getError());
        }
        long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTime;
        if (enableVerboseLogging) {
          logOutput(
              correlationId + " Time taken to put blob id " + blobId + " in ms " + latencyPerBlob / SystemTime.NsPerMs
                  + " for blob of size " + blob.length);
        }
        return new Pair(blobId.getID(), blob);
      } catch (InterruptedException e1) {
        logOutput(correlationId + " InterruptedException thrown when putting blob " + e1.getStackTrace());
      } catch (Exception e) {
        logOutput(correlationId + " Unknown Exception thrown when putting blob " + e.getStackTrace());
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
    public void consumeAndValidate(String blobIdStr, byte[] blobContent, ServerErrorCode expectedErrorCode) {
      BlobData blobData = null;
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
        List<RequestMetadata> requestInfoList = new ArrayList<>();
        RequestMetadata requestMetadata = new RequestMetadata<>();
        requestMetadata._requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), getRequest);
        requestMetadata.futureResult = new FutureResult<ByteBuffer>();
        requestMetadata.correlationId = correlationId;
        requestInfoList.add(requestMetadata);
        _networkClientUtils.poll(requestInfoList);
        ByteBuffer response = (ByteBuffer) requestMetadata.futureResult.get();
        GetResponse getResponse =
            GetResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)), clusterMap);
        long latencyPerBlob = -1;
        if (getResponse.getError() == ServerErrorCode.No_Error) {
          ServerErrorCode serverErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
          if (serverErrorCode == ServerErrorCode.No_Error) {
            blobData = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
            long sizeRead = 0;
            byte[] outputBuffer = new byte[(int) blobData.getSize()];
            ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
            while (sizeRead < blobData.getSize()) {
              streamOut.write(blobData.getStream().read());
              sizeRead++;
            }
            latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            boolean blobcontentMatched = Arrays.equals(blobContent, outputBuffer);
            if (!blobcontentMatched) {
              logOutput(correlationId + " Blob content mismatch for " + blobIdStr + " from " + hostName + ":" + port);
            }
          } else if (expectedErrorCode.equals(serverErrorCode)) {
            latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            if (enableVerboseLogging) {
              logOutput(correlationId + " Get of blob " + blobIdStr + " throws " + expectedErrorCode + " as expected ");
            }
          } else {
            latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            logOutput(correlationId + " Get of blob " + blobIdStr + " failed with unexpected Store level error code "
                + serverErrorCode + ", expected " + expectedErrorCode);
            if (enableVerboseLogging) {
              logOutput(correlationId + " Time taken to get blob id " + blobId + " in ms "
                  + latencyPerBlob / SystemTime.NsPerMs);
            }
            throw new IllegalStateException(correlationId + " Get of " + blobIdStr + " throws " + serverErrorCode
                + " different from what is expected  " + expectedErrorCode);
          }
        } else {
          latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
          logOutput(correlationId + " Get of blob " + blobIdStr + " failed with unexpected Server Level error code "
              + getResponse.getError() + ", expected " + expectedErrorCode);
          if (enableVerboseLogging) {
            logOutput(correlationId + " Time taken to get blob id " + blobId + " in ms "
                + latencyPerBlob / SystemTime.NsPerMs);
          }
          throw new IllegalStateException(correlationId + " Get of " + blobIdStr + " throws " + getResponse.getError()
              + " different from what is expected  " + expectedErrorCode);
        }
        if (enableVerboseLogging) {
          logOutput(
              correlationId + " Time taken to get blob " + blobId + " in ms " + latencyPerBlob / SystemTime.NsPerMs);
        }
      } catch (InterruptedException e) {
        logOutput(
            correlationId + " InterruptedException thrown when getting blob " + blobId + ", " + e.getStackTrace());
      } catch (MessageFormatException e) {
        logOutput(
            correlationId + " MessageFormatException thrown when getting blob " + blobId + ", " + e.getStackTrace());
      } catch (Exception e) {
        logOutput(correlationId + " Unknown Exception thrown when getting blob " + blobId + ", " + e.getStackTrace());
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
    public void deleteAndValidate(String blobId, ServerErrorCode expectedErrorCode) {
      int correlationId = -1;
      try {
        correlationId = correlationIdGenerator.incrementAndGet();
        DeleteRequest deleteRequest =
            new DeleteRequest(correlationId, "ConcurrencyTest", new BlobId(blobId, clusterMap));
        Long startTimeGetBlob = SystemTime.getInstance().nanoseconds();
        List<RequestMetadata> requestMetadataList = new ArrayList<>();
        RequestMetadata requestInfo = new RequestMetadata<>();
        requestInfo._requestInfo = new RequestInfo(hostName, new Port(port, PortType.PLAINTEXT), deleteRequest);
        requestInfo.futureResult = new FutureResult<ByteBuffer>();
        requestInfo.correlationId = correlationId;
        requestMetadataList.add(requestInfo);
        _networkClientUtils.poll(requestMetadataList);
        ByteBuffer response = (ByteBuffer) requestInfo.futureResult.get();
        DeleteResponse deleteResponse =
            DeleteResponse.readFrom(new DataInputStream(new ByteBufferInputStream(response)));
        long latencyTime = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
        if (enableVerboseLogging) {
          logOutput(correlationId + " Delete of  " + blobId + " took " + latencyTime / SystemTime.NsPerMs + " ms");
        }
        if (deleteResponse.getError() != ServerErrorCode.No_Error) {
          logOutput(
              correlationId + " Deletion of " + blobId + " failed with an exception " + deleteResponse.getError());
        } else {
          if (enableVerboseLogging) {
            logOutput(correlationId + " Deletion of " + blobId + " succeeded ");
          }
          consumeAndValidate(blobId, null, ServerErrorCode.Blob_Deleted);
        }
      } catch (InterruptedException e) {
        logOutput(correlationId + " InterruptedException thrown when deleting " + blobId + ", " + e.getStackTrace());
      } catch (Exception e) {
        logOutput(correlationId + " Unknown Exception thrown when deleting " + blobId + ", " + e.getStackTrace());
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
      _networkClientUtils.close();
    }
  }

  /**
   * Holds metadata about {@link com.github.ambry.network.RequestInfo} like the associated correlationID and the
   * {@link FutureResult}
   * @param <T>
   */
  static class RequestMetadata<T> {
    RequestInfo _requestInfo;
    FutureResult<T> futureResult;
    Callback<T> callback;
    int correlationId;
  }

  static class InvocationOptions {
    public final String hardwareLayoutFilePath;
    public final String partitionLayoutFilePath;
    public final int producerThreadCount;
    public final int getConsumerThreadCount;
    public final int burstThreadCountForPut;
    public final int burstThreadCountForGet;
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
    public final String outFile;

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
      ArgumentAcceptingOptionSpec<Integer> producerThreadCountOpt =
          parser.accepts("producerThreadCount", "Total number of producer threads")
              .withOptionalArg()
              .describedAs("producerThreadCount")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> getConsumerThreadCountOpt =
          parser.accepts("getConsumerThreadCount", "Total number of get consumer threads")
              .withOptionalArg()
              .describedAs("getConsumerThreadCount")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> burstThreadCountForPutOpt =
          parser.accepts("burstThreadCountForPut", "Total number of burst producer threads")
              .withOptionalArg()
              .describedAs("burstThreadCountForPut")
              .ofType(Integer.class)
              .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Integer> burstThreadCountForGetOpt =
          parser.accepts("burstThreadCountForGet", "Total number of burst get consumer threads")
              .withOptionalArg()
              .describedAs("burstThreadCountForGet")
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
      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "Output file to redirect the output to")
              .withOptionalArg()
              .describedAs("outFile")
              .ofType(String.class);
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
      this.producerThreadCount = options.valueOf(producerThreadCountOpt);
      this.getConsumerThreadCount = options.valueOf(getConsumerThreadCountOpt);
      this.burstThreadCountForPut = options.valueOf(burstThreadCountForPutOpt);
      this.burstThreadCountForGet = options.valueOf(burstThreadCountForGetOpt);
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
      this.outFile = options.valueOf(outFileOpt);
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
