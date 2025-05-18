/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.http2.Http2BlockingChannelPool;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to do load testing on one single ambry-server instance.
 *
 * This tool can be used to generate GET, PUT, DELETE and TTL_UPDATE requests to a single ambry-server instance at a
 * desired QPS, with blob size randomization based on a distribution.
 *
 * Notice that this test doesn't provide any client size metric. You have to check server side metric to validate the
 * server performance.
 *
 * To run this tool.
 * First compile ambry.jar with ./gradelw allJar
 * Then copy target/ambry.jar to a host that has access to your test ambry-server.
 * Then run
 *  > java -cp "*" com.github.ambry.tools.perf.ConcurrentServerLoadTest --props <your_properties_file>
 *
 *  You have to provide a properties file in the command line with the following properties:
 *  > cat load_test.props
 *  hardware.layout.file.path=./HardwareLayoutValidation.json
 *  partition.layout.file.path=./PartitionLayoutValidation.json
 *  clustermap.cluster.name=validation
 *  clustermap.datacenter.name=datacenter
 *  clustermap.host.name=test.server.example.com
 *  clustermap.enable.http2.replication=true
 *  hostname=test.server.example.com
 *  port=6670
 *  # Add ssl configurations here
 *  put.per.second=100
 *  get.per.second=2000
 *  delete.per.second=10
 *  update.ttl.per.second=10
 *  test.duration.in.second=3600
 *  test.server.hostname=test.server.example.com
 *  test.server.port=6670
 *  account.id=1024
 *  container.id=512
 *  get.blob.id.file.path=./blob_ids.txt
 *  put.blob.size.distribution={"0.5":2000,"0.75":20000,"0.95":200000,"0.999":2000000}
 *  put.blob.size=2048000
 *  number.of.blob.ids.to.save.for.get=1000000
 *  http2.min.connection.per.port=10
 *
 *  To just run upload test, please cset get.per.second, delete.per.second and update.ttl.per.second to 0.
 *  To just run download test, please set put.per.second, delete.per.second and update.ttl.per.second to 0. Also you
 *  have to provide a file with blob ids to get in the property "get.blob.id.file.path".
 *
 *  You can also provide a map of size distribution for put requests. The key of the map should be the percentile and the
 *  value should be the desired size in bytes. Please make sure that the 99.9 percentile size can't be larger than the
 *  max blob size allowed in the server. Alternatively, you can provide a fixed size for all blobs by setting "put.blob.size"
 *  in the property file. This tool prefer "put.blob.size.distribution" over "put.blob.size" if both are present in the file.
 *
 *  When delete qps is not 0, you have to run put request before delete request. The blob ids provided through "get.blob.id.file.path"
 *  won't be deleted by this tool.
 */
public class ConcurrentServerLoadTest {

  private static class LoadTestConfig {
    /**
     * Desired Put QPS
     */
    @Config("put.per.second")
    final int putPerSecond;

    /**
     * Desired Get QPS
     */
    @Config("get.per.second")
    final int getPerSecond;

    /**
     * Desired Delete QPS.
     */
    @Config("delete.per.second")
    final int deletePerSecond;

    /**
     * Desired update ttl QPS.
     */
    @Config("update.ttl.per.second")
    final int updateTtlPerSecond;

    /**
     * How long this test should last
     */
    @Config("test.duration.in.second")
    final int testDurationInSecond;

    /**
     * Test ambry-server hostname
     */
    @Config("test.server.hostname")
    final String testServerHostname;

    /**
     * Test ambry-server port
     */
    @Config("test.server.port")
    final int testServerPort;

    /**
     * This tool generates testing blobs, they should have ttl.
     */
    @Config("blob.ttl.in.second")
    final int blobTtlInSecond;

    /**
     * Hardware layout file path
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * Partition layout file path
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * File path to a list of blob ids for get request. If put qps is 0, this file should be provided.
     */
    @Config("get.blob.id.file.path")
    final String getBlobIdFilePath;

    /**
     * AccountId used for put
     */
    @Config("account.id")
    final short accountId;

    /**
     * ContainerId used for put
     */
    @Config("container.id")
    final short containerId;

    /**
     * A json map that contains the distribution of blob sizes for put requests.
     * the key should be percentile and the value should be the size in bytes.
     *
     * For example, it should be something like this:
     * {
     *   "0.5": 2000,
     *   "0.75": 20000,
     *   "0.95": 200000,
     *   "0.999": 2000000
     * }
     * which means, 50% of the blobs are smaller than 2000 bytes, 75% of the blobs are smaller than 20000 bytes, etc.
     */
    @Config("put.blob.size.distribution")
    final String putBlobSizeDistribution;

    /**
     * A fixed size for all blobs for put requests. This will be ignored if put.blob.size.distribution is provided.
     */
    @Config("put.blob.size")
    final int putBlobSize;

    /**
     * Number of blob ids to keep in memory from put requests, later to be used for get and delete requests.
     */
    @Config("number.of.blob.ids.to.save.for.get")
    final int numberOfBlobIdsToSaveForGet;

    final Map<String, Integer> putBlobSizeDistributionMap = new HashMap<>();
    private static final int MAX_QPS_PER_OPERATION = 1000000;
    private static final int DEFAULT_TTL = 30 * 24 * 60 * 60; // 30 days

    public LoadTestConfig(VerifiableProperties verifiableProperties) {
      putPerSecond = verifiableProperties.getIntInRange("put.per.second", 0, 0, MAX_QPS_PER_OPERATION);
      getPerSecond = verifiableProperties.getIntInRange("get.per.second", 0, 0, MAX_QPS_PER_OPERATION);
      deletePerSecond = verifiableProperties.getIntInRange("delete.per.second", 0, 0, MAX_QPS_PER_OPERATION);
      updateTtlPerSecond = verifiableProperties.getIntInRange("update.ttl.per.second", 0, 0, MAX_QPS_PER_OPERATION);
      testDurationInSecond = verifiableProperties.getIntInRange("test.duration.in.second", 300, 1, Integer.MAX_VALUE);
      testServerHostname = verifiableProperties.getString("test.server.hostname", "");
      testServerPort = verifiableProperties.getInt("test.server.port", 15088);
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      getBlobIdFilePath = verifiableProperties.getString("get.blob.id.file.path", "");
      putBlobSize = verifiableProperties.getInt("put.blob.size", 0);
      putBlobSizeDistribution = verifiableProperties.getString("put.blob.size.distribution", "");
      numberOfBlobIdsToSaveForGet = verifiableProperties.getInt("number.of.blob.ids.to.save.for.get", 0);
      accountId = verifiableProperties.getShort("account.id", Account.UNKNOWN_ACCOUNT_ID);
      containerId = verifiableProperties.getShort("container.id", Container.UNKNOWN_CONTAINER_ID);
      blobTtlInSecond = verifiableProperties.getInt("blob.ttl.in.second", DEFAULT_TTL);
    }

    void validate() {
      if (putPerSecond + getPerSecond + deletePerSecond + updateTtlPerSecond == 0) {
        throw new IllegalArgumentException("At least one of put, get, delete or update ttl should be enabled");
      }

      if (deletePerSecond > 0 || updateTtlPerSecond > 0) {
        if (putPerSecond == 0) {
          throw new IllegalArgumentException("Put should be enabled for delete or update ttl to work");
        }
      }

      if (getPerSecond > 0) {
        if ((getBlobIdFilePath == null || getBlobIdFilePath.isEmpty()) && putPerSecond == 0) {
          throw new IllegalArgumentException(
              "get.blob.id.file.path should be set for get requests or put should be enabled");
        }
      }

      if (testServerHostname == null || testServerHostname.isEmpty()) {
        throw new IllegalArgumentException("test.server.hostname should be set");
      }

      if (hardwareLayoutFilePath == null || hardwareLayoutFilePath.isEmpty()) {
        throw new IllegalArgumentException("hardware.layout.file.path should be set");
      }
      if (partitionLayoutFilePath == null || partitionLayoutFilePath.isEmpty()) {
        throw new IllegalArgumentException("partition.layout.file.path should be set");
      }
      validatePutBlobSize();
    }

    void validatePutBlobSize() {
      ObjectMapper objectMapper = new ObjectMapper();
      if (putBlobSizeDistribution != null && !putBlobSizeDistribution.isEmpty()) {
        try {
          putBlobSizeDistributionMap.putAll(objectMapper.readValue(putBlobSizeDistribution, Map.class));
          int previousSize = 0;
          for (String key : Arrays.asList("0.5", "0.75", "0.95", "0.999")) {
            if (putBlobSizeDistributionMap.containsKey(key)) {
              int size = putBlobSizeDistributionMap.get(key);
              if (size <= previousSize) {
                throw new IllegalArgumentException("Put blob size distribution should be in ascending order");
              }
              previousSize = size;
            } else {
              throw new IllegalArgumentException("Missing key " + key + " in put blob size distribution");
            }
          }
        } catch (Exception e) {
          throw new IllegalArgumentException("Failed to parse put blob size distribution", e);
        }
      } else {
        if (putBlobSize <= 0) {
          throw new IllegalArgumentException("put.blob.size should be set");
        }
      }
    }
  }

  // Expect the average latency of each operation to be less than 20ms
  private static final Logger logger = LoggerFactory.getLogger(ConcurrentServerLoadTest.class);
  private static final int MAX_OPERATIONS_PER_THREAD = 50;
  private static final String BIN_CONTENT_TYPE = "application/octet-stream";

  private final LoadTestConfig config;
  private final byte[] blobData;
  private final ConnectionPool connectionPool;
  private final ClusterMap clusterMap;
  private final SizeSampler sizeSampler;
  private final BlobIdStore blobIdStore;
  private final AtomicInteger correlationId = new AtomicInteger(0);
  private final DataNodeId dataNodeId;
  private final List<LoadTestThread> loadTestThreads = new ArrayList<>();
  // protect replicas
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final List<ReplicaId> replicas = new ArrayList<>();

  public ConcurrentServerLoadTest(VerifiableProperties verifiableProperties) throws Exception {
    config = new LoadTestConfig(verifiableProperties);
    config.validate();
    blobData = TestUtils.getRandomBytes(getBlobDataSize());

    SSLFactory sslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(verifiableProperties);
    connectionPool =
        new Http2BlockingChannelPool(sslFactory, http2ClientConfig, new Http2ClientMetrics(new MetricRegistry()));

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
        config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap();
    sizeSampler = getSizeSampler();
    blobIdStore = new BlobIdStore(config.getBlobIdFilePath);
    dataNodeId = clusterMap.getDataNodeId(config.testServerHostname, config.testServerPort);
    replicas.addAll(getEligibleReplicaIds(clusterMap.getReplicaIds(dataNodeId)));
    logger.info("There are {} writable replicas", replicas.size());
  }

  /**
   * Return the maximum blob size for put requests. If "put.blob.size.distribution" is provided, it will return the size
   * from 99.9%. Otherwise, it returns the fixed size from "put.blob.size".
   * @return
   */
  private int getBlobDataSize() {
    if (config.putBlobSizeDistributionMap.isEmpty()) {
      return config.putBlobSize;
    } else {
      return config.putBlobSizeDistributionMap.get("0.999");
    }
  }

  /**
   * Get the eligible replica ids from the list of replicas. The eligible replicas are those that are not down or sealed.
   * @param replicas
   * @return
   */
  private List<ReplicaId> getEligibleReplicaIds(List<? extends ReplicaId> replicas) {
    List<ReplicaId> eligibleReplicas = new ArrayList<>();
    for (ReplicaId replica : replicas) {
      if (!replica.isDown() && !replica.isSealed()) {
        eligibleReplicas.add(replica);
      }
    }
    return eligibleReplicas;
  }

  /**
   * Get a random writable replica id from the list of replicas. If there is no writable replica, it will throw an
   * IllegalStateException.
   * @return
   */
  private ReplicaId getRandomWritableReplicaId() {
    rwLock.readLock().lock();
    try {
      if (replicas.isEmpty()) {
        throw new IllegalStateException("No writable replicas available");
      }
      return replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Return the number of writable replicas. This is used to determine if the test should continue or not.
   * @return
   */
  private int getNumberWritableReplicas() {
    rwLock.readLock().lock();
    try {
      return replicas.size();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * This method is called when a replica id becomes available. It will remove the replica id from the list of writable
   * replicas.
   * @param replicaId
   */
  private void onReplicaIdBecomeUnavailable(ReplicaId replicaId) {
    rwLock.writeLock().lock();
    try {
      if (replicas.remove(replicaId)) {
        logger.info("Replica {} is unavailable, removing it from the list of writable replicas", replicaId);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void start() {
    connectionPool.start();
    // Assume each operation is going to take 20 milliseconds to finish on average, which means one thread can execute
    // 50 operations per seconds. Add up all qps and seeing how many threads we need to create.
    int totalQps = config.putPerSecond + config.getPerSecond + config.deletePerSecond + config.updateTtlPerSecond;
    int numThreads = (int) Math.ceil((double) totalQps / MAX_OPERATIONS_PER_THREAD);
    int[] putLoads = distributeLoad(config.putPerSecond, numThreads);
    int[] getLoads = distributeLoad(config.getPerSecond, numThreads);
    int[] deleteLoads = distributeLoad(config.deletePerSecond, numThreads);
    int[] updateTtlLoads = distributeLoad(config.updateTtlPerSecond, numThreads);
    logger.info("Total qps is {}, creating {} threads for load tests", totalQps, numThreads);

    for (int i = 0; i < numThreads; i++) {
      // The last thread should take care of the remaining operations
      LoadTestThread testThread =
          new LoadTestThread("LoadTestThread-" + i, putLoads[i], getLoads[i], deleteLoads[i], updateTtlLoads[i]);
      testThread.start();
      loadTestThreads.add(testThread);
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        loadTestThreads.get(i).join();
      } catch (InterruptedException e) {
        logger.error("Thread interrupted", e);
      }
    }
  }

  private void stop() {
    for (LoadTestThread thread : loadTestThreads) {
      try {
        thread.stopRunning();
      } catch (Exception e) {
        logger.error("Failed to stop thread {}", thread.getName(), e);
      }
    }
    connectionPool.shutdown();
  }

  /**
   * Return a {@link SizeSampler} based on the configuration. If "put.blob.size.distribution" is provided, it will
   * return a {@link PercentileSizeSampler} with the distribution. Otherwise, it will return a {@link FixedSizeSampler}.
   * @return
   */
  private SizeSampler getSizeSampler() {
    if (config.putBlobSizeDistributionMap.isEmpty()) {
      return new FixedSizeSampler(config.putBlobSize);
    } else {
      return new PercentileSizeSampler(config.putBlobSizeDistributionMap);
    }
  }

  /**
   * Distribute the load evenly across the threads.
   * @param totalLoad The total load
   * @param numThreads The number of threads.
   * @return
   */
  private int[] distributeLoad(int totalLoad, int numThreads) {
    int[] loads = new int[numThreads];
    int baseLoad = totalLoad / numThreads;
    int remainingLoad = totalLoad % numThreads;

    for (int i = 0; i < numThreads; i++) {
      loads[i] = baseLoad;
      if (remainingLoad > 0) {
        loads[i]++;
        remainingLoad--;
      }
    }
    // shuffle loads
    for (int i = 0; i < loads.length; i++) {
      int randomIndex = (int) (Math.random() * loads.length);
      int temp = loads[i];
      loads[i] = loads[randomIndex];
      loads[randomIndex] = temp;
    }
    return loads;
  }

  /**
   * Workder thread to run load test.
   */
  private class LoadTestThread extends Thread {
    private final String threadName;
    private final int putPerSecond;
    private final int getPerSecond;
    private final int deletePerSecond;
    private final int updateTtlPerSecond;
    private final Random threadLocalRandom = ThreadLocalRandom.current();
    private final Throttler throttler;
    private volatile boolean isRunning = true;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private Operation[] operations;

    public LoadTestThread(String threadName, int putPerSecond, int getPerSecond, int deletePerSecond,
        int updateTtlPerSecond) {
      this.threadName = threadName;
      this.putPerSecond = putPerSecond;
      this.getPerSecond = getPerSecond;
      this.deletePerSecond = deletePerSecond;
      this.updateTtlPerSecond = updateTtlPerSecond;
      int totalQps = putPerSecond + getPerSecond + deletePerSecond + updateTtlPerSecond;
      this.throttler = new Throttler(totalQps, 100, true, SystemTime.getInstance());
      this.operations = new Operation[totalQps];
      int baseIdx = 0;
      // Add all operations to an array so we can iterate through all operations.
      for (int i = 0; i < putPerSecond; i++) {
        this.operations[baseIdx + i] = new PutOperation();
      }
      baseIdx += putPerSecond;
      for (int i = 0; i < getPerSecond; i++) {
        this.operations[baseIdx + i] = new GetOperation();
      }
      baseIdx += getPerSecond;
      for (int i = 0; i < deletePerSecond; i++) {
        this.operations[baseIdx + i] = new DeleteOperation();
      }
      baseIdx += deletePerSecond;
      for (int i = 0; i < updateTtlPerSecond; i++) {
        this.operations[baseIdx + i] = new UpdateTtlOperation();
      }
    }

    /**
     * Return true if we should continue the test. The test should continue if:
     * 1. The test duration is not over
     * 2. The test is not stopped
     * 3. The put per second is not  0 and there are writable replicas available
     * @param startTime
     * @param duration
     * @return
     */
    private boolean shouldContinueTest(long startTime, long duration) {
      return SystemTime.getInstance().milliseconds() < startTime + duration && isRunning && (putPerSecond == 0
          || getNumberWritableReplicas() > 0);
    }

    @Override
    public void run() {
      long startTime = SystemTime.getInstance().milliseconds();
      long duration = TimeUnit.SECONDS.toMillis(config.testDurationInSecond);
      logger.info("Starting load test thread {} with duration {} ms", threadName, duration);

      while (shouldContinueTest(startTime, duration)) {
        ConnectedChannel channel = null;
        try {
          channel = connectionPool.checkOutConnection(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(), 0);
          for (Operation operation : operations) {
            if (!shouldContinueTest(startTime, duration)) {
              break;
            }
            operation.execute(channel);
            throttler.maybeThrottle(1);
          }
          connectionPool.checkInConnection(channel);
          // shuffle operations after first try
          if (isRunning) {
            for (int i = operations.length - 1; i > 0; i--) {
              int j = threadLocalRandom.nextInt(i + 1);
              Operation temp = operations[i];
              operations[i] = operations[j];
              operations[j] = temp;
            }
          }
        } catch (Exception e) {
          logger.error("Failed to execute operation", e);
          if (channel != null) {
            connectionPool.destroyConnection(channel);
          }
        }
      }
      stopLatch.countDown();
    }

    public void stopRunning() throws Exception {
      isRunning = false;
      logger.info("Shutting down load test thread {}", threadName);
      stopLatch.await(5, TimeUnit.SECONDS);
    }

    private class PutOperation implements Operation {
      @Override
      public void execute(ConnectedChannel channel) throws IOException {
        int blobSize = sizeSampler.getNextSize();
        logger.trace("BlobSize for put operation is {}", blobSize);
        byte[] userMetaData = new byte[new Random().nextInt(1024)];
        BlobProperties props =
            new BlobProperties(blobSize, threadName, null, BIN_CONTENT_TYPE, false, config.blobTtlInSecond,
                config.accountId, config.containerId, false, null, null, null);
        // Randomly select a replicaId
        ReplicaId replicaId = getRandomWritableReplicaId();
        BlobId blobId = new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            props.getAccountId(), props.getContainerId(), replicaId.getPartitionId(), false,
            BlobId.BlobDataType.DATACHUNK);
        logger.trace("Generate blob id {} in partition {}", blobId, replicaId.getPartitionId());

        PutRequest putRequest =
            new PutRequest(correlationId.incrementAndGet(), threadName, blobId, props, ByteBuffer.wrap(userMetaData),
                Unpooled.wrappedBuffer(blobData, 0, blobSize), props.getBlobSize(), BlobType.DataBlob, null);

        DataInputStream stream = null;
        try {
          ChannelOutput output = channel.sendAndReceive(putRequest);
          stream = output.getInputStream();
          PutResponse response = PutResponse.readFrom(stream);
          if (response.getError() != ServerErrorCode.NoError) {
            logger.error("PutRequest failed, errorCode: {}, blobId: {}", response.getError(), blobId);
            if (response.getError() == ServerErrorCode.PartitionReadOnly
                || response.getError() == ServerErrorCode.ReplicaUnavailable) {
              // handle read-only partition
              onReplicaIdBecomeUnavailable(replicaId);
            } else {
            }
          } else {
            logger.trace("Successfully put blob id {}", blobId);
            blobIdStore.maybeAddBlobId(blobId);
          }
        } catch (IOException e) {
          logger.error("Failed to send put request", e);
          throw e;
        } finally {
          if (stream != null && stream instanceof NettyByteBufDataInputStream) {
            // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
            ((NettyByteBufDataInputStream) stream).getBuffer().release();
          }
        }
      }
    }

    private class GetOperation implements Operation {
      @Override
      public void execute(ConnectedChannel channel) throws IOException {
        // Implement the get operation logic here
        BlobId blobId = blobIdStore.getRandomBlobId();
        PartitionRequestInfo partitionRequestInfo =
            new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
        GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), threadName, MessageFormatFlags.All,
            Collections.singletonList(partitionRequestInfo), GetOption.Include_All);

        DataInputStream stream = null;
        try {
          ChannelOutput output = channel.sendAndReceive(getRequest);
          stream = output.getInputStream();
          GetResponse response = GetResponse.readFrom(stream, clusterMap);
          if (response.getError() != ServerErrorCode.NoError) {
            logger.error("GetResponse failed, errorCode: {}, blobId: {}", response.getError(), blobId);
          }
        } catch (IOException e) {
          logger.error("Failed to send put request", e);
          throw e;
        } finally {
          if (stream != null && stream instanceof NettyByteBufDataInputStream) {
            // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
            ((NettyByteBufDataInputStream) stream).getBuffer().release();
          }
        }
      }
    }

    private class DeleteOperation implements Operation {
      @Override
      public void execute(ConnectedChannel channel) throws IOException {
        // Implement the delete operation logic here
        BlobId blobId = blobIdStore.getRandomBlobIdForDelete();
        DeleteRequest deleteRequest = new DeleteRequest(correlationId.incrementAndGet(), threadName, blobId,
            SystemTime.getInstance().milliseconds());

        DataInputStream stream = null;
        try {
          // blobIdStore.removeBlobId(blobId);
          ChannelOutput output = channel.sendAndReceive(deleteRequest);
          stream = output.getInputStream();
          DeleteResponse response = DeleteResponse.readFrom(stream);
          if (response.getError() != ServerErrorCode.NoError && response.getError() != ServerErrorCode.BlobDeleted) {
            logger.error("DeleteResponse failed, errorCode: {}, blobId: {}", response.getError(), blobId);
          }
        } catch (IOException e) {
          logger.error("Failed to send put request", e);
          throw e;
        } finally {
          if (stream != null && stream instanceof NettyByteBufDataInputStream) {
            // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
            ((NettyByteBufDataInputStream) stream).getBuffer().release();
          }
        }
      }
    }

    private class UpdateTtlOperation implements Operation {
      @Override
      public void execute(ConnectedChannel channel) throws IOException {
        // Implement the delete operation logic here
        BlobId blobId = blobIdStore.getRandomBlobId();
        TtlUpdateRequest ttlUpdateRequest =
            new TtlUpdateRequest(correlationId.incrementAndGet(), threadName, blobId, Utils.Infinite_Time,
                SystemTime.getInstance().milliseconds());

        DataInputStream stream = null;
        try {
          ChannelOutput output = channel.sendAndReceive(ttlUpdateRequest);
          stream = output.getInputStream();
          TtlUpdateResponse response = TtlUpdateResponse.readFrom(stream);
          if (response.getError() != ServerErrorCode.NoError
              && response.getError() != ServerErrorCode.BlobAlreadyUpdated) {
            logger.error("TtlUpdateResponse failed, errorCode: {}, blobId: {}", response.getError(), blobId);
          }
        } catch (IOException e) {
          logger.error("Failed to send put request", e);
          throw e;
        } finally {
          if (stream != null && stream instanceof NettyByteBufDataInputStream) {
            // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
            ((NettyByteBufDataInputStream) stream).getBuffer().release();
          }
        }
      }
    }
  }

  interface Operation {
    void execute(ConnectedChannel channel) throws IOException;
  }

  /**
   * Interface for sampling blob sizes.
   */
  interface SizeSampler {
    int getNextSize();
  }

  /**
   * Sampler that returns a fixed size.
   */
  private class FixedSizeSampler implements SizeSampler {
    private final int size;

    public FixedSizeSampler(int size) {
      this.size = size;
    }

    @Override
    public int getNextSize() {
      return size;
    }
  }

  /**
   * Sampler that returns size based on the provided size distribution.
   */
  private class PercentileSizeSampler implements SizeSampler {
    private static final int MAX_SAMPLE_SIZE = 100000;
    private final double[] percentiles;
    private final int[] sizes;
    private final int[] sampleSizes = new int[MAX_SAMPLE_SIZE];
    private final AtomicLong idx = new AtomicLong(0);

    public PercentileSizeSampler(Map<String, Integer> sizeDistribution) {
      TreeMap<String, Integer> orderedDistribution = new TreeMap<>(sizeDistribution);
      percentiles = new double[sizeDistribution.size()];
      sizes = new int[sizeDistribution.size()];
      int i = 0;
      for (Map.Entry<String, Integer> entry : orderedDistribution.entrySet()) {
        percentiles[i] = Double.parseDouble(entry.getKey());
        sizes[i] = entry.getValue();
        i++;
      }

      for (i = 0; i < MAX_SAMPLE_SIZE; i++) {
        sampleSizes[i] = getRandomSize();
      }
    }

    @Override
    public int getNextSize() {
      long nextIdx = idx.getAndIncrement();
      int modIdx = (int) (nextIdx % MAX_SAMPLE_SIZE);
      return sampleSizes[modIdx];
    }

    private int getRandomSize() {
      double r = Math.random();
      if (r <= percentiles[0]) {
        return sizes[0];
      }
      if (r >= percentiles[percentiles.length - 1]) {
        return sizes[sizes.length - 1];
      }

      // Find the interval
      for (int i = 1; i < percentiles.length; i++) {
        if (r <= percentiles[i]) {
          double x0 = percentiles[i - 1];
          double x1 = percentiles[i];
          double y0 = sizes[i - 1];
          double y1 = sizes[i];

          // Linear interpolation
          return (int) (y0 + (r - x0) * (y1 - y0) / (x1 - x0));
        }
      }
      throw new IllegalStateException("Unexpected state in percentile interpolation.");
    }
  }

  private class BlobIdStore {
    private final List<BlobId> blobIdsFromFile = new ArrayList<>();
    private final List<BlobId> blobIdsFromPut = new ArrayList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    public BlobIdStore(String blobIdFilePath) {
      if (blobIdFilePath != null && !blobIdFilePath.isEmpty()) {
        try {
          loadBlobIdsFromFile();
        } catch (Exception e) {
          logger.error("Failed to load blob ids from file", e);
        }
      }
    }

    private void loadBlobIdsFromFile() throws Exception {
      try (final BufferedReader br = new BufferedReader(new FileReader(config.getBlobIdFilePath))) {
        String line;
        while ((line = br.readLine()) != null) {
          String id = line.trim();
          BlobId blobId = new BlobId(id, clusterMap);
          blobIdsFromFile.add(blobId);
        }
      }
    }

    public BlobId getRandomBlobId() {
      rwLock.readLock().lock();
      try {
        int totalSize = blobIdsFromFile.size() + blobIdsFromPut.size();
        int idx = threadLocalRandom.nextInt(totalSize);
        if (idx >= blobIdsFromFile.size()) {
          return blobIdsFromPut.get(idx - blobIdsFromFile.size());
        } else {
          return blobIdsFromFile.get(idx);
        }
      } finally {
        rwLock.readLock().unlock();
      }
    }

    public BlobId getRandomBlobIdForDelete() {
      rwLock.writeLock().lock();
      try {
        int idx = threadLocalRandom.nextInt(blobIdsFromPut.size());
        BlobId toDelete = blobIdsFromPut.get(idx);
        // Swap it with last element
        blobIdsFromPut.set(idx, blobIdsFromPut.get(blobIdsFromPut.size() - 1));
        // remove last element
        blobIdsFromPut.remove(blobIdsFromPut.size() - 1);
        return toDelete;
      } finally {
        rwLock.writeLock().unlock();
      }
    }

    public void maybeAddBlobId(BlobId blobId) {
      rwLock.writeLock().lock();
      try {
        if (blobIdsFromPut.size() < config.numberOfBlobIdsToSaveForGet) {
          logger.trace("Adding blob id in the store: {}", blobId);
          blobIdsFromPut.add(blobId);
        } else {
          // randomly remove one blobId
          int idx = threadLocalRandom.nextInt(blobIdsFromPut.size() * 2);
          if (idx < blobIdsFromPut.size()) {
            // 50% chance to save this blob id
            logger.trace("Exceed threshold, but adding blob id in the store: {}", blobId);
            blobIdsFromPut.set(idx, blobId);
          }
        }
      } finally {
        rwLock.writeLock().unlock();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    ConcurrentServerLoadTest loadTest = new ConcurrentServerLoadTest(verifiableProperties);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Starting the shutdown");
        loadTest.stop();
      } catch (Exception e) {
        logger.error("Caught error while shut down", e);
      }
    }));
    loadTest.start();
    System.exit(0);
  }
}
