/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf.serverperf;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Supports running performance tests on the Ambry server.
 * Supports performance testing for GET request, where it creates one thread
 * which queues requests in {@link ServerPerfNetworkQueue} , it creates second
 * thread which continuous polls and processes the responses currently in {@link ServerPerfNetworkQueue}
 *
 */

public class ServerPerformance {
  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final Http2ClientMetrics clientMetrics;

  private final CountDownLatch timedShutDownLatch;
  private final CountDownLatch shutDownLatch;

  LoadProducerConsumer producerConsumer;

  private static final Logger logger = LoggerFactory.getLogger(ServerPerformance.class);

  public enum TestType {
    GET_BLOB, PUT_BLOB
  }

  public static class ServerPerformanceConfig {

    @Config("server.performance.test.type")
    public final TestType serverPerformanceTestType;

    /**
     * The path to the hardware layout file. Needed if using
     * {@link StaticClusterAgentsFactory}.
     */
    @Config("server.performance.hardware.layout.file.path")
    @Default("")
    public final String serverPerformanceHardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link StaticClusterAgentsFactory}.
     */
    @Config("server.performance.partition.layout.file.path")
    @Default("")
    public final String serverPerformancePartitionLayoutFilePath;

    /**
     * maximum parallel network requests at a point of time
     */
    @Config("server.performance.max.parallel.requests")
    @Default("20")
    public final int serverPerformanceMaxParallelRequests;

    /**
     * Total number of network clients
     */
    @Config("server.performance.network.clients.count")
    @Default("2")
    public final int serverPerformanceNetworkClientsCount;

    /**
     * Time after which to drop a request
     */
    @Config("server.performance.operations.time.out.sec")
    @Default("15")
    public final int serverPerformanceOperationsTimeOutSec;

    /**
     * Path to file from which to read the blob ids
     */
    @Config("server.performance.blob.id.file.path")
    @Default("")
    public final String serverPerformanceBlobIdFilePath;

    /**
     * The hostname of the target server as it appears in the partition layout.
     */
    @Config("server.performance.hostname")
    @Default("localhost")
    public final String serverPerformanceHostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to).
     */
    @Config("server.performance.port")
    @Default("6667")
    public final int serverPerformancePort;

    /**
     * Total time after which to stop the performance test
     */
    @Config("server.performance.time.out.seconds")
    @Default("30")
    public final int serverPerformanceTimeOutSeconds;

    public ServerPerformanceConfig(VerifiableProperties verifiableProperties) {
      serverPerformanceTestType = TestType.valueOf(verifiableProperties.getString("server.performance.test.type"));
      serverPerformanceHardwareLayoutFilePath =
          verifiableProperties.getString("server.performance.hardware.layout.file.path", "");
      serverPerformancePartitionLayoutFilePath =
          verifiableProperties.getString("server.performance.partition.layout.file.path", "");
      serverPerformanceBlobIdFilePath = verifiableProperties.getString("server.performance.blob.id.file.path", "");
      serverPerformanceHostname = verifiableProperties.getString("server.performance.hostname", "localhost");
      serverPerformancePort = verifiableProperties.getInt("server.performance.port", 6667);
      serverPerformanceMaxParallelRequests =
          verifiableProperties.getInt("server.performance.max.parallel.requests", 20);
      serverPerformanceNetworkClientsCount = verifiableProperties.getInt("server.performance.network.clients.count", 2);
      serverPerformanceTimeOutSeconds = verifiableProperties.getInt("server.performance.time.out.seconds", 30);
      serverPerformanceOperationsTimeOutSec =
          verifiableProperties.getInt("server.performance.operations.time.out.sec", 15);
    }
  }

  public ServerPerformance(VerifiableProperties verifiableProperties) throws Exception {
    config = new ServerPerformanceConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            config.serverPerformanceHardwareLayoutFilePath,
            config.serverPerformancePartitionLayoutFilePath)).getClusterMap();
    clientMetrics = new Http2ClientMetrics(new MetricRegistry());
    timedShutDownLatch = new CountDownLatch(1);
    shutDownLatch = new CountDownLatch(1);
    networkQueue = new ServerPerfNetworkQueue(verifiableProperties, clientMetrics, new SystemTime(),
        config.serverPerformanceMaxParallelRequests, config.serverPerformanceNetworkClientsCount,
        config.serverPerformanceOperationsTimeOutSec);
    networkQueue.start();

   switch (config.serverPerformanceTestType) {
     case GET_BLOB:
       producerConsumer = new GetLoadProducerConsumer(networkQueue, config, clusterMap);
       break;
     case PUT_BLOB:
       producerConsumer = new PutLoadProducerConsumer(networkQueue, config, clusterMap);
     default:
       throw new IllegalArgumentException("Unrecognized test type: "+ config.serverPerformanceTestType);
   }
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    ServerPerformance serverPerformance = new ServerPerformance(verifiableProperties);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Starting the shutdown");
        serverPerformance.forceShutDown();
        serverPerformance.printMetrics();
      } catch (Exception e) {
        logger.error("Caught error while shut down", e);
      }
    }));
    serverPerformance.startLoadTest();
    System.exit(0);
  }

  public void startLoadTest() throws Exception {
    Thread loadProducer = getLoadProducerThread();
    Thread loadConsumer = getLoadConsumerThread();
    Thread shutDownThread = getTimedShutDownThread();
    shutDownThread.start();
    loadProducer.start();
    loadConsumer.start();
    loadProducer.join();
    loadConsumer.join();
    shutDownLatch.countDown();
  }

  Thread getLoadProducerThread() {
    return new Thread(() -> {
      while (true) {
        try {
          producerConsumer.produce();
        } catch (ShutDownException e) {
          logger.info("Load producer thread is shutting down");
          break;
        } catch (Exception e) {
          logger.error("encountered error in loadProducer", e);
        }
      }
      logger.info("Load producer thread is finished");
    });
  }

  Thread getLoadConsumerThread() {
    return new Thread(() -> {
      while (true) {
        try {
          producerConsumer.consume();
        } catch (ShutDownException e) {
          logger.info("Network queue is shutdown. Exiting from thread");
          break;
        } catch (Exception e) {
          logger.error("error in load consumer thread", e);
        }
      }
      logger.info("Load consumer thread is finished");
    });
  }

  public void printMetrics() {
    logger.info("HTTP2 error count {}", clientMetrics.http2NetworkErrorCount.getCount());
    logger.info("HTTP2 dropped request count {}", clientMetrics.http2RequestsToDropCount.getCount());
    logger.info("HTTP2 send Mean rate {}", clientMetrics.http2ClientSendRate.getMeanRate());
    logger.info("HTTP2 stream median read time {}",
        clientMetrics.http2StreamFirstToLastFrameTime.getSnapshot().getMedian());
    logger.info("HTTP2 stream median acquire time, {}",
        clientMetrics.http2FirstStreamAcquireTime.getSnapshot().getMedian());
  }

  /**
   * Waits until the {@link ServerPerformanceConfig#serverPerformanceTimeOutSeconds} to elapse
   * or is forced out of wait and starts the shutdown
   * @return
   */
  Thread getTimedShutDownThread() {
    return new Thread(() -> {
      try {
        timedShutDownLatch.await(config.serverPerformanceTimeOutSeconds, TimeUnit.SECONDS);
        logger.info("Timed shutdown triggerred");
        shutDown();
      } catch (Exception e) {
        logger.error("Caught exception in shutdown thread", e);
      }
    });
  }

  /**
   * forces the {@link #getTimedShutDownThread()} to wake up and
   * start the shutdown, waits until the shutdown is complete
   */
  public void forceShutDown() {
    timedShutDownLatch.countDown();
    try {
      shutDownLatch.await();
    } catch (Exception e) {
      logger.error("Error while waiting gor shutdown latch", e);
    }
  }

  /**
   * Shuts down the network client,
   * which will cause load producer thread to stop and load consumer threads to
   * stop after consuming the pending requests
   * @throws Exception
   */
  public void shutDown() throws Exception {
    networkQueue.shutDown();
  }
}
