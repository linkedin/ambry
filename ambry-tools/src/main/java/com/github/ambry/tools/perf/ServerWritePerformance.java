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
package com.github.ambry.tools.perf;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.github.ambry.utils.Utils.*;


/**
 * Generates load for write performance test
 */
public class ServerWritePerformance {
  public static void main(String args[]) {
    FileWriter blobIdsWriter = null;
    FileWriter performanceWriter = null;
    ConnectionPool connectionPool = null;
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file")
              .withRequiredArg()
              .describedAs("hardware_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file")
              .withRequiredArg()
              .describedAs("partition_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfWritersOpt =
          parser.accepts("numberOfWriters", "The number of writers that issue put request")
              .withRequiredArg()
              .describedAs("The number of writers")
              .ofType(Integer.class)
              .defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> minBlobSizeOpt =
          parser.accepts("minBlobSizeInBytes", "The minimum size of the blob that can be put")
              .withRequiredArg()
              .describedAs("The minimum blob size in bytes")
              .ofType(Integer.class)
              .defaultsTo(51200);

      ArgumentAcceptingOptionSpec<Integer> maxBlobSizeOpt =
          parser.accepts("maxBlobSizeInBytes", "The maximum size of the blob that can be put")
              .withRequiredArg()
              .describedAs("The maximum blob size in bytes")
              .ofType(Integer.class)
              .defaultsTo(4194304);

      ArgumentAcceptingOptionSpec<Integer> writesPerSecondOpt =
          parser.accepts("writesPerSecond", "The rate at which writes need to be performed")
              .withRequiredArg()
              .describedAs("The number of writes per second")
              .ofType(Integer.class)
              .defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Long> measurementIntervalOpt =
          parser.accepts("measurementInterval", "The interval in second to report performance result")
              .withOptionalArg()
              .describedAs("The CPU time spent for putting blobs, not wall time")
              .ofType(Long.class)
              .defaultsTo(300L);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging")
              .withOptionalArg()
              .describedAs("Enable verbose logging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt =
          parser.accepts("sslEnabledDatacenters", "Datacenters to which ssl should be enabled")
              .withOptionalArg()
              .describedAs("Comma separated list")
              .ofType(String.class)
              .defaultsTo("");

      ArgumentAcceptingOptionSpec<String> sslKeystorePathOpt = parser.accepts("sslKeystorePath", "SSL key store path")
          .withOptionalArg()
          .describedAs("The file path of SSL key store")
          .defaultsTo("")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslKeystoreTypeOpt = parser.accepts("sslKeystoreType", "SSL key store type")
          .withOptionalArg()
          .describedAs("The type of SSL key store")
          .defaultsTo("")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslTruststorePathOpt =
          parser.accepts("sslTruststorePath", "SSL trust store path")
              .withOptionalArg()
              .describedAs("The file path of SSL trust store")
              .defaultsTo("")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslKeystorePasswordOpt =
          parser.accepts("sslKeystorePassword", "SSL key store password")
              .withOptionalArg()
              .describedAs("The password of SSL key store")
              .defaultsTo("")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslKeyPasswordOpt = parser.accepts("sslKeyPassword", "SSL key password")
          .withOptionalArg()
          .describedAs("The password of SSL private key")
          .defaultsTo("")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslTruststorePasswordOpt =
          parser.accepts("sslTruststorePassword", "SSL trust store password")
              .withOptionalArg()
              .describedAs("The password of SSL trust store")
              .defaultsTo("")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslCipherSuitesOpt =
          parser.accepts("sslCipherSuites", "SSL enabled cipher suites")
              .withOptionalArg()
              .describedAs("Comma separated list")
              .defaultsTo("TLS_RSA_WITH_AES_128_CBC_SHA")
              .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec> listOpt = new ArrayList<>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

      ToolUtils.ensureOrExit(listOpt, options, parser);

      long measurementIntervalNs = options.valueOf(measurementIntervalOpt) * SystemTime.NsPerSec;
      ToolUtils.validateSSLOptions(options, parser, sslEnabledDatacentersOpt, sslKeystorePathOpt, sslKeystoreTypeOpt,
          sslTruststorePathOpt, sslKeystorePasswordOpt, sslKeyPasswordOpt, sslTruststorePasswordOpt);

      String sslEnabledDatacenters = options.valueOf(sslEnabledDatacentersOpt);
      Properties sslProperties;
      if (sslEnabledDatacenters.length() != 0) {
        sslProperties = ToolUtils.createSSLProperties(sslEnabledDatacenters, options.valueOf(sslKeystorePathOpt),
            options.valueOf(sslKeystoreTypeOpt), options.valueOf(sslKeystorePasswordOpt),
            options.valueOf(sslKeyPasswordOpt), options.valueOf(sslTruststorePathOpt),
            options.valueOf(sslTruststorePasswordOpt), options.valueOf(sslCipherSuitesOpt));
      } else {
        sslProperties = new Properties();
      }

      ToolUtils.addClusterMapProperties(sslProperties);

      int numberOfWriters = options.valueOf(numberOfWritersOpt);
      int writesPerSecond = options.valueOf(writesPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      int minBlobSize = options.valueOf(minBlobSizeOpt);
      int maxBlobSize = options.valueOf(maxBlobSizeOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalWrites = new AtomicLong(0);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(sslProperties));
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      File logFile = new File(System.getProperty("user.dir"), "writeperflog");
      blobIdsWriter = new FileWriter(logFile);
      File performanceFile = new File(System.getProperty("user.dir"), "writeperfresult");
      performanceWriter = new FileWriter(performanceFile);

      final CountDownLatch latch = new CountDownLatch(numberOfWriters);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            latch.await();
            System.out.println("Total writes : " + totalWrites.get() + "  Total time taken : " + totalTimeTaken.get()
                + " Nano Seconds  Average time taken per write "
                + ((double) totalTimeTaken.get()) / SystemTime.NsPerSec / totalWrites.get() + " Seconds");
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });

      Throttler throttler = new Throttler(writesPerSecond, 100, true, SystemTime.getInstance());
      Thread[] threadIndexPerf = new Thread[numberOfWriters];
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(new VerifiableProperties(new Properties()));
      VerifiableProperties vProps = new VerifiableProperties(sslProperties);
      SSLConfig sslConfig = new SSLConfig(vProps);
      clusterMapConfig = new ClusterMapConfig(vProps);
      connectionPool =
          new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, new MetricRegistry());
      connectionPool.start();

      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i] = new Thread(
            new ServerWritePerfRun(i, throttler, shutdown, latch, minBlobSize, maxBlobSize, blobIdsWriter,
                performanceWriter, totalTimeTaken, totalWrites, measurementIntervalNs, enableVerboseLogging, map,
                connectionPool));
        threadIndexPerf[i].start();
      }
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i].join();
      }
    } catch (Exception e) {
      System.err.println("Error on exit " + e);
    } finally {
      if (blobIdsWriter != null) {
        try {
          blobIdsWriter.close();
        } catch (Exception e) {
          System.out.println("Error when closing the blob id writer");
        }
      }
      if (performanceWriter != null) {
        try {
          performanceWriter.close();
        } catch (Exception e) {
          System.out.println("Error when closing the performance writer");
        }
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }

  public static class ServerWritePerfRun implements Runnable {

    private final Throttler throttler;
    private final AtomicBoolean isShutdown;
    private final CountDownLatch latch;
    private final ClusterMap clusterMap;
    private final Random rand = new Random();
    private final int minBlobSize;
    private final int maxBlobSize;
    private final FileWriter blobIdWriter;
    private final FileWriter performanceWriter;
    private final AtomicLong totalTimeTaken;
    private final AtomicLong totalWrites;
    private final long measurementIntervalNs;
    private final boolean enableVerboseLogging;
    private final int threadIndex;
    private final ConnectionPool connectionPool;
    private final short blobIdVersion;

    public ServerWritePerfRun(int threadIndex, Throttler throttler, AtomicBoolean isShutdown, CountDownLatch latch,
        int minBlobSize, int maxBlobSize, FileWriter blobIdWriter, FileWriter performanceWriter,
        AtomicLong totalTimeTaken, AtomicLong totalWrites, long measurementIntervalNs, boolean enableVerboseLogging,
        ClusterMap clusterMap, ConnectionPool connectionPool) {
      this.threadIndex = threadIndex;
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
      this.clusterMap = clusterMap;
      this.minBlobSize = minBlobSize;
      this.maxBlobSize = maxBlobSize;
      this.blobIdWriter = blobIdWriter;
      this.performanceWriter = performanceWriter;
      this.totalTimeTaken = totalTimeTaken;
      this.totalWrites = totalWrites;
      this.measurementIntervalNs = measurementIntervalNs;
      this.enableVerboseLogging = enableVerboseLogging;
      this.connectionPool = connectionPool;
      Properties props = new Properties();
      props.setProperty("router.hostname", "localhost");
      props.setProperty("router.datacenter.name", "localDC");
      blobIdVersion = new RouterConfig(new VerifiableProperties(props)).routerBlobidCurrentVersion;
    }

    public void run() {
      try {
        System.out.println("Starting write server performance");
        long numberOfPuts = 0;
        long timePassedInNanoSeconds = 0;
        long maxLatencyInNanoSeconds = 0;
        long minLatencyInNanoSeconds = Long.MAX_VALUE;
        long totalLatencyInNanoSeconds = 0;
        ArrayList<Long> latenciesForPutBlobs = new ArrayList<Long>();
        while (!isShutdown.get()) {

          int randomNum = rand.nextInt((maxBlobSize - minBlobSize) + 1) + minBlobSize;
          byte[] blob = new byte[randomNum];
          byte[] usermetadata = new byte[new Random().nextInt(1024)];
          BlobProperties props =
              new BlobProperties(randomNum, "test", Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false);
          ConnectedChannel channel = null;
          try {
            List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
            int index = (int) getRandomLong(rand, partitionIds.size());
            PartitionId partitionId = partitionIds.get(index);
            BlobId blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
                props.getAccountId(), props.getContainerId(), partitionId, false, BlobId.BlobDataType.DATACHUNK);
            PutRequest putRequest =
                new PutRequest(0, "perf", blobId, props, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(blob),
                    props.getBlobSize(), BlobType.DataBlob, null);
            ReplicaId replicaId = partitionId.getReplicaIds().get(0);
            Port port = replicaId.getDataNodeId().getPortToConnectTo();
            channel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);
            long startTime = SystemTime.getInstance().nanoseconds();
            channel.send(putRequest);
            PutResponse putResponse = PutResponse.readFrom(new DataInputStream(channel.receive().getInputStream()));
            if (putResponse.getError() != ServerErrorCode.No_Error) {
              throw new UnexpectedException("error " + putResponse.getError());
            }
            long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTime;
            timePassedInNanoSeconds += latencyPerBlob;
            latenciesForPutBlobs.add(latencyPerBlob);
            blobIdWriter.write("Blob-" + blobId + "\n");
            totalWrites.incrementAndGet();
            if (enableVerboseLogging) {
              System.out.println("Time taken to put blob id " + blobId + " in ms " + latencyPerBlob / SystemTime.NsPerMs
                  + " for blob of size " + blob.length);
            }
            numberOfPuts++;
            if (maxLatencyInNanoSeconds < latencyPerBlob) {
              maxLatencyInNanoSeconds = latencyPerBlob;
            }
            if (minLatencyInNanoSeconds > latencyPerBlob) {
              minLatencyInNanoSeconds = latencyPerBlob;
            }
            totalLatencyInNanoSeconds += latencyPerBlob;
            if (timePassedInNanoSeconds >= measurementIntervalNs) {
              Collections.sort(latenciesForPutBlobs);
              int index99 = (int) (latenciesForPutBlobs.size() * 0.99) - 1;
              int index95 = (int) (latenciesForPutBlobs.size() * 0.95) - 1;
              String message = threadIndex + "," + numberOfPuts + ","
                  + (double) latenciesForPutBlobs.get(index99) / SystemTime.NsPerSec + ","
                  + (double) latenciesForPutBlobs.get(index95) / SystemTime.NsPerSec + "," + (
                  ((double) totalLatencyInNanoSeconds) / SystemTime.NsPerSec / numberOfPuts);
              System.out.println(message);
              performanceWriter.write(message + "\n");
              numberOfPuts = 0;
              timePassedInNanoSeconds = 0;
              latenciesForPutBlobs.clear();
              maxLatencyInNanoSeconds = 0;
              minLatencyInNanoSeconds = Long.MAX_VALUE;
              totalLatencyInNanoSeconds = 0;
            }
            totalTimeTaken.addAndGet(latencyPerBlob);
            throttler.maybeThrottle(1);
          } catch (Exception e) {
            System.out.println("Exception when putting blob " + e);
          } finally {
            if (channel != null) {
              connectionPool.checkInConnection(channel);
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Exiting server write perf thread " + e);
      } finally {
        latch.countDown();
      }
    }
  }
}
