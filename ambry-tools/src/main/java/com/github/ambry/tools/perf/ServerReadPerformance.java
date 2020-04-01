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
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 *
 */
public class ServerReadPerformance {
  public static void main(String args[]) {
    ConnectionPool connectionPool = null;
    FileWriter writer = null;
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> logToReadOpt =
          parser.accepts("logToRead", "The log that needs to be replayed for traffic")
              .withRequiredArg()
              .describedAs("log_to_read")
              .ofType(String.class);

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

      ArgumentAcceptingOptionSpec<Integer> readsPerSecondOpt =
          parser.accepts("readsPerSecond", "The rate at which reads need to be performed")
              .withRequiredArg()
              .describedAs("The number of reads per second")
              .ofType(Integer.class)
              .defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Long> measurementIntervalOpt =
          parser.accepts("measurementInterval", "The interval in second to report performance result")
              .withOptionalArg()
              .describedAs("The CPU time spent for getting blobs, not wall time")
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
      listOpt.add(logToReadOpt);
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
      String logToRead = options.valueOf(logToReadOpt);

      int readsPerSecond = options.valueOf(readsPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      File logFile = new File(System.getProperty("user.dir"), "readperfresult");
      writer = new FileWriter(logFile);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(sslProperties));
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalReads = new AtomicLong(0);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            String message = "Total reads : " + totalReads.get() + "  Total time taken : " + totalTimeTaken.get()
                + " Nano Seconds  Average time taken per read "
                + ((double) totalTimeTaken.get()) / SystemTime.NsPerSec / totalReads.get() + " Seconds";
            System.out.println(message);
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });
      final BufferedReader br = new BufferedReader(new FileReader(logToRead));
      Throttler throttler = new Throttler(readsPerSecond, 100, true, SystemTime.getInstance());
      String line;
      ConnectedChannel channel = null;
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(new VerifiableProperties(new Properties()));
      VerifiableProperties vProps = new VerifiableProperties(sslProperties);
      SSLConfig sslConfig = new SSLConfig(vProps);
      clusterMapConfig = new ClusterMapConfig(vProps);
      connectionPool =
          new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, new MetricRegistry());
      long totalNumberOfGetBlobs = 0;
      long totalLatencyForGetBlobs = 0;
      ArrayList<Long> latenciesForGetBlobs = new ArrayList<Long>();
      long maxLatencyForGetBlobs = 0;
      long minLatencyForGetBlobs = Long.MAX_VALUE;

      while ((line = br.readLine()) != null) {
        String[] id = line.split("-");
        BlobData blobData = null;
        BlobId blobId = new BlobId(id[1], map);
        ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
        blobIds.add(blobId);
        for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
          long startTimeGetBlob = 0;
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
          try {
            partitionRequestInfoList.clear();
            PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
            partitionRequestInfoList.add(partitionRequestInfo);
            GetRequest getRequest =
                new GetRequest(1, "getperf", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
            Port port = replicaId.getDataNodeId().getPortToConnectTo();
            channel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);
            startTimeGetBlob = SystemTime.getInstance().nanoseconds();
            channel.send(getRequest);
            InputStream receiveStream = channel.receive().getInputStream();
            GetResponse getResponse = GetResponse.readFrom(new DataInputStream(receiveStream), map);
            blobData = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
            long sizeRead = 0;
            byte[] outputBuffer = new byte[(int) blobData.getSize()];
            ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
            ByteBuf buffer = blobData.content();
            try {
              buffer.readBytes(streamOut, (int) blobData.getSize());
            } finally {
              buffer.release();
            }
            long latencyPerBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            totalTimeTaken.addAndGet(latencyPerBlob);
            latenciesForGetBlobs.add(latencyPerBlob);
            totalReads.incrementAndGet();
            totalNumberOfGetBlobs++;
            totalLatencyForGetBlobs += latencyPerBlob;
            if (enableVerboseLogging) {
              System.out.println(
                  "Time taken to get blob id " + blobId + " in ms " + latencyPerBlob / SystemTime.NsPerMs);
            }
            if (latencyPerBlob > maxLatencyForGetBlobs) {
              maxLatencyForGetBlobs = latencyPerBlob;
            }
            if (latencyPerBlob < minLatencyForGetBlobs) {
              minLatencyForGetBlobs = latencyPerBlob;
            }
            if (totalLatencyForGetBlobs >= measurementIntervalNs) {
              Collections.sort(latenciesForGetBlobs);
              int index99 = (int) (latenciesForGetBlobs.size() * 0.99) - 1;
              int index95 = (int) (latenciesForGetBlobs.size() * 0.95) - 1;
              String message =
                  totalNumberOfGetBlobs + "," + (double) latenciesForGetBlobs.get(index99) / SystemTime.NsPerSec + ","
                      + (double) latenciesForGetBlobs.get(index95) / SystemTime.NsPerSec + "," + (
                      (double) totalLatencyForGetBlobs / SystemTime.NsPerSec / totalNumberOfGetBlobs);
              System.out.println(message);
              writer.write(message + "\n");
              totalLatencyForGetBlobs = 0;
              latenciesForGetBlobs.clear();
              totalNumberOfGetBlobs = 0;
              maxLatencyForGetBlobs = 0;
              minLatencyForGetBlobs = Long.MAX_VALUE;
            }

            partitionRequestInfoList.clear();
            partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
            partitionRequestInfoList.add(partitionRequestInfo);
            GetRequest getRequestProperties =
                new GetRequest(1, "getperf", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                    GetOption.None);
            long startTimeGetBlobProperties = SystemTime.getInstance().nanoseconds();
            channel.send(getRequestProperties);
            InputStream receivePropertyStream = channel.receive().getInputStream();
            GetResponse getResponseProperty = GetResponse.readFrom(new DataInputStream(receivePropertyStream), map);
            BlobProperties blobProperties =
                MessageFormatRecord.deserializeBlobProperties(getResponseProperty.getInputStream());
            long endTimeGetBlobProperties = SystemTime.getInstance().nanoseconds() - startTimeGetBlobProperties;

            partitionRequestInfoList.clear();
            partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
            partitionRequestInfoList.add(partitionRequestInfo);
            GetRequest getRequestUserMetadata =
                new GetRequest(1, "getperf", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
                    GetOption.None);

            long startTimeGetBlobUserMetadata = SystemTime.getInstance().nanoseconds();
            channel.send(getRequestUserMetadata);
            InputStream receiveUserMetadataStream = channel.receive().getInputStream();
            GetResponse getResponseUserMetadata =
                GetResponse.readFrom(new DataInputStream(receiveUserMetadataStream), map);
            ByteBuffer userMetadata =
                MessageFormatRecord.deserializeUserMetadata(getResponseUserMetadata.getInputStream());
            long endTimeGetBlobUserMetadata = SystemTime.getInstance().nanoseconds() - startTimeGetBlobUserMetadata;
            // delete the blob
            DeleteRequest deleteRequest = new DeleteRequest(0, "perf", blobId, System.currentTimeMillis());
            channel.send(deleteRequest);
            DeleteResponse deleteResponse =
                DeleteResponse.readFrom(new DataInputStream(channel.receive().getInputStream()));
            if (deleteResponse.getError() != ServerErrorCode.No_Error) {
              throw new UnexpectedException("error " + deleteResponse.getError());
            }
            throttler.maybeThrottle(1);
          } finally {
            if (channel != null) {
              connectionPool.checkInConnection(channel);
              channel = null;
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error in server read performance " + e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          System.out.println("Error when closing writer");
        }
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }
}
