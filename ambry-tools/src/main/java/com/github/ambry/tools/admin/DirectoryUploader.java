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
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
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
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Supports uploading files from a directory to a specific partition on a single colo
 * Steps:
 * 1. For each file, generate the blob id with the given partition
 * 2. Generate Blob props, usermetadata and blob content
 * 3. PutRequest is made to every replica in the specified partition and datacenter,
 *    or a single replica if a hostname and port is specified
 * 4. Write the output to out file
 */
public class DirectoryUploader {
  private PartitionId partitionId;
  private DataNodeId dataNodeId = null;
  private final ConnectionPool connectionPool;
  private final short blobIdVersion;

  private DirectoryUploader() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "localDC");
    ToolUtils.addClusterMapProperties(properties);
    VerifiableProperties vProps = new VerifiableProperties(properties);
    ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(vProps);
    SSLConfig sslConfig = new SSLConfig(vProps);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(vProps);
    blobIdVersion = new RouterConfig(vProps).routerBlobidCurrentVersion;
    connectionPool =
        new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, new MetricRegistry());
    connectionPool.start();
  }

  private void setPartitionId(ClusterMap clusterMap, String partitionStr, boolean enableVerboseLogging) {
    for (PartitionId writablePartition : clusterMap.getWritablePartitionIds(null)) {
      if (writablePartition.toString().equalsIgnoreCase(partitionStr)) {
        partitionId = writablePartition;
        break;
      }
    }
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition " + partitionStr + " is not writable/available");
    }

    if (enableVerboseLogging) {
      System.out.println("Chosen partition " + partitionId);
    }
  }

  /**
   * Specify a data node to write to by hostname and port.  If this is specified, only this node will be written to.
   * This node must be in the defined partition and data center.
   *
   * @param clusterMap used to find data node corresponding to hostname and port
   * @param hostname the hostname of the data node
   * @param port the port number of the data node
   * @param enableVerboseLogging true to emit verbose log messages
   */
  private void setDataNodeId(ClusterMap clusterMap, String hostname, int port, boolean enableVerboseLogging) {
    dataNodeId = clusterMap.getDataNodeId(hostname, port);
    if (dataNodeId == null) {
      throw new IllegalArgumentException("Node at " + hostname + ":" + port + " not found");
    }
    boolean inPartition = false;
    for (ReplicaId replicaId : partitionId.getReplicaIds()) {
      if (replicaId.getDataNodeId().equals(dataNodeId)) {
        inPartition = true;
        break;
      }
    }
    if (!inPartition) {
      throw new IllegalArgumentException("Node at " + hostname + ":" + port + " not in partition " + partitionId);
    }
    if (enableVerboseLogging) {
      System.out.println("Chosen node " + dataNodeId);
    }
  }

  public void walkDirectoryToCreateBlobs(String path, FileWriter writer, String datacenter, byte datacenterId,
      boolean enableVerboseLogging) throws InterruptedException {

    File root = new File(path);
    File[] list = root.listFiles();
    Random random = new Random();

    if (list == null) {
      return;
    }

    for (File f : list) {
      if (!f.isDirectory()) {
        System.out.println("File :" + f.getAbsoluteFile());
        if (f.length() > Integer.MAX_VALUE) {
          System.out.println("File length is " + f.length());
          throw new IllegalArgumentException("File length is " + f.length() + "; files larger than " + Integer.MAX_VALUE
              + " cannot be put using this tool.");
        }
        BlobProperties props =
            new BlobProperties(f.length(), "migration", Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
                false);
        byte[] usermetadata = new byte[1];
        FileInputStream stream = null;
        try {
          int replicaCount = 0;
          BlobId blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, datacenterId, props.getAccountId(),
              props.getContainerId(), partitionId, false, BlobId.BlobDataType.DATACHUNK);
          List<ReplicaId> successList = new ArrayList<>();
          List<ReplicaId> failureList = new ArrayList<>();
          for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
            if (replicaId.getDataNodeId().getDatacenterName().equalsIgnoreCase(datacenter)) {
              // If a node was specified, only write to that node instead of all nodes of a partition
              if (dataNodeId != null && !dataNodeId.equals(replicaId.getDataNodeId())) {
                continue;
              }
              replicaCount += 1;
              try {
                stream = new FileInputStream(f);
                AtomicInteger correlationId = new AtomicInteger(random.nextInt(100000));
                if (writeToAmbryReplica(props, ByteBuffer.wrap(usermetadata), stream, blobId, replicaId, correlationId,
                    enableVerboseLogging)) {
                  successList.add(replicaId);
                } else {
                  failureList.add(replicaId);
                }
              } catch (Exception e) {
                System.out.println("Exception thrown on replica " + replicaId);
              } finally {
                if (stream != null) {
                  stream.close();
                }
              }
            }
          }
          System.out.println(
              "Successfuly put blob " + blobId + " to " + successList + ", but failed in " + failureList + "\n");
          if (successList.size() == replicaCount) {
            writer.write(
                "blobId|" + blobId.getID() + "|source|" + f.getAbsolutePath() + "|fileSize|" + f.length() + "|\n");
          }
          // failed blobs are not written to the out file
        } catch (FileNotFoundException e) {
          System.out.println("File not found path : " + f.getAbsolutePath() + " exception : " + e);
        } catch (IOException e) {
          System.out.println("IOException when writing to migration log " + e);
        } finally {
          try {
            if (stream != null) {
              stream.close();
            }
          } catch (Exception e) {
            System.out.println("Error while closing file stream " + e);
          }
        }
      }
    }
  }

  /**
   * Adding a blob to a particular server.
   * @param stream stream containing the actual blob
   * @param blobId blob id generated by ambry
   * @param replicaId replica to which the blob has to be added
   * @param correlationId coorelation id to uniquely identify the call
   * @return true if write succeeds, else false
   * @throws Exception
   */
  private boolean writeToAmbryReplica(BlobProperties blobProperties, ByteBuffer userMetaData, InputStream stream,
      BlobId blobId, ReplicaId replicaId, AtomicInteger correlationId, boolean enableVerboseLogging) throws Exception {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    ConnectedChannel blockingChannel = null;

    try {
      blockingChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(),
          new Port(replicaId.getDataNodeId().getPort(), PortType.PLAINTEXT), 100000);
      int size = (int) blobProperties.getBlobSize();
      ByteBufferInputStream blobStream = new ByteBufferInputStream(stream, size);
      PutRequest putRequest =
          new PutRequest(correlationId.incrementAndGet(), "consumerThread", blobId, blobProperties, userMetaData,
              Unpooled.wrappedBuffer(blobStream.getByteBuffer()), size, BlobType.DataBlob, null);

      if (enableVerboseLogging) {
        System.out.println("Put Request to a replica : " + putRequest + " for blobId " + blobId);
      }
      PutResponse putResponse = getPutResponseFromStream(blockingChannel, putRequest, connectionPool);
      if (putResponse == null) {
        System.out.println("PutResponse to a replica " + replicaId + " failed with null Response for blobId " + blobId);
        blockingChannel = null;
        return false;
      } else {
        if (putResponse.getError() != ServerErrorCode.No_Error
            && putResponse.getError() != ServerErrorCode.Blob_Already_Exists) {
          System.out.println(
              "PutResponse to a replica " + replicaId + " failed with Error code  " + putResponse.getError()
                  + " for blobId " + blobId);
          return false;
        }
        return true;
      }
    } finally {
      if (stream != null) {
        stream.close();
      }
      if (blockingChannel != null) {
        connectionPool.checkInConnection(blockingChannel);
      }
    }
  }

  /**
   * Method to send PutRequest and receive PutResponse to and from a blocking channel. If it fails, blocking channel is destroyed
   *
   * @param blockingChannel BlockingChannel used to send and receive request and response
   * @param putRequest      PutRequest which has to be sent through the blockingchannel
   * @param connectionPool  ConnectionPool from which connections are checked out
   * @return PutResponse the response from the PutRequest made
   */
  private static PutResponse getPutResponseFromStream(ConnectedChannel blockingChannel, PutRequest putRequest,
      ConnectionPool connectionPool) {
    PutResponse putResponse = null;
    try {
      blockingChannel.send(putRequest);
      InputStream stream = blockingChannel.receive().getInputStream();
      putResponse = PutResponse.readFrom(new DataInputStream(stream));
    } catch (Exception exception) {
      connectionPool.destroyConnection(blockingChannel);
      System.out.println("Unknown Exception Error" + exception);
      return null;
    }
    return putResponse;
  }

  public static void main(String args[]) {
    FileWriter writer = null;
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> rootDirectoryOpt =
          parser.accepts("rootDirectory", "The root folder from which all the files will be migrated")
              .withRequiredArg()
              .describedAs("root_directory")
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

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging")
              .withOptionalArg()
              .describedAs("Enable verbose logging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      ArgumentAcceptingOptionSpec<String> partitionOpt =
          parser.accepts("partition", "The partition to which the put calls to be made against")
              .withRequiredArg()
              .describedAs("partition")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> datacenterOpt =
          parser.accepts("datacenter", "The datacenter to which the put calls to be made against")
              .withRequiredArg()
              .describedAs("datacenter")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "The file to which output should be redirected")
              .withRequiredArg()
              .describedAs("outFile")
              .ofType(String.class);

      // Optional arguments for defining a specific node to write to.
      ArgumentAcceptingOptionSpec<String> nodeHostnameOpt =
          parser.accepts("nodeHostname", "The hostname of the node to put to (if specifying single node)")
              .withOptionalArg()
              .describedAs("nodeHostname")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> nodePortOpt =
          parser.accepts("nodePort", "The port of the node to put to (if specifying single node)")
              .withOptionalArg()
              .describedAs("nodePort")
              .ofType(Integer.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec> listOpt = new ArrayList<>();
      listOpt.add(rootDirectoryOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(partitionOpt);
      listOpt.add(datacenterOpt);
      listOpt.add(outFileOpt);

      ToolUtils.ensureOrExit(listOpt, options, parser);

      System.out.println("Starting to parse arguments");
      boolean enableVerboseLogging = options.has(verboseLoggingOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      String rootDirectory = options.valueOf(rootDirectoryOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed rootdir " + rootDirectory);
      }
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed Hardware layout " + hardwareLayoutPath);
      }
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed partition layout " + partitionLayoutPath);
      }
      String partition = options.valueOf(partitionOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed partition " + partition);
      }
      partition = "Partition[" + partition + "]";
      String datacenter = options.valueOf(datacenterOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed datacenter " + datacenter);
      }
      String nodeHostname = options.valueOf(nodeHostnameOpt);
      if (enableVerboseLogging && nodeHostname != null) {
        System.out.println("Parsed node hostname " + nodeHostname);
      }
      Integer nodePort = options.valueOf(nodePortOpt);
      if (enableVerboseLogging && nodePort != null) {
        System.out.println("Parsed node port " + nodePort);
      }
      String outFile = options.valueOf(outFileOpt);
      if (enableVerboseLogging) {
        System.out.println("Parsed outFile " + outFile);
        System.out.println("Done parsing all args");
      }
      VerifiableProperties vprops = new VerifiableProperties((new Properties()));
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(vprops);
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      File logFile = new File(outFile);
      writer = new FileWriter(logFile);
      DirectoryUploader directoryUploader = new DirectoryUploader();
      directoryUploader.setPartitionId(map, partition, enableVerboseLogging);
      if (nodeHostname != null && nodePort != null) {
        directoryUploader.setDataNodeId(map, nodeHostname, nodePort, enableVerboseLogging);
      }
      directoryUploader.walkDirectoryToCreateBlobs(rootDirectory, writer, datacenter, map.getLocalDatacenterId(),
          enableVerboseLogging);
    } catch (Exception e) {
      System.err.println("Error on exit " + e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          System.out.println("Error when closing the writer");
        }
      }
    }
  }
}
