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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.tools.util.ToolUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Tool to support admin related operations
 * Operations supported so far:
 * List Replicas for a given blobid
 */
public class AdminTool {
  private final ConnectionPool connectionPool;

  public AdminTool(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }

  public static void main(String args[]) {
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

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt = parser.accepts("typeOfOperation",
          "The type of operation to execute - LIST_REPLICAS/GET_BLOB/GET_BLOB_PROPERTIES/GET_USERMETADATA")
          .withRequiredArg()
          .describedAs("The type of file")
          .ofType(String.class)
          .defaultsTo("GET");

      ArgumentAcceptingOptionSpec<String> ambryBlobIdOpt =
          parser.accepts("ambryBlobId", "The blob id to execute get on")
              .withRequiredArg()
              .describedAs("The blob id")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> includeExpiredBlobsOpt =
          parser.accepts("includeExpiredBlob", "Included expired blobs too")
              .withRequiredArg()
              .describedAs("Whether to include expired blobs while querying or not")
              .defaultsTo("false")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt =
          parser.accepts("sslEnabledDatacenters", "Datacenters to which ssl should be enabled")
              .withOptionalArg()
              .describedAs("Comma separated list")
              .defaultsTo("")
              .ofType(String.class);

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
      listOpt.add(typeOfOperationOpt);
      listOpt.add(ambryBlobIdOpt);
      ToolUtils.ensureOrExit(listOpt, options, parser);

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
      Properties connectionPoolProperties = ToolUtils.createConnectionPoolProperties();
      ConnectionPoolConfig connectionPoolConfig =
          new ConnectionPoolConfig(new VerifiableProperties(connectionPoolProperties));
      VerifiableProperties vProps = new VerifiableProperties(sslProperties);
      SSLConfig sslConfig = new SSLConfig(vProps);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(vProps);
      connectionPool =
          new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, new MetricRegistry());
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath, clusterMapConfig);

      String blobIdStr = options.valueOf(ambryBlobIdOpt);
      AdminTool adminTool = new AdminTool(connectionPool);
      BlobId blobId = new BlobId(blobIdStr, map);
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      boolean includeExpiredBlobs = Boolean.parseBoolean(options.valueOf(includeExpiredBlobsOpt));
      if (typeOfOperation.equalsIgnoreCase("LIST_REPLICAS")) {
        List<ReplicaId> replicaIdList = adminTool.getReplicas(blobId);
        for (ReplicaId replicaId : replicaIdList) {
          System.out.println(replicaId);
        }
      } else if (typeOfOperation.equalsIgnoreCase("GET_BLOB")) {
        adminTool.getBlob(blobId, map, includeExpiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("GET_BLOB_PROPERTIES")) {
        adminTool.getBlobProperties(blobId, map, includeExpiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("GET_USERMETADATA")) {
        adminTool.getUserMetadata(blobId, map, includeExpiredBlobs);
      } else {
        System.out.println("Invalid Type of Operation ");
        System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    } finally {
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }

  public List<ReplicaId> getReplicas(BlobId blobId) {
    return blobId.getPartition().getReplicaIds();
  }

  public BlobProperties getBlobProperties(BlobId blobId, ClusterMap map, boolean expiredBlobs) {
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    BlobProperties blobProperties = null;
    for (ReplicaId replicaId : replicas) {
      try {
        blobProperties = getBlobProperties(blobId, map, replicaId, expiredBlobs);
        break;
      } catch (Exception e) {
        System.out.println("Get blob properties error ");
        e.printStackTrace();
      }
    }
    return blobProperties;
  }

  public BlobProperties getBlobProperties(BlobId blobId, ClusterMap clusterMap, ReplicaId replicaId,
      boolean expiredBlobs)
      throws MessageFormatException, IOException, ConnectionPoolTimeoutException, InterruptedException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    ConnectedChannel connectedChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOption getOption = (expiredBlobs) ? GetOption.Include_Expired_Blobs : GetOption.None;

    try {
      Port port = replicaId.getDataNodeId().getPortToConnectTo();
      connectedChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);

      GetRequest getRequest =
          new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobProperties,
              partitionRequestInfos, getOption);
      System.out.println("Get Request to verify replica blob properties : " + getRequest);
      GetResponse getResponse = null;

      getResponse = BlobValidator.getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob properties is null ");
        System.out.println(blobId + " STATE FAILED");
        throw new IOException(" Get Response from Stream to verify replica blob properties is null ");
      }
      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      System.out.println("Get Response from Stream to verify replica blob properties : " + getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println("getBlobProperties error on response " + getResponse.getError() + " error code on partition "
            + serverResponseCode + " ambryReplica " + replicaId.getDataNodeId().getHostname() + " port "
            + port.toString() + " blobId " + blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          return null;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          return null;
        } else {
          return null;
        }
      } else {
        BlobProperties properties = MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
        System.out.println(
            "Blob Properties : Content Type : " + properties.getContentType() + ", OwnerId : " + properties.getOwnerId()
                + ", Size : " + properties.getBlobSize() + ", CreationTimeInMs : " + properties.getCreationTimeInMs()
                + ", ServiceId : " + properties.getServiceId() + ", TTL : " + properties.getTimeToLiveInSeconds());
        return properties;
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw e;
    } finally {
      if (connectedChannel != null) {
        connectionPool.checkInConnection(connectedChannel);
      }
    }
  }

  public BlobData getBlob(BlobId blobId, ClusterMap map, boolean expiredBlobs)
      throws MessageFormatException, IOException {
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    BlobData blobData = null;
    for (ReplicaId replicaId : replicas) {
      try {
        blobData = getBlob(blobId, map, replicaId, expiredBlobs);
        break;
      } catch (Exception e) {
        System.out.println("Get blob error ");
        e.printStackTrace();
      }
    }
    return blobData;
  }

  public BlobData getBlob(BlobId blobId, ClusterMap clusterMap, ReplicaId replicaId, boolean expiredBlobs)
      throws MessageFormatException, IOException, ConnectionPoolTimeoutException, InterruptedException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    ConnectedChannel connectedChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOption getOption = (expiredBlobs) ? GetOption.Include_Expired_Blobs : GetOption.None;

    try {
      Port port = replicaId.getDataNodeId().getPortToConnectTo();
      connectedChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);

      GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.Blob,
          partitionRequestInfos, getOption);
      System.out.println("Get Request to get blob : " + getRequest);
      GetResponse getResponse = null;
      getResponse = BlobValidator.getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob is null ");
        System.out.println(blobId + " STATE FAILED");
        throw new IOException(" Get Response from Stream to verify replica blob properties is null ");
      }
      System.out.println("Get Response to get blob : " + getResponse.getError());
      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println(
            "blob get error on response " + getResponse.getError() + " error code on partition " + serverResponseCode
                + " ambryReplica " + replicaId.getDataNodeId().getHostname() + " port " + port.toString() + " blobId "
                + blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          return null;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          return null;
        } else {
          return null;
        }
      } else {
        return MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw e;
    } finally {
      if (connectedChannel != null) {
        connectionPool.checkInConnection(connectedChannel);
      }
    }
  }

  public ByteBuffer getUserMetadata(BlobId blobId, ClusterMap map, boolean expiredBlobs)
      throws MessageFormatException, IOException {
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    ByteBuffer userMetadata = null;
    for (ReplicaId replicaId : replicas) {
      try {
        userMetadata = getUserMetadata(blobId, map, replicaId, expiredBlobs);
        break;
      } catch (Exception e) {
        System.out.println("Get user metadata error ");
        e.printStackTrace();
      }
    }
    return userMetadata;
  }

  public ByteBuffer getUserMetadata(BlobId blobId, ClusterMap clusterMap, ReplicaId replicaId, boolean expiredBlobs)
      throws MessageFormatException, IOException, ConnectionPoolTimeoutException, InterruptedException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    ConnectedChannel connectedChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOption getOption = (expiredBlobs) ? GetOption.Include_Expired_Blobs : GetOption.None;

    try {
      Port port = replicaId.getDataNodeId().getPortToConnectTo();
      connectedChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), port, 10000);

      GetRequest getRequest =
          new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobUserMetadata,
              partitionRequestInfos, getOption);
      System.out.println("Get Request to check blob usermetadata : " + getRequest);
      GetResponse getResponse = null;
      getResponse = BlobValidator.getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob usermetadata is null ");
        System.out.println(blobId + " STATE FAILED");
        throw new IOException(" Get Response from Stream to verify replica blob properties is null ");
      }
      System.out.println("Get Response to check blob usermetadata : " + getResponse.getError());

      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println("usermetadata get error on response " + getResponse.getError() + " error code on partition "
            + serverResponseCode + " ambryReplica " + replicaId.getDataNodeId().getHostname() + " port "
            + port.toString() + " blobId " + blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          return null;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          return null;
        } else {
          return null;
        }
      } else {
        ByteBuffer userMetadata = MessageFormatRecord.deserializeUserMetadata(getResponse.getInputStream());
        System.out.println("Usermetadata deserialized. Size " + userMetadata.capacity());
        return userMetadata;
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw e;
    } finally {
      if (connectedChannel != null) {
        connectionPool.checkInConnection(connectedChannel);
      }
    }
  }
}
