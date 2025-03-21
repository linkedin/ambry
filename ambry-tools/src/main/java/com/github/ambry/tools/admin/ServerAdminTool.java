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

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SendWithCorrelationId;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.AdminResponseWithContent;
import com.github.ambry.protocol.BlobIndexAdminRequest;
import com.github.ambry.protocol.BlobStoreControlAction;
import com.github.ambry.protocol.BlobStoreControlAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminResponse;
import com.github.ambry.protocol.ForceDeleteAdminRequest;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.tools.util.ToolRequestResponseUtil;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to support admin related operations on Ambry server
 * Currently supports:
 * 1. Get of either BlobProperties, UserMetadata or blob data for a particular blob from a particular storage node.
 * 2. Triggering of compaction of a particular partition on a particular node.
 * 3. Get catchup status of peers for a particular blob.
 * 4. Stop/Start a particular blob store via BlobStoreControl operation.
 */
public class ServerAdminTool implements Closeable {
  private static final int POLL_TIMEOUT_MS = 10;
  private static final int OPERATION_TIMEOUT_MS = 5000;
  private static final String CLIENT_ID = "ServerAdminTool";
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerAdminTool.class);

  private final Http2NetworkClient networkClient;
  private final AtomicInteger correlationId = new AtomicInteger(0);
  private final Time time = SystemTime.getInstance();
  private final ClusterMap clusterMap;

  /**
   * The different operations supported by the tool.
   */
  private enum Operation {
    GetBlobProperties,
    GetUserMetadata,
    GetBlob,
    BlobIndex,
    ForceDelete,
    TriggerCompaction,
    RequestControl,
    ReplicationControl,
    CatchupStatus,
    BlobStoreControl
  }

  /**
   * Config values associated with the tool.
   */
  private static class ServerAdminToolConfig {

    /**
     * The type of operation.
     * Operations are: GetBlobProperties,GetUserMetadata,GetBlob,BlobIndex,ForceDelete,TriggerCompaction,RequestControl,
     * ReplicationControl,CatchupStatus,BlobStoreControl
     */
    @Config("type.of.operation")
    final Operation typeOfOperation;

    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    /**
     * The hostname of the target server as it appears in the partition layout.
     */
    @Config("hostname")
    @Default("localhost")
    final String hostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to).
     */
    @Config("port")
    @Default("6667")
    final int port;

    /**
     * The id of the blob to operate on (if applicable).
     * Applicable for: GetBlobProperties,GetUserMetadata,GetBlob
     */
    @Config("blob.id")
    @Default("")
    final String blobId;

    /**
     * life version of the Blob
     * Applicable for: ForceDelete
     */
    @Config("life.version")
    @Default("0")
    final short lifeVersion;

    /**
     * The get option to use to do the get operation (if applicable)
     * Applicable for: GetBlobProperties,GetUserMetadata,GetBlob
     */
    @Config("get.option")
    @Default("None")
    final GetOption getOption;

    /**
     * Comma separated list of the string representations of the partitions to operate on (if applicable).
     * Some requests (TriggerCompaction) will not work with an empty list but some requests treat empty lists as "all
     * partitions" (RequestControl,ReplicationControl,CatchupStatus,BlobStoreControl).
     * Applicable for: TriggerCompaction,RequestControl,ReplicationControl,CatchupStatus,BlobStoreControl
     */
    @Config("partition.ids")
    @Default("")
    final String[] partitionIds;

    /**
     * The type of request to control
     * Applicable for: RequestControl
     */
    @Config("request.type.to.control")
    @Default("PutRequest")
    final RequestOrResponseType requestTypeToControl;

    /**
     * Enables the request type or replication if {@code true}. Disables if {@code false}.
     * Applicable for: RequestControl,ReplicationControl
     */
    @Config("enable.state")
    @Default("true")
    final boolean enableState;

    /**
     * The comma separated names of the datacenters from which replication should be controlled.
     * Applicable for: ReplicationControl
     */
    @Config("replication.origins")
    @Default("")
    final String[] origins;

    /**
     * The acceptable lag in bytes in case of catchup status requests
     * Applicable for: CatchupStatus
     */
    @Config("acceptable.lag.in.bytes")
    @Default("0")
    final long acceptableLagInBytes;

    /**
     * The number of replicas of each partition that have to be within "acceptable.lag.in.bytes" in case of catchup
     * status requests. The min of this value or the total count of replicas -1 is considered.
     * Applicable for: CatchupStatus,BlobStoreControl
     */
    @Config("num.replicas.caught.up.per.partition")
    @Default("Short.MAX_VALUE")
    final short numReplicasCaughtUpPerPartition;

    @Config("store.control.request.type")
    @Default("StartStore")
    final BlobStoreControlAction storeControlRequestType;

    /**
     * Path of the file where the data from certain operations will output. For example, the blob from GetBlob and the
     * user metadata from GetUserMetadata will be written into this file.
     */
    @Config("data.output.file.path")
    @Default("/tmp/ambryResult.out")
    final String dataOutputFilePath;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    ServerAdminToolConfig(VerifiableProperties verifiableProperties) {
      typeOfOperation = Operation.valueOf(verifiableProperties.getString("type.of.operation"));
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getIntInRange("port", 6667, 1, 65535);
      blobId = verifiableProperties.getString("blob.id", "");
      lifeVersion = verifiableProperties.getShortInRange("life.version", (short) 0, (short) 0, (short) 100);
      getOption = GetOption.valueOf(verifiableProperties.getString("get.option", "None"));
      partitionIds = verifiableProperties.getString("partition.ids", "").split(",");
      requestTypeToControl =
          RequestOrResponseType.valueOf(verifiableProperties.getString("request.type.to.control", "PutRequest"));
      enableState = verifiableProperties.getBoolean("enable.state", true);
      origins = verifiableProperties.getString("replication.origins", "").split(",");
      acceptableLagInBytes = verifiableProperties.getLongInRange("acceptable.lag.in.bytes", 0, 0, Long.MAX_VALUE);
      numReplicasCaughtUpPerPartition =
          verifiableProperties.getShortInRange("num.replicas.caught.up.per.partition", Short.MAX_VALUE, (short) 0,
              Short.MAX_VALUE);
      storeControlRequestType =
          BlobStoreControlAction.valueOf(verifiableProperties.getString("store.control.request.type"));
      dataOutputFilePath = verifiableProperties.getString("data.output.file.path", "/tmp/ambryResult.out");
    }
  }

  /**
   * Runs the server admin tool
   * @param args associated arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    ServerAdminToolConfig config = new ServerAdminToolConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap();
    SSLFactory sslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    ServerAdminTool serverAdminTool = new ServerAdminTool(clusterMap, sslFactory, verifiableProperties);
    File file = new File(config.dataOutputFilePath);
    if (!file.exists() && !file.createNewFile()) {
      throw new IllegalStateException("Could not create " + file);
    }
    FileOutputStream outputFileStream = new FileOutputStream(config.dataOutputFilePath);
    DataNodeId dataNodeId = clusterMap.getDataNodeId(config.hostname, config.port);
    if (dataNodeId == null) {
      throw new IllegalArgumentException(
          "Could not find a data node corresponding to " + config.hostname + ":" + config.port);
    }
    switch (config.typeOfOperation) {
      case GetBlobProperties:
        BlobId blobId = new BlobId(config.blobId, clusterMap);
        Pair<ServerErrorCode, BlobProperties> bpResponse =
            serverAdminTool.getBlobProperties(dataNodeId, blobId, config.getOption, clusterMap);
        if (bpResponse.getFirst() == ServerErrorCode.NoError) {
          LOGGER.info("Blob properties for {} from {}: {}", blobId, dataNodeId, bpResponse.getSecond());
        } else {
          LOGGER.error("Failed to get blob properties for {} from {} with option {}. Error code is {}", blobId,
              dataNodeId, config.getOption, bpResponse.getFirst());
        }
        break;
      case GetUserMetadata:
        blobId = new BlobId(config.blobId, clusterMap);
        Pair<ServerErrorCode, ByteBuffer> umResponse =
            serverAdminTool.getUserMetadata(dataNodeId, blobId, config.getOption, clusterMap);
        if (umResponse.getFirst() == ServerErrorCode.NoError) {
          writeBufferToFile(umResponse.getSecond(), outputFileStream);
          LOGGER.info("User metadata for {} from {} written to {}", blobId, dataNodeId, config.dataOutputFilePath);
        } else {
          LOGGER.error("Failed to get user metadata for {} from {} with option {}. Error code is {}", blobId,
              dataNodeId, config.getOption, umResponse.getFirst());
        }
        break;
      case GetBlob:
        blobId = new BlobId(config.blobId, clusterMap);
        Pair<ServerErrorCode, BlobData> bResponse =
            serverAdminTool.getBlob(dataNodeId, blobId, config.getOption, clusterMap);
        if (bResponse.getFirst() == ServerErrorCode.NoError) {
          LOGGER.info("Blob type of {} from {} is {}", blobId, dataNodeId, bResponse.getSecond().getBlobType());
          ByteBuf buffer = bResponse.getSecond().content();
          try {
            writeByteBufToFile(buffer, outputFileStream);
          } finally {
            buffer.release();
          }
          LOGGER.info("Blob data for {} from {} written to {}", blobId, dataNodeId, config.dataOutputFilePath);
        } else {
          LOGGER.error("Failed to get blob data for {} from {} with option {}. Error code is {}", blobId, dataNodeId,
              config.getOption, bResponse.getFirst());
        }
        break;
      case BlobIndex:
        blobId = new BlobId(config.blobId, clusterMap);
        Pair<ServerErrorCode, String> biResponse = serverAdminTool.getBlobIndex(dataNodeId, blobId);
        if (biResponse.getFirst() == ServerErrorCode.NoError) {
          LOGGER.info("Blob index values of {} from {} is {}", blobId, dataNodeId, biResponse.getSecond());
        } else {
          LOGGER.error("Failed to get blob index values for {} from {}. Error code is {}", blobId, dataNodeId,
              biResponse.getFirst());
        }
        break;
      case ForceDelete:
        blobId = new BlobId(config.blobId, clusterMap);
        short lifeVersion = config.lifeVersion;
        ServerErrorCode errorCode = serverAdminTool.forceDeleteBlob(dataNodeId, blobId, lifeVersion);
        if (errorCode == ServerErrorCode.NoError) {
          LOGGER.info("Force delete {} {} from {} is successful.", blobId, lifeVersion, dataNodeId);
        } else {
          LOGGER.error("Failed to run force delete {} {} from {}. Error code is {}", blobId, lifeVersion, dataNodeId,
              errorCode);
        }
        break;
      case TriggerCompaction:
        if (config.partitionIds.length > 0 && !config.partitionIds[0].isEmpty()) {
          for (String partitionIdStr : config.partitionIds) {
            PartitionId partitionId = getPartitionIdFromStr(partitionIdStr, clusterMap);
            errorCode = serverAdminTool.triggerCompaction(dataNodeId, partitionId);
            if (errorCode == ServerErrorCode.NoError) {
              LOGGER.info("Compaction has been triggered for {} on {}", partitionId, dataNodeId);
            } else {
              LOGGER.error("From {}, received server error code {} for trigger compaction request on {}", dataNodeId,
                  errorCode, partitionId);
            }
          }
        } else {
          LOGGER.error("There were no partitions provided to trigger compaction on");
        }
        break;
      case RequestControl:
        if (config.partitionIds.length > 0 && !config.partitionIds[0].isEmpty()) {
          for (String partitionIdStr : config.partitionIds) {
            PartitionId partitionId = getPartitionIdFromStr(partitionIdStr, clusterMap);
            sendRequestControlRequest(serverAdminTool, dataNodeId, partitionId, config.requestTypeToControl,
                config.enableState);
          }
        } else {
          LOGGER.info("No partition list provided. Requesting enable status of {} to be set to {} on all partitions",
              config.requestTypeToControl, config.enableState);
          sendRequestControlRequest(serverAdminTool, dataNodeId, null, config.requestTypeToControl, config.enableState);
        }
        break;
      case ReplicationControl:
        List<String> origins = Collections.emptyList();
        if (config.origins.length > 0 && !config.origins[0].isEmpty()) {
          origins = Arrays.asList(config.origins);
        }
        if (config.partitionIds.length > 0 && !config.partitionIds[0].isEmpty()) {
          for (String partitionIdStr : config.partitionIds) {
            PartitionId partitionId = getPartitionIdFromStr(partitionIdStr, clusterMap);
            sendReplicationControlRequest(serverAdminTool, dataNodeId, partitionId, origins, config.enableState);
          }
        } else {
          LOGGER.info("No partition list provided. Requesting enable status for replication from {} to be set to {} on "
              + "all partitions", origins.isEmpty() ? "all DCs" : origins, config.enableState);
          sendReplicationControlRequest(serverAdminTool, dataNodeId, null, origins, config.enableState);
        }
        break;
      case CatchupStatus:
        if (config.partitionIds.length > 0 && !config.partitionIds[0].isEmpty()) {
          for (String partitionIdStr : config.partitionIds) {
            PartitionId partitionId = getPartitionIdFromStr(partitionIdStr, clusterMap);
            Pair<ServerErrorCode, Boolean> response =
                serverAdminTool.isCaughtUp(dataNodeId, partitionId, config.acceptableLagInBytes,
                    config.numReplicasCaughtUpPerPartition);
            if (response.getFirst() == ServerErrorCode.NoError) {
              LOGGER.info("Replicas are {} within {} bytes for {}", response.getSecond() ? "" : "NOT",
                  config.acceptableLagInBytes, partitionId);
            } else {
              LOGGER.error("From {}, received server error code {} for request for catchup status of {}", dataNodeId,
                  response.getFirst(), partitionId);
            }
          }
        } else {
          Pair<ServerErrorCode, Boolean> response =
              serverAdminTool.isCaughtUp(dataNodeId, null, config.acceptableLagInBytes,
                  config.numReplicasCaughtUpPerPartition);
          if (response.getFirst() == ServerErrorCode.NoError) {
            LOGGER.info("Replicas are {} within {} bytes for all partitions", response.getSecond() ? "" : "NOT",
                config.acceptableLagInBytes);
          } else {
            LOGGER.error("From {}, received server error code {} for request for catchup status of all partitions",
                dataNodeId, response.getFirst());
          }
        }
        break;
      case BlobStoreControl:
        if (config.partitionIds.length > 0 && !config.partitionIds[0].isEmpty()) {
          for (String partitionIdStr : config.partitionIds) {
            PartitionId partitionId = getPartitionIdFromStr(partitionIdStr, clusterMap);
            sendBlobStoreControlRequest(serverAdminTool, dataNodeId, partitionId,
                config.numReplicasCaughtUpPerPartition, config.storeControlRequestType);
          }
        } else {
          LOGGER.error("There were no partitions provided to be controlled (Start/Stop)");
        }
        break;
      default:
        throw new IllegalStateException("Recognized but unsupported operation: " + config.typeOfOperation);
    }
    serverAdminTool.close();
    outputFileStream.close();
    clusterMap.close();
    System.out.println("Server admin tool is safely closed");
    System.exit(0);
  }

  /**
   * Gets the {@link PartitionId} in the {@code clusterMap} whose string representation matches {@code partitionIdStr}.
   * @param partitionIdStr the string representation of the partition required.
   * @param clusterMap the {@link ClusterMap} to use to list and process {@link PartitionId}s.
   * @return the {@link PartitionId} in the {@code clusterMap} whose string repr matches {@code partitionIdStr}.
   * {@code null} if {@code partitionIdStr} is {@code null}.
   * @throws IllegalArgumentException if there is no @link PartitionId} in the {@code clusterMap} whose string repr
   * matches {@code partitionIdStr}.
   */
  public static PartitionId getPartitionIdFromStr(String partitionIdStr, ClusterMap clusterMap) {
    if (partitionIdStr == null) {
      return null;
    }
    PartitionId targetPartitionId = null;
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      if (partitionId.isEqual(partitionIdStr)) {
        targetPartitionId = partitionId;
        break;
      }
    }
    if (targetPartitionId == null) {
      throw new IllegalArgumentException("Partition Id is not valid: [" + partitionIdStr + "]");
    }
    return targetPartitionId;
  }

  /**
   * Writes the content of {@code buffer} into {@link ServerAdminToolConfig#dataOutputFilePath}.
   * @param buffer the {@link ByteBuffer} whose content needs to be written.
   * @param outputFileStream the {@link FileOutputStream} to write to.
   * @throws IOException
   */
  private static void writeBufferToFile(ByteBuffer buffer, FileOutputStream outputFileStream) throws IOException {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    outputFileStream.write(bytes);
  }

  /**
   * Writes the content of {@code buffer} into {@link ServerAdminToolConfig#dataOutputFilePath}.
   * @param buffer the {@link ByteBuf} whose content needs to be written.
   * @param outputFileStream the {@link FileOutputStream} to write to.
   * @throws IOException
   */
  private static void writeByteBufToFile(ByteBuf buffer, FileOutputStream outputFileStream) throws IOException {
    buffer.readBytes(outputFileStream, buffer.readableBytes());
  }

  /**
   * Sends a {@link RequestControlAdminRequest} to {@code dataNodeId} to set enable status of {@code toControl} to
   * {@code enable} for {@code partitionId}.
   * @param serverAdminTool the {@link ServerAdminTool} instance to use.
   * @param dataNodeId the {@link DataNodeId} to send the request to.
   * @param partitionId the partition id (string) on which the operation will take place. Can be {@code null}.
   * @param toControl the {@link RequestOrResponseType} to control.
   * @param enable the enable (or disable) status required for {@code toControl}.
   * @throws IOException
   * @throws TimeoutException
   */
  private static void sendRequestControlRequest(ServerAdminTool serverAdminTool, DataNodeId dataNodeId,
      PartitionId partitionId, RequestOrResponseType toControl, boolean enable) throws IOException, TimeoutException {
    ServerErrorCode errorCode = serverAdminTool.controlRequest(dataNodeId, partitionId, toControl, enable);
    if (errorCode == ServerErrorCode.NoError) {
      LOGGER.info("{} enable state has been set to {} for {} on {}", toControl, enable, partitionId, dataNodeId);
    } else {
      LOGGER.error("From {}, received server error code {} for request to set enable state {} for {} on {}", dataNodeId,
          errorCode, enable, toControl, partitionId);
    }
  }

  /**
   * Sends a {@link ReplicationControlAdminRequest} to {@code dataNodeId} to set enable status of replication from
   * {@code origins} to {@code enable} for {@code partitionId}.
   * @param serverAdminTool the {@link ServerAdminTool} instance to use.
   * @param dataNodeId the {@link DataNodeId} to send the request to.
   * @param partitionId the partition id  on which the operation will take place. Can be {@code null}.
   * @param origins the names of the datacenters from which replication should be controlled.
   * @param enable the enable (or disable) status required for replication control.
   * @throws IOException
   * @throws TimeoutException
   */
  private static void sendReplicationControlRequest(ServerAdminTool serverAdminTool, DataNodeId dataNodeId,
      PartitionId partitionId, List<String> origins, boolean enable) throws IOException, TimeoutException {
    ServerErrorCode errorCode = serverAdminTool.controlReplication(dataNodeId, partitionId, origins, enable);
    if (errorCode == ServerErrorCode.NoError) {
      LOGGER.info("Enable state of replication from {} has been set to {} for {} on {}",
          origins.isEmpty() ? "all DCs" : origins, enable, partitionId == null ? "all partitions" : partitionId,
          dataNodeId);
    } else {
      LOGGER.error(
          "From {}, received server error code {} for request to set enable state {} for replication from {} for {}",
          dataNodeId, errorCode, enable, origins, partitionId);
    }
  }

  /**
   * Sends a {@link BlobStoreControlAdminRequest} to {@code dataNodeId} to set enable status of controlling BlobStore
   * to {@code enable} for {@code partitionId}.
   * @param serverAdminTool the {@link ServerAdminTool} instance to use.
   * @param dataNodeId the {@link DataNodeId} to send the request to.
   * @param partitionId the partition id  on which the operation will take place. Can be {@code null}.
   * @param numReplicasCaughtUpPerPartition the minimum number of peers should catch up with the partition.
   * @param storeControlRequestType the type of control operation that will performed on certain store.
   * @throws IOException
   * @throws TimeoutException
   */
  private static void sendBlobStoreControlRequest(ServerAdminTool serverAdminTool, DataNodeId dataNodeId,
      PartitionId partitionId, short numReplicasCaughtUpPerPartition, BlobStoreControlAction storeControlRequestType)
      throws IOException, TimeoutException {
    ServerErrorCode errorCode =
        serverAdminTool.controlBlobStore(dataNodeId, partitionId, numReplicasCaughtUpPerPartition,
            storeControlRequestType);
    if (errorCode == ServerErrorCode.NoError) {
      LOGGER.info("{} control request has been performed for {} on {}", storeControlRequestType, partitionId,
          dataNodeId);
    } else {
      LOGGER.error("From {}, received server error code {} for {} request that performed on {}", dataNodeId, errorCode,
          storeControlRequestType, partitionId);
    }
  }

  /**
   * Creates an instance of the server admin tool
   * @param clusterMap the {@link ClusterMap} to use
   * @param sslFactory the {@link SSLFactory} to use
   * @param verifiableProperties the {@link VerifiableProperties} to use for config.
   * @throws Exception
   */
  public ServerAdminTool(ClusterMap clusterMap, SSLFactory sslFactory, VerifiableProperties verifiableProperties)
      throws Exception {
    Http2ClientMetrics metrics = new Http2ClientMetrics(clusterMap.getMetricRegistry());
    Http2ClientConfig config = new Http2ClientConfig(verifiableProperties);
    this.clusterMap = clusterMap;
    networkClient = new Http2NetworkClientFactory(metrics, config, sslFactory, time).getNetworkClient();
  }

  /**
   * Releases all resources associated with the tool.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    networkClient.close();
  }

  /**
   * Gets {@link BlobProperties} for {@code blobId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to send with the {@link GetRequest}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the {@link ServerErrorCode} and {@link BlobProperties} of {@code blobId}.
   * @throws Exception
   */
  public Pair<ServerErrorCode, BlobProperties> getBlobProperties(DataNodeId dataNodeId, BlobId blobId,
      GetOption getOption, ClusterMap clusterMap) throws Exception {
    Pair<Pair<ServerErrorCode, InputStream>, ResponseInfo> response =
        getGetResponse(dataNodeId, blobId, MessageFormatFlags.BlobProperties, getOption, clusterMap);
    InputStream stream = response.getFirst().getSecond();
    BlobProperties blobProperties = stream != null ? MessageFormatRecord.deserializeBlobProperties(stream) : null;
    response.getSecond().release();
    return new Pair<>(response.getFirst().getFirst(), blobProperties);
  }

  /**
   * Gets user metadata for {@code blobId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to send with the {@link GetRequest}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the {@link ServerErrorCode} and user metadata as a {@link ByteBuffer} for {@code blobId}
   * @throws Exception
   */
  public Pair<ServerErrorCode, ByteBuffer> getUserMetadata(DataNodeId dataNodeId, BlobId blobId, GetOption getOption,
      ClusterMap clusterMap) throws Exception {
    Pair<Pair<ServerErrorCode, InputStream>, ResponseInfo> response =
        getGetResponse(dataNodeId, blobId, MessageFormatFlags.BlobUserMetadata, getOption, clusterMap);
    InputStream stream = response.getFirst().getSecond();
    ByteBuffer userMetadata = stream != null ? MessageFormatRecord.deserializeUserMetadata(stream) : null;
    response.getSecond().release();
    return new Pair<>(response.getFirst().getFirst(), userMetadata);
  }

  /**
   * Gets blob data for {@code blobId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to send with the {@link GetRequest}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the {@link ServerErrorCode} and {@link BlobData} for {@code blobId}
   * @throws Exception
   */
  public Pair<ServerErrorCode, BlobData> getBlob(DataNodeId dataNodeId, BlobId blobId, GetOption getOption,
      ClusterMap clusterMap) throws Exception {
    Pair<Pair<ServerErrorCode, InputStream>, ResponseInfo> response =
        getGetResponse(dataNodeId, blobId, MessageFormatFlags.Blob, getOption, clusterMap);
    InputStream stream = response.getFirst().getSecond();
    BlobData blobData = stream != null ? MessageFormatRecord.deserializeBlob(stream) : null;
    response.getSecond().release();
    return new Pair<>(response.getFirst().getFirst(), blobData);
  }

  /**
   * Gets all data for {@code blobId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to send with the {@link GetRequest}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return the {@link ServerErrorCode} and {@link BlobAll} for {@code blobId}
   * @throws Exception
   */
  public Pair<ServerErrorCode, BlobAll> getAll(DataNodeId dataNodeId, BlobId blobId, GetOption getOption,
      ClusterMap clusterMap, StoreKeyFactory storeKeyFactory) throws Exception {
    Pair<Pair<ServerErrorCode, InputStream>, ResponseInfo> response =
        getGetResponse(dataNodeId, blobId, MessageFormatFlags.All, getOption, clusterMap);
    InputStream stream = response.getFirst().getSecond();
    BlobAll blobAll = stream != null ? MessageFormatRecord.deserializeBlobAll(stream, storeKeyFactory) : null;
    response.getSecond().release();
    return new Pair<>(response.getFirst().getFirst(), blobAll);
  }

  /**
   * Get the blob index values for {@code blobId}
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @return the {@link ServerErrorCode} and the json serialized content of blob index values.
   * @throws Exception
   */
  public Pair<ServerErrorCode, String> getBlobIndex(DataNodeId dataNodeId, BlobId blobId) throws Exception {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobIndex, null, correlationId.incrementAndGet(), CLIENT_ID);
    BlobIndexAdminRequest blobIndexAdminRequest = new BlobIndexAdminRequest(blobId, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, blobId.getPartition(), blobIndexAdminRequest);
    AdminResponseWithContent adminResponse =
        AdminResponseWithContent.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    ServerErrorCode errorCode = adminResponse.getError();
    String content = "";
    if (errorCode == ServerErrorCode.NoError) {
      content = new String(adminResponse.getContent());
    }
    return new Pair<>(errorCode, content);
  }

  /**
   * Force Delete a not existing blob.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to create tombstone.
   * @param lifeVersion life version of the blob.
   * @return the {@link ServerErrorCode}
   * @throws Exception
   */
  public ServerErrorCode forceDeleteBlob(DataNodeId dataNodeId, BlobId blobId, short lifeVersion) throws Exception {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.ForceDelete, null, correlationId.incrementAndGet(), CLIENT_ID);
    ForceDeleteAdminRequest forceDeleteAdminRequest = new ForceDeleteAdminRequest(blobId, lifeVersion, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, blobId.getPartition(), forceDeleteAdminRequest);
    AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return adminResponse.getError();
  }

  /**
   * Triggers compaction on {@code dataNodeId} for the partition defined in {@code partitionIdStr}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} to compact.
   * @return the {@link ServerErrorCode} that is returned.
   * @throws IOException
   * @throws TimeoutException
   */
  public ServerErrorCode triggerCompaction(DataNodeId dataNodeId, PartitionId partitionId)
      throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.TriggerCompaction, partitionId, correlationId.incrementAndGet(),
            CLIENT_ID);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, adminRequest);
    AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return adminResponse.getError();
  }

  /**
   * Sends a {@link RequestControlAdminRequest} to set the enable state of {@code toControl} on {@code partitionIdStr}
   * to {@code enable} in {@code dataNodeId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} to control requests to. Can be {@code null}.
   * @param toControl the {@link RequestOrResponseType} to control.
   * @param enable the enable (or disable) status required for {@code toControl}.
   * @return the {@link ServerErrorCode} that is returned.
   * @throws IOException
   * @throws TimeoutException
   */
  public ServerErrorCode controlRequest(DataNodeId dataNodeId, PartitionId partitionId, RequestOrResponseType toControl,
      boolean enable) throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.RequestControl, partitionId, correlationId.incrementAndGet(),
            CLIENT_ID);
    RequestControlAdminRequest controlRequest = new RequestControlAdminRequest(toControl, enable, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, controlRequest);
    AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return adminResponse.getError();
  }

  /**
   * Sends a {@link ReplicationControlAdminRequest} to enable/disable replication from {@code origins} for
   * {@code partitionIdStr} in {@code dataNodeId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} to control replication for. Can be {@code null}.
   * @param origins the names of the datacenters from which replication should be controlled.
   * @param enable the enable (or disable) status required for replication from {@code origins}.
   * @return the {@link ServerErrorCode} that is returned.
   * @throws IOException
   * @throws TimeoutException
   */
  public ServerErrorCode controlReplication(DataNodeId dataNodeId, PartitionId partitionId, List<String> origins,
      boolean enable) throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.ReplicationControl, partitionId, correlationId.incrementAndGet(),
            CLIENT_ID);
    ReplicationControlAdminRequest controlRequest = new ReplicationControlAdminRequest(origins, enable, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, controlRequest);
    AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return adminResponse.getError();
  }

  /**
   * Sends a {@link BlobStoreControlAdminRequest} to start or stop a store associated with {@code partitionId}
   * on {@code dataNodeId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} to start or stop.
   * @param numReplicasCaughtUpPerPartition the minimum number of peers should catch up with partition if the store is
   *                                        being stopped
   * @param storeControlRequestType the type of control operation that will performed on certain store.
   * @return the {@link ServerErrorCode} that is returned.
   * @throws IOException
   * @throws TimeoutException
   */
  private ServerErrorCode controlBlobStore(DataNodeId dataNodeId, PartitionId partitionId,
      short numReplicasCaughtUpPerPartition, BlobStoreControlAction storeControlRequestType)
      throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, correlationId.incrementAndGet(),
            CLIENT_ID);
    BlobStoreControlAdminRequest controlRequest =
        new BlobStoreControlAdminRequest(numReplicasCaughtUpPerPartition, storeControlRequestType, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, controlRequest);
    AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return adminResponse.getError();
  }

  /**
   * Sends a {@link CatchupStatusAdminRequest} for {@code partitionIdStr} to {@code dataNodeId}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} to check catchup status for. If {@code null}, status is for all
   *                    partitions on {@code dataNodeId}
   * @param acceptableLagInBytes that lag in bytes that is considered OK.
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within {@code acceptableLagInBytes}
   *                                        (per partition). The min of this value or the total count of replicas - 1 is
   *                                        considered.
   * @return the {@link ServerErrorCode} and the catchup status that is returned if the error code is
   *          {@link ServerErrorCode#NoError}, otherwise {@code false}.
   * @throws IOException
   * @throws TimeoutException
   */
  public Pair<ServerErrorCode, Boolean> isCaughtUp(DataNodeId dataNodeId, PartitionId partitionId,
      long acceptableLagInBytes, short numReplicasCaughtUpPerPartition) throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.CatchupStatus, partitionId, correlationId.incrementAndGet(),
            CLIENT_ID);
    CatchupStatusAdminRequest catchupStatusRequest =
        new CatchupStatusAdminRequest(acceptableLagInBytes, numReplicasCaughtUpPerPartition, adminRequest);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, catchupStatusRequest);
    CatchupStatusAdminResponse adminResponse =
        CatchupStatusAdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
    response.release();
    return new Pair<>(adminResponse.getError(),
        adminResponse.getError() == ServerErrorCode.NoError && adminResponse.isCaughtUp());
  }

  /**
   * Sends a {@link GetRequest} based on the provided parameters and returns the response stream if the request was
   * successful. {@code null} otherwise.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param blobId the {@link BlobId} to operate on.
   * @param flags the {@link MessageFormatFlags} associated with the {@link GetRequest}.
   * @param getOption the {@link GetOption} to send with the {@link GetRequest}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the {@link ServerErrorCode} and response stream if the request was successful. {@code null} for the
   * response stream otherwise.
   * @throws Exception
   */
  private Pair<Pair<ServerErrorCode, InputStream>, ResponseInfo> getGetResponse(DataNodeId dataNodeId, BlobId blobId,
      MessageFormatFlags flags, GetOption getOption, ClusterMap clusterMap) throws Exception {
    PartitionId partitionId = blobId.getPartition();
    PartitionRequestInfo partitionRequestInfo =
        new PartitionRequestInfo(partitionId, Collections.singletonList(blobId));
    List<PartitionRequestInfo> partitionRequestInfos = new ArrayList<>();
    partitionRequestInfos.add(partitionRequestInfo);
    GetRequest getRequest =
        new GetRequest(correlationId.incrementAndGet(), CLIENT_ID, flags, partitionRequestInfos, getOption);
    ResponseInfo response = sendRequestGetResponse(dataNodeId, partitionId, getRequest);
    return new Pair<>(ToolRequestResponseUtil.decodeGetResponse(response, clusterMap).getFirst(), response);
  }

  /**
   * Sends {@code request} to {@code dataNodeId} and returns the response as a {@link ByteBuffer}.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionId the {@link PartitionId} associated with request.
   * @param request the request to send.
   * @return the response as a {@link ResponseInfo} if the response was successfully received. {@code null} otherwise.
   * @throws TimeoutException
   */
  private ResponseInfo sendRequestGetResponse(DataNodeId dataNodeId, PartitionId partitionId,
      SendWithCorrelationId request) throws TimeoutException {
    ReplicaId replicaId = ToolRequestResponseUtil.getReplicaFromNode(dataNodeId, partitionId, clusterMap);
    String hostname = dataNodeId.getHostname();
    Port port = dataNodeId.getPortToConnectTo();
    String identifier = hostname + ":" + port.getPort();
    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replicaId, null);
    List<RequestInfo> requestInfos = Collections.singletonList(requestInfo);
    ResponseInfo responseInfo = null;
    long startTimeMs = time.milliseconds();
    do {
      if (time.milliseconds() - startTimeMs > OPERATION_TIMEOUT_MS) {
        throw new TimeoutException(identifier + ": Operation did not complete within " + OPERATION_TIMEOUT_MS + " ms");
      }
      List<ResponseInfo> responseInfos =
          networkClient.sendAndPoll(requestInfos, Collections.emptySet(), POLL_TIMEOUT_MS);
      if (responseInfos.size() > 1) {
        // May need to relax this check because response list may contain more than 1 response
        throw new IllegalStateException("Received more than one response even though a single request was sent");
      } else if (!responseInfos.isEmpty()) {
        responseInfo = responseInfos.get(0);
      }
      requestInfos = Collections.emptyList();
    } while (responseInfo == null);
    if (responseInfo.getError() != null) {
      throw new IllegalStateException(
          identifier + ": Encountered error while trying to send request - " + responseInfo.getError());
    }
    return responseInfo;
  }
}
