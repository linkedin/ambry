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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to validate the presence of equivalence of blobs across replicas.
 * Features supported so far are:
 * Get blob from all replicas and compare the record across replicas for a list of blob ids.
 * Get blob from all replicas in a datacenter and compare the record across replicas for a list of blob ids.
 * Get blob from a specific replica and ensure that it deserializes correctly for a list of blob ids.

 */
public class BlobValidator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlobValidator.class);

  private final Throttler throttler;
  private final ServerAdminTool serverAdminTool;

  /**
   * The different operations supported by BlobValidator.
   */
  private enum Operation {
    ValidateBlobOnReplica, ValidateBlobOnDatacenter, ValidateBlobOnAllReplicas
  }

  /**
   * Config values associated with the BlobValidator.
   */
  private static class BlobValidatorConfig {

    /**
     * The path to the hardware layout file.
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file.
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * The type of operation.
     * Operations are: ValidateBlobOnReplica,ValidateBlobOnDatacenter,ValidateBlobOnAllReplicas
     */
    @Config("type.of.operation")
    final Operation typeOfOperation;

    /**
     * The path of the file that contains blob ids. The blob ids should be comma separated with no spaces.
     * One of this or "blob.ids" must be present.
     */
    @Config("blob.ids.file.path")
    final String blobIdsFilePath;

    /**
     * Comma separated list of the blob ids to operate on.
     * One of this or "blob.ids.file.path" must be present.
     */
    @Config("blob.ids")
    final String[] blobIds;

    /**
     * The hostname of the target server as it appears in the partition layout if applicable.
     * Applicable to: ValidateBlobOnReplica
     */
    @Config("hostname")
    @Default("localhost")
    final String hostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to) if applicable.
     * Applicable to: ValidateBlobOnReplica
     */
    @Config("port")
    @Default("6667")
    final int port;

    /**
     * The target datacenter if applicable
     * Applicable to: ValidateBlobOnDatacenter
     */
    @Config("datacenter")
    @Default("")
    final String datacenter;

    /**
     * The get option to use to do the get operation (if applicable)
     * Applicable for: GetBlobProperties,GetUserMetadata,GetBlob
     */
    @Config("get.option")
    @Default("None")
    final GetOption getOption;

    /**
     * Replicas to contact per second (for throttling)
     */
    @Config("replicas.to.contact.per.sec")
    @Default("1")
    final int replicasToContactPerSec;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    BlobValidatorConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
      typeOfOperation = Operation.valueOf(verifiableProperties.getString("type.of.operation"));
      if (verifiableProperties.containsKey("blob.ids.file.path")) {
        blobIdsFilePath = verifiableProperties.getString("blob.ids.file.path");
        blobIds = null;
      } else {
        blobIdsFilePath = null;
        blobIds = verifiableProperties.getString("blob.ids").split(",");
      }
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getIntInRange("port", 6667, 1, 65535);
      datacenter = verifiableProperties.getString("datacenter", "");
      getOption = GetOption.valueOf(verifiableProperties.getString("get.option", "None"));
      replicasToContactPerSec =
          verifiableProperties.getIntInRange("replicas.to.contact.per.sec", 1, 1, Integer.MAX_VALUE);
    }
  }

  /**
   * Runs the BlobValidator
   * @param args associated arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    BlobValidatorConfig config = new BlobValidatorConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap();
    List<BlobId> blobIds = getBlobIds(config, clusterMap);
    SSLFactory sslFactory = !clusterMapConfig.clusterMapSslEnabledDatacenters.isEmpty() ? SSLFactory.getNewInstance(
        new SSLConfig(verifiableProperties)) : null;
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    BlobValidator validator =
        new BlobValidator(clusterMap, config.replicasToContactPerSec, sslFactory, verifiableProperties);
    LOGGER.info("Validation starting");
    switch (config.typeOfOperation) {
      case ValidateBlobOnAllReplicas:
        Map<BlobId, List<String>> mismatchDetailsMap =
            validator.validateBlobsOnAllReplicas(blobIds, config.getOption, clusterMap, storeKeyFactory);
        logMismatches(mismatchDetailsMap);
        break;
      case ValidateBlobOnDatacenter:
        if (config.datacenter.isEmpty() || !clusterMap.hasDatacenter(config.datacenter)) {
          throw new IllegalArgumentException("Please provide a valid datacenter");
        }
        mismatchDetailsMap =
            validator.validateBlobsOnDatacenter(config.datacenter, blobIds, config.getOption, clusterMap,
                storeKeyFactory);
        logMismatches(mismatchDetailsMap);
        break;
      case ValidateBlobOnReplica:
        DataNodeId dataNodeId = clusterMap.getDataNodeId(config.hostname, config.port);
        if (dataNodeId == null) {
          throw new IllegalArgumentException(
              "Could not find a data node corresponding to " + config.hostname + ":" + config.port);
        }
        List<ServerErrorCode> validErrorCodes =
            Arrays.asList(ServerErrorCode.No_Error, ServerErrorCode.Blob_Deleted, ServerErrorCode.Blob_Expired);
        Map<BlobId, ServerErrorCode> blobIdToErrorCode =
            validator.validateBlobsOnReplica(dataNodeId, blobIds, config.getOption, clusterMap, storeKeyFactory);
        for (Map.Entry<BlobId, ServerErrorCode> entry : blobIdToErrorCode.entrySet()) {
          ServerErrorCode errorCode = entry.getValue();
          if (!validErrorCodes.contains(errorCode)) {
            LOGGER.error("[" + entry.getKey() + "] received error code: " + errorCode);
          }
        }
        break;
      default:
        throw new IllegalStateException("Recognized but unsupported operation: " + config.typeOfOperation);
    }
    LOGGER.info("Validation complete");
    validator.close();
    clusterMap.close();
  }

  @Override
  public void close() throws IOException {
    throttler.disable();
    serverAdminTool.close();
  }

  /**
   * Gets the list of {@link BlobId} that need to be operated upon.
   * @param config the {@link BlobValidatorConfig} to use.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @return the list of {@link BlobId} that need to be operated upon.
   * @throws IOException
   */
  private static List<BlobId> getBlobIds(BlobValidatorConfig config, ClusterMap clusterMap) throws IOException {
    String[] ids = config.blobIds;
    if (config.blobIdsFilePath != null) {
      String content = new String(Files.readAllBytes(Paths.get(config.blobIdsFilePath)));
      ids = content.split(",");
    }
    List<BlobId> blobIds = new ArrayList<>();
    for (String id : ids) {
      blobIds.add(new BlobId(id, clusterMap));
    }
    return blobIds;
  }

  /**
   * Logs all mismatches
   * @param mismatchDetailsMap a map that contains a mapping from blob id to mismatch messages
   */
  private static void logMismatches(Map<BlobId, List<String>> mismatchDetailsMap) {
    for (Map.Entry<BlobId, List<String>> entry : mismatchDetailsMap.entrySet()) {
      BlobId blobId = entry.getKey();
      List<String> mismatchDetailsList = entry.getValue();
      for (String mismatchDetails : mismatchDetailsList) {
        LOGGER.error("[" + blobId + "] : " + mismatchDetails);
      }
    }
  }

  /**
   * Constructs a BlobValidator
   *
   * @param clusterMap the {@link ClusterMap} to use.
   * @param replicasToContactPerSec the number of replicas to contact in a second.
   * @param sslFactory the {@link SSLFactory} to use.
   * @param verifiableProperties the {@link VerifiableProperties} to use to construct configs.
   * @throws Exception
   */
  BlobValidator(ClusterMap clusterMap, long replicasToContactPerSec, SSLFactory sslFactory,
      VerifiableProperties verifiableProperties) throws Exception {
    throttler = new Throttler(replicasToContactPerSec, 1000, true, SystemTime.getInstance());
    serverAdminTool = new ServerAdminTool(clusterMap, sslFactory, verifiableProperties);
  }

  /**
   * Validates each {@link BlobId} in {@code blobIds} on all of its replicas.
   * @param blobIds the {@link BlobId}s to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return a mapping from {@link BlobId} to details about any mismatches.
   * @throws InterruptedException
   */
  Map<BlobId, List<String>> validateBlobsOnAllReplicas(List<BlobId> blobIds, GetOption getOption, ClusterMap clusterMap,
      StoreKeyFactory storeKeyFactory) throws InterruptedException {
    Map<BlobId, List<String>> mismatchDetailsMap = new HashMap<>();
    for (BlobId blobId : blobIds) {
      LOGGER.info("Validating blob {} on all replicas", blobId);
      List<String> mismatchDetails = validateBlobOnAllReplicas(blobId, getOption, clusterMap, storeKeyFactory);
      mismatchDetailsMap.put(blobId, mismatchDetails);
    }
    return mismatchDetailsMap;
  }

  /**
   * Validates each {@link BlobId} in {@code blobIds} on all of its replicas in {@code datacenter}.
   * @param datacenter the datacenter in which the blob ids have to be validated.
   * @param blobIds the {@link BlobId}s to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return a mapping from {@link BlobId} to details about any mismatches.
   * @throws InterruptedException
   */
  Map<BlobId, List<String>> validateBlobsOnDatacenter(String datacenter, List<BlobId> blobIds, GetOption getOption,
      ClusterMap clusterMap, StoreKeyFactory storeKeyFactory) throws InterruptedException {
    Map<BlobId, List<String>> mismatchDetailsMap = new HashMap<>();
    for (BlobId blobId : blobIds) {
      LOGGER.info("Validating blob {} in {}", blobId, datacenter);
      List<String> mismatchDetails =
          validateBlobOnDatacenter(datacenter, blobId, getOption, clusterMap, storeKeyFactory);
      mismatchDetailsMap.put(blobId, mismatchDetails);
    }
    return mismatchDetailsMap;
  }

  /**
   * Validates each {@link BlobId} in {@code blobIds} on {@code dataNodeId}.
   * @param dataNodeId the {@link DataNodeId} where the blobs have to be validated.
   * @param blobIds the {@link BlobId}s to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return a mapping from {@link BlobId} to the response error codes from {@code dataNodeId}
   * @throws InterruptedException
   */
  Map<BlobId, ServerErrorCode> validateBlobsOnReplica(DataNodeId dataNodeId, List<BlobId> blobIds, GetOption getOption,
      ClusterMap clusterMap, StoreKeyFactory storeKeyFactory) throws InterruptedException {
    Map<BlobId, ServerErrorCode> blobIdToErrorCode = new HashMap<>();
    for (BlobId blobId : blobIds) {
      LOGGER.info("Validating blob {} in {}", blobId, dataNodeId);
      ServerResponse response = getRecordFromNode(dataNodeId, blobId, getOption, clusterMap, storeKeyFactory);
      blobIdToErrorCode.put(blobId, response.serverErrorCode);
    }
    return blobIdToErrorCode;
  }

  /**
   * Validates {@code blobId} on all of its replicas.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return a list of details if there are mismatches. Zero sized list if there aren't any mismatches.
   * @throws InterruptedException
   */
  private List<String> validateBlobOnAllReplicas(BlobId blobId, GetOption getOption, ClusterMap clusterMap,
      StoreKeyFactory storeKeyFactory) throws InterruptedException {
    Map<DataNodeId, ServerResponse> dataNodeIdBlobContentMap = new HashMap<>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      ServerResponse response =
          getRecordFromNode(replicaId.getDataNodeId(), blobId, getOption, clusterMap, storeKeyFactory);
      dataNodeIdBlobContentMap.put(replicaId.getDataNodeId(), response);
    }
    return getMismatchDetails(blobId.getID(), dataNodeIdBlobContentMap);
  }

  /**
   * Validates {@code blobId} on all of its replicas in {@code datacenter}
   * @param datacenter the datacenter in which {@code blobId} have to be validated.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return a list of details if there are mismatches. Zero sized list if there aren't any mismatches.
   * @throws InterruptedException
   */
  private List<String> validateBlobOnDatacenter(String datacenter, BlobId blobId, GetOption getOption,
      ClusterMap clusterMap, StoreKeyFactory storeKeyFactory) throws InterruptedException {
    Map<DataNodeId, ServerResponse> dataNodeIdBlobContentMap = new HashMap<>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      if (replicaId.getDataNodeId().getDatacenterName().equalsIgnoreCase(datacenter)) {
        ServerResponse response =
            getRecordFromNode(replicaId.getDataNodeId(), blobId, getOption, clusterMap, storeKeyFactory);
        dataNodeIdBlobContentMap.put(replicaId.getDataNodeId(), response);
      }
    }
    return getMismatchDetails(blobId.getID(), dataNodeIdBlobContentMap);
  }

  /**
   * Gets the {@link ServerResponse} from {@code dataNodeId} for {@code blobId}.
   * @param dataNodeId the {@link DataNodeId} to query.
   * @param blobId the {@link BlobId} to operate on.
   * @param getOption the {@link GetOption} to use with the {@link com.github.ambry.protocol.GetRequest}.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use.
   * @return the {@link ServerResponse} from {@code dataNodeId} for {@code blobId}.
   * @throws InterruptedException
   */
  private ServerResponse getRecordFromNode(DataNodeId dataNodeId, BlobId blobId, GetOption getOption,
      ClusterMap clusterMap, StoreKeyFactory storeKeyFactory) throws InterruptedException {
    LOGGER.debug("Getting {} from {}", blobId, dataNodeId);
    ServerResponse serverResponse;
    try {
      Pair<ServerErrorCode, BlobAll> response =
          serverAdminTool.getAll(dataNodeId, blobId, getOption, clusterMap, storeKeyFactory);
      ServerErrorCode errorCode = response.getFirst();
      if (errorCode == ServerErrorCode.No_Error) {
        BlobAll blobAll = response.getSecond();
        ByteBuf buffer = blobAll.getBlobData().content();
        byte[] blobBytes = new byte[buffer.readableBytes()];
        buffer.readBytes(blobBytes);
        buffer.release();
        serverResponse = new ServerResponse(errorCode, blobAll.getStoreKey(), blobAll.getBlobInfo().getBlobProperties(),
            blobAll.getBlobInfo().getUserMetadata(), blobBytes, blobAll.getBlobEncryptionKey());
      } else {
        serverResponse = new ServerResponse(errorCode, null, null, null, null, null);
      }
    } catch (MessageFormatException e) {
      LOGGER.error("Error while deserializing record for {} from {}", blobId, dataNodeId, e);
      serverResponse = new ServerResponse(ServerErrorCode.Data_Corrupt, null, null, null, null, null);
    } catch (Exception e) {
      LOGGER.error("Error while getting record for {} from {}", blobId, dataNodeId, e);
      serverResponse = new ServerResponse(ServerErrorCode.Unknown_Error, null, null, null, null, null);
    } finally {
      throttler.maybeThrottle(1);
    }
    LOGGER.debug("ServerError code is {} for blob {} from {}", serverResponse.serverErrorCode, blobId, dataNodeId);
    return serverResponse;
  }

  /**
   * Compares {@link ServerResponse} for a list of replicas.
   * @param blobId the blobId for which the comparison is made
   * @param dataNodeIdBlobContentMap the map containing the replica to their respective {@link ServerResponse}
   * @return a list of details if there are mismatches. Zero sized list if there aren't any mismatches.
   */
  private List<String> getMismatchDetails(String blobId, Map<DataNodeId, ServerResponse> dataNodeIdBlobContentMap) {
    List<String> mismatchDetailsList = new ArrayList<>();
    Iterator<DataNodeId> dataNodeIdIterator = dataNodeIdBlobContentMap.keySet().iterator();
    DataNodeId dataNodeId1 = dataNodeIdIterator.next();
    ServerResponse dataNode1ServerResponse = dataNodeIdBlobContentMap.get(dataNodeId1);
    while (dataNodeIdIterator.hasNext()) {
      DataNodeId dataNodeId2 = dataNodeIdIterator.next();
      ServerResponse dataNode2ServerResponse = dataNodeIdBlobContentMap.get(dataNodeId2);
      String mismatchDetails = dataNode1ServerResponse.getMismatchDetails(dataNode2ServerResponse);
      if (mismatchDetails != null) {
        mismatchDetails = "Mismatch for [" + blobId + "] between [" + dataNodeId1 + "] and [" + dataNodeId2 + "] - "
            + mismatchDetails;
        mismatchDetailsList.add(mismatchDetails);
      }
    }
    return mismatchDetailsList;
  }

  /**
   * Holds all the data pertaining to a blob obtained from a server like {@link BlobProperties}, UserMetata and the
   * actual blob content
   */
  class ServerResponse {
    final ServerErrorCode serverErrorCode;
    final StoreKey storeKey;
    final BlobProperties blobProperties;
    final byte[] userMetadata;
    final byte[] blobData;
    final ByteBuffer encryptionKey;

    ServerResponse(ServerErrorCode serverErrorCode, StoreKey storeKey, BlobProperties blobProperties,
        byte[] userMetadata, byte[] blobData, ByteBuffer encryptionKeyBuffer) {
      this.serverErrorCode = serverErrorCode;
      this.storeKey = storeKey;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.blobData = blobData;
      this.encryptionKey = encryptionKeyBuffer;
    }

    /**
     * Gets details about any mismatches.
     * @param that the {@link ServerResponse} to compare against.
     * @return details about any mismatches.
     */
    String getMismatchDetails(ServerResponse that) {
      String mismatchDetails = null;
      if (!serverErrorCode.equals(that.serverErrorCode)) {
        mismatchDetails = "ServerErrorCode mismatch: " + serverErrorCode + " v/s " + that.serverErrorCode;
      } else if (serverErrorCode == ServerErrorCode.No_Error) {
        if (!storeKey.equals(that.storeKey)) {
          mismatchDetails = "StoreKey mismatch: " + storeKey + " v/s " + that.storeKey;
        } else {
          mismatchDetails = compareBlobProperties(that);
          if (mismatchDetails == null) {
            if (!Arrays.equals(userMetadata, that.userMetadata)) {
              mismatchDetails = "UserMetadata does not match";
            } else if (!Objects.equals(encryptionKey, that.encryptionKey)) {
              mismatchDetails = "EncryptionKey does not match";
            } else if (!Arrays.equals(blobData, that.blobData)) {
              mismatchDetails = "Blob data does not match";
            }
          }
        }
      }
      return mismatchDetails;
    }

    /**
     * Gets details about any mismatches in {@link BlobProperties}
     * @param that the {@link ServerResponse} to compare against.
     * @return details about any mismatches.
     */
    private String compareBlobProperties(ServerResponse that) {
      String mismatchDetails = null;
      if (blobProperties == null || that.blobProperties == null) {
        mismatchDetails = "Either of BlobProperties is null";
      } else if (blobProperties.getBlobSize() != that.blobProperties.getBlobSize()) {
        mismatchDetails =
            "Size mismatch: " + blobProperties.getBlobSize() + " v/s " + that.blobProperties.getBlobSize();
      } else if (blobProperties.getTimeToLiveInSeconds() != that.blobProperties.getTimeToLiveInSeconds()) {
        mismatchDetails = "TTL mismatch: " + blobProperties.getTimeToLiveInSeconds() + " v/s "
            + that.blobProperties.getTimeToLiveInSeconds();
      } else if (blobProperties.getCreationTimeInMs() != that.blobProperties.getCreationTimeInMs()) {
        mismatchDetails = "Creation time mismatch: " + blobProperties.getCreationTimeInMs() + " v/s "
            + that.blobProperties.getCreationTimeInMs();
      } else if (blobProperties.getContentType() != null && that.blobProperties.getContentType() != null
          && (!blobProperties.getContentType().equals(that.blobProperties.getContentType()))) {
        mismatchDetails = "Content type mismatch: " + blobProperties.getContentType() + " v/s "
            + that.blobProperties.getContentType();
      } else if (blobProperties.getContentType() == null || that.blobProperties.getContentType() == null) {
        mismatchDetails = "Content type mismatch: " + blobProperties.getContentType() + " v/s "
            + that.blobProperties.getContentType();
      } else if (blobProperties.getOwnerId() != null && that.blobProperties.getOwnerId() != null
          && (!blobProperties.getOwnerId().equals(that.blobProperties.getOwnerId()))) {
        mismatchDetails =
            "Owner ID mismatch: " + blobProperties.getOwnerId() + " v/s " + that.blobProperties.getOwnerId();
      } else if (blobProperties.getOwnerId() == null || that.blobProperties.getOwnerId() == null) {
        mismatchDetails =
            "Owner ID mismatch: " + blobProperties.getOwnerId() + " v/s " + that.blobProperties.getOwnerId();
      } else if (blobProperties.getServiceId() != null && that.blobProperties.getServiceId() != null && (!blobProperties
          .getServiceId()
          .equals(that.blobProperties.getServiceId()))) {
        mismatchDetails =
            "Service ID mismatch: " + blobProperties.getServiceId() + " v/s " + that.blobProperties.getServiceId();
      } else if (blobProperties.getServiceId() == null || that.blobProperties.getServiceId() == null) {
        mismatchDetails =
            "Service ID mismatch: " + blobProperties.getServiceId() + " v/s " + that.blobProperties.getServiceId();
      } else if (blobProperties.getAccountId() != that.blobProperties.getAccountId()) {
        mismatchDetails =
            "AccountId mismatch: " + blobProperties.getAccountId() + " v/s " + that.blobProperties.getAccountId();
      } else if (blobProperties.getContainerId() != that.blobProperties.getContainerId()) {
        mismatchDetails =
            "ContainerId mismatch: " + blobProperties.getContainerId() + " v/s " + that.blobProperties.getContainerId();
      }
      return mismatchDetails;
    }

    public String toString() {
      return "[StoreKey=" + storeKey + ",ServerErrorCode=" + serverErrorCode + ",BlobProperties=" + blobProperties
          + ",UserMetadata size=" + ((userMetadata != null) ? userMetadata.length : 0) + "," + "Blob data size=" + (
          (blobData != null) ? blobData.length : 0) + "]";
    }
  }
}
