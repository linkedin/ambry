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
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.tools.util.ToolUtils;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to perform blob operations directly with the server for a blobid
 * Features supported so far are:
 * Get blob (in other words deserialize blob) for a given blobid from all replicas
 * Get blob (in other words deserialize blob) for a given blobid from replicas for a datacenter
 * Get blob (in other words deserialize blob) for a given blobid for a specific replica
 *
 */
public class BlobValidator {
  private final ConnectionPool connectionPool;
  private final Map<String, Exception> invalidBlobs;
  private static final Logger logger = LoggerFactory.getLogger(BlobValidator.class);

  public BlobValidator(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    invalidBlobs = new HashMap<String, Exception>();
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
          "The type of operation to execute - VALIDATE_BLOB_ON_REPLICA/"
              + "/VALIDATE_BLOB_ON_DATACENTER/VALIDATE_BLOB_ON_ALL_REPLICAS")
          .withRequiredArg()
          .describedAs("The type of operation")
          .ofType(String.class)
          .defaultsTo("VALIDATE_BLOB_ON_ALL_REPLICAS");

      ArgumentAcceptingOptionSpec<String> ambryBlobIdListOpt =
          parser.accepts("blobIds", "Comma separated blobIds to execute get on")
              .withRequiredArg()
              .describedAs("Blob Ids")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> blobIdFilePathOpt =
          parser.accepts("blobIdsFilePath", "File path referring to list of blobIds, one blobId per line")
              .withRequiredArg()
              .describedAs("blobIdsFilePath")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> replicaHostOpt =
          parser.accepts("replicaHost", "The replica host to execute get on")
              .withRequiredArg()
              .describedAs("The host name")
              .defaultsTo("localhost")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> replicaPortOpt =
          parser.accepts("replicaPort", "The replica port to execute get on")
              .withRequiredArg()
              .describedAs("The host name")
              .defaultsTo("15088")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> datacenterOpt =
          parser.accepts("datacenter", "Datacenter for which the replicas should be chosen from")
              .withRequiredArg()
              .describedAs("The file name with absolute path")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> expiredBlobsOpt =
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

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          logger.error("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

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
      VerifiableProperties vProps = new VerifiableProperties(sslProperties);
      SSLConfig sslConfig = new SSLConfig(vProps);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(vProps);
      ConnectionPoolConfig connectionPoolConfig =
          new ConnectionPoolConfig(new VerifiableProperties(connectionPoolProperties));
      connectionPool =
          new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, new MetricRegistry());

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      logger.trace("Hardware layout and partition layout parsed");
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath, clusterMapConfig);

      String blobIdListStr = options.valueOf(ambryBlobIdListOpt);
      String blobIdFilePath = options.valueOf(blobIdFilePathOpt);
      String datacenter = options.valueOf(datacenterOpt);
      logger.trace("Datacenter {}", datacenter);
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      logger.trace("Type of Operation {}", typeOfOperation);
      String replicaHost = options.valueOf(replicaHostOpt);
      logger.trace("ReplciaHost {}", replicaHost);

      boolean expiredBlobs = Boolean.parseBoolean(options.valueOf(expiredBlobsOpt));
      logger.trace("Exp blobs {}", expiredBlobs);
      int replicaPort = Integer.parseInt(options.valueOf(replicaPortOpt));
      logger.trace("ReplicPort {}", replicaPort);

      BlobValidator blobValidator = new BlobValidator(connectionPool);

      Set<BlobId> blobIdSet = blobValidator.generateBlobIds(blobIdListStr, blobIdFilePath, map);
      if (typeOfOperation.equalsIgnoreCase("VALIDATE_BLOB_ON_REPLICA")) {
        blobValidator.validate(new String[]{replicaHost});
        blobValidator.validateBlobOnReplica(blobIdSet, map, replicaHost, replicaPort, expiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("VALIDATE_BLOB_ON_DATACENTER")) {
        blobValidator.validate(new String[]{datacenter});
        blobValidator.validateBlobOnDatacenter(blobIdSet, map, datacenter, expiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("VALIDATE_BLOB_ON_ALL_REPLICAS")) {
        blobValidator.validateBlobOnAllReplicas(blobIdSet, map, expiredBlobs);
      } else {
        logger.error("Invalid Type of Operation ");
        System.exit(1);
      }
    } catch (Exception e) {
      logger.error("Closed with exception ", e);
    } finally {
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }

  /**
   * Validates that elements of values are not null
   * @param values
   */
  public void validate(String[] values) {
    for (String value : values) {
      if (value == null) {
        logger.error("Value " + value + " has to be set");
        System.exit(0);
      }
    }
  }

  /**
   * Generate list of blobs to be filtered based on the arguments
   * @param blobIdListStr Comma separated list of blobIds. Could be {@code null}
   * @param blobIdsFilePath File path referring to a file containing one blobId per line. Could be {@code null}
   * @param map the {@link ClusterMap} to use to generate the BlobId
   * @return Set of BlobIds to be filtered.
   * @throws IOException
   */
  private Set<BlobId> generateBlobIds(String blobIdListStr, String blobIdsFilePath, ClusterMap map) throws IOException {
    Set<String> blobIdStrSet = new HashSet<String>();
    if (blobIdListStr != null) {
      String[] blobArray = blobIdListStr.split(",");
      blobIdStrSet.addAll(Arrays.asList(blobArray));
    } else {
      BufferedReader br = new BufferedReader(new FileReader(new File(blobIdsFilePath)));
      String line;
      while ((line = br.readLine()) != null) {
        blobIdStrSet.add(line);
      }
    }
    Set<BlobId> blobIdSet = new HashSet<BlobId>();
    for (String blobIdStr : blobIdStrSet) {
      try {
        BlobId blobId = new BlobId(blobIdStr, map);
        blobIdSet.add(blobId);
      } catch (Exception e) {
        logger.error("Exception thrown for blobId " + blobIdStr, e);
        invalidBlobs.put(blobIdStr, e);
      }
    }
    return blobIdSet;
  }

  private void validateBlobOnAllReplicas(Set<BlobId> blobIdSet, ClusterMap clusterMap, boolean expiredBlobs) {
    Map<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>> resultMap =
        new HashMap<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>>();
    for (BlobId blobId : blobIdSet) {
      logger.info("Validating blob " + blobId + " on all replicas");
      validateBlobOnAllReplicas(blobId, clusterMap, expiredBlobs, resultMap);
    }
    logger.info("Overall Summary");
    for (BlobId blobId : resultMap.keySet()) {
      Map<ServerErrorCode, ArrayList<ReplicaId>> resultSet = resultMap.get(blobId);
      logger.info("\n" + blobId.getID());
      for (ServerErrorCode result : resultSet.keySet()) {
        logger.info(result + " -> " + resultSet.get(result));
      }
    }
    if (invalidBlobs.size() != 0) {
      logger.error("Invalid blobIds " + invalidBlobs);
    }
  }

  private void validateBlobOnAllReplicas(BlobId blobId, ClusterMap clusterMap, boolean expiredBlobs,
      Map<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>> resultMap) {
    Map<ServerErrorCode, ArrayList<ReplicaId>> responseMap = new HashMap<ServerErrorCode, ArrayList<ReplicaId>>();
    Map<ReplicaId, ServerResponse> replicaIdBlobContentMap = new HashMap<>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      ServerErrorCode serverErrorCode = null;
      try {
        serverErrorCode = validateBlobOnReplica(blobId, clusterMap, replicaId, expiredBlobs, replicaIdBlobContentMap);
      } catch (MessageFormatException e) {
        serverErrorCode = ServerErrorCode.Data_Corrupt;
      } catch (IOException e) {
        serverErrorCode = ServerErrorCode.IO_Error;
      } catch (Exception e) {
        serverErrorCode = ServerErrorCode.Unknown_Error;
      }
      if (responseMap.containsKey(serverErrorCode)) {
        responseMap.get(serverErrorCode).add(replicaId);
      } else {
        ArrayList<ReplicaId> replicaList = new ArrayList<ReplicaId>();
        replicaList.add(replicaId);
        responseMap.put(serverErrorCode, replicaList);
      }
    }
    compareListOfBlobContent(blobId.getID(), replicaIdBlobContentMap);
    logger.info("Summary ");
    for (ServerErrorCode serverErrorCode : responseMap.keySet()) {
      logger.info(serverErrorCode + ": " + responseMap.get(serverErrorCode));
    }
    resultMap.put(blobId, responseMap);
  }

  private void validateBlobOnDatacenter(Set<BlobId> blobIdSet, ClusterMap clusterMap, String datacenter,
      boolean expiredBlobs) {
    Map<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>> resultMap =
        new HashMap<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>>();
    for (BlobId blobId : blobIdSet) {
      logger.info("Validating blob " + blobId + " on datacenter " + datacenter);
      validateBlobOnDatacenter(blobId, clusterMap, datacenter, expiredBlobs, resultMap);
    }
    logger.info("Overall Summary");
    for (BlobId blobId : resultMap.keySet()) {
      Map<ServerErrorCode, ArrayList<ReplicaId>> resultSet = resultMap.get(blobId);
      logger.info("\n" + blobId.getID());
      for (ServerErrorCode serverErrorCode : resultSet.keySet()) {
        logger.info(serverErrorCode + " -> " + resultSet.get(serverErrorCode));
      }
    }
  }

  private void validateBlobOnDatacenter(BlobId blobId, ClusterMap clusterMap, String datacenter, boolean expiredBlobs,
      Map<BlobId, Map<ServerErrorCode, ArrayList<ReplicaId>>> resultMap) {
    Map<ServerErrorCode, ArrayList<ReplicaId>> responseMap = new HashMap<ServerErrorCode, ArrayList<ReplicaId>>();
    Map<ReplicaId, ServerResponse> replicaIdBlobContentMap = new HashMap<>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      if (replicaId.getDataNodeId().getDatacenterName().equalsIgnoreCase(datacenter)) {
        ServerErrorCode serverErrorCode = null;
        try {
          serverErrorCode = validateBlobOnReplica(blobId, clusterMap, replicaId, expiredBlobs, replicaIdBlobContentMap);
        } catch (MessageFormatException e) {
          serverErrorCode = ServerErrorCode.Data_Corrupt;
        } catch (IOException e) {
          serverErrorCode = ServerErrorCode.IO_Error;
        } catch (Exception e) {
          serverErrorCode = ServerErrorCode.Unknown_Error;
        }
        if (responseMap.containsKey(serverErrorCode)) {
          responseMap.get(serverErrorCode).add(replicaId);
        } else {
          ArrayList<ReplicaId> replicaList = new ArrayList<ReplicaId>();
          replicaList.add(replicaId);
          responseMap.put(serverErrorCode, replicaList);
        }
      }
    }
    compareListOfBlobContent(blobId.getID(), replicaIdBlobContentMap);
    logger.info("Summary ");
    for (ServerErrorCode serverErrorCode : responseMap.keySet()) {
      logger.info(serverErrorCode + ": " + responseMap.get(serverErrorCode));
    }
    resultMap.put(blobId, responseMap);
  }

  /**
   * Compares {@link ServerResponse} for a list of replicas
   * @param blobId the blobId for which the comparison is made
   * @param replicaIdBlobContentMap the map containing the replica to their respective {@link ServerResponse}
   */
  private void compareListOfBlobContent(String blobId, Map<ReplicaId, ServerResponse> replicaIdBlobContentMap) {
    Iterator<ReplicaId> replicaIdIterator = replicaIdBlobContentMap.keySet().iterator();
    ReplicaId replicaId1 = replicaIdIterator.next();
    ServerResponse replica1ServerResponse = replicaIdBlobContentMap.get(replicaId1);
    while (replicaIdIterator.hasNext()) {
      ReplicaId replicaId2 = replicaIdIterator.next();
      ServerResponse replica2ServerResponse = replicaIdBlobContentMap.get(replicaId2);
      if (!replica1ServerResponse.equals(replica2ServerResponse)) {
        logger.error("ServerResponse mismatch for {} from {} and {}. Response from {} : {}, Response from {} : {}",
            blobId, replicaId1, replicaId2, replicaId1, replica1ServerResponse, replicaId2, replica2ServerResponse);
      }
    }
  }

  private void validateBlobOnReplica(Set<BlobId> blobIdSet, ClusterMap clusterMap, String replicaHost, int replicaPort,
      boolean expiredBlobs) throws Exception {
    Map<BlobId, ServerErrorCode> resultMap = new HashMap<BlobId, ServerErrorCode>();
    // find the replicaId based on given host name and port number
    for (BlobId blobId : blobIdSet) {
      Map<ReplicaId, ServerResponse> replicaIdBlobContentMap = new HashMap<>();
      ReplicaId targetReplicaId = null;
      for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
        if (replicaId.getDataNodeId().getHostname().equals(replicaHost)) {
          Port port = replicaId.getDataNodeId().getPortToConnectTo();
          if (port.getPort() == replicaPort) {
            targetReplicaId = replicaId;
            break;
          }
        }
      }
      if (targetReplicaId == null) {
        throw new Exception("Can not find blob " + blobId.getID() + "in host " + replicaHost);
      }
      logger.info("Validating blob " + blobId + " on replica " + replicaHost + ":" + replicaPort + "\n");
      ServerErrorCode response = null;
      try {
        response = validateBlobOnReplica(blobId, clusterMap, targetReplicaId, expiredBlobs, replicaIdBlobContentMap);
        if (response == ServerErrorCode.No_Error) {
          logger.trace("Successfully read the blob {}", blobId);
        } else {
          logger.error("Failed to read the blob " + blobId + " due to " + response);
        }
        resultMap.put(blobId, response);
      } catch (MessageFormatException e) {
        resultMap.put(blobId, ServerErrorCode.Data_Corrupt);
      } catch (IOException e) {
        resultMap.put(blobId, ServerErrorCode.IO_Error);
      } catch (Exception e) {
        resultMap.put(blobId, ServerErrorCode.Unknown_Error);
      }
    }
    logger.info("Overall Summary");
    for (BlobId blobId : resultMap.keySet()) {
      logger.info(blobId + " :: " + resultMap.get(blobId));
    }
  }

  private ServerErrorCode validateBlobOnReplica(BlobId blobId, ClusterMap clusterMap, ReplicaId replicaId,
      boolean expiredBlobs, Map<ReplicaId, ServerResponse> replicaBlobContentMap)
      throws MessageFormatException, IOException, ConnectionPoolTimeoutException, InterruptedException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    ConnectedChannel connectedChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);
    BlobProperties properties = null;
    byte[] userMetadataInBytes = null;
    byte[] blobContentInBytes = null;

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
      logger.trace("----- Contacting {}:{} ------", replicaId.getDataNodeId().getHostname(), port.toString());
      logger.trace("Get Request to verify replica blob properties : {}", getRequest);
      GetResponse getResponse = null;

      getResponse = getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        logger.error(" Get Response from Stream to verify replica blob properties is null ");
        throw new IOException("Get Response from Stream to verify replica blob properties is null");
      }
      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      logger.trace("Get Response from Stream to verify replica blob properties : {}", getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        logger.trace(
            "getBlobProperties error on response {} error code on partition {} ambryReplica {} port {} blobId {}",
            getResponse.getError(), serverResponseCode, replicaId.getDataNodeId().getHostname(), port.toString(),
            blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return ServerErrorCode.Blob_Not_Found;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        } else if (serverResponseCode == ServerErrorCode.Blob_Expired) {
          if (getOption != GetOption.Include_Expired_Blobs) {
            replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
            return serverResponseCode;
          }
        } else {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        }
      } else {
        properties = MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
        logger.trace(
            "Blob Properties : Content Type : {}, OwnerId : {}, Size : {}, CreationTimeInMs : {}, ServiceId : {}, TTL : {}",
            properties.getContentType(), properties.getOwnerId(), properties.getBlobSize(),
            properties.getCreationTimeInMs(), properties.getServiceId(), properties.getTimeToLiveInSeconds());
      }

      getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobUserMetadata,
          partitionRequestInfos, getOption);
      logger.trace("Get Request to check blob usermetadata : {}", getRequest);
      getResponse = getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        logger.error(" Get Response from Stream to verify replica blob usermetadata is null ");
        throw new IOException("Get Response from Stream to verify replica blob usermetadata is null");
      }
      logger.trace("Get Response to check blob usermetadata : {}", getResponse.getError());

      serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        logger.trace(
            "usermetadata get error on response {} error code on partition " + "{} ambryReplica {} port {} blobId {}",
            getResponse.getError(), serverResponseCode, replicaId.getDataNodeId().getHostname(), port.toString(),
            blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        } else if (serverResponseCode == ServerErrorCode.Blob_Expired) {
          if (getOption != GetOption.Include_Expired_Blobs) {
            replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
            return serverResponseCode;
          }
        } else {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        }
      } else {
        ByteBuffer userMetadata = MessageFormatRecord.deserializeUserMetadata(getResponse.getInputStream());
        userMetadataInBytes = userMetadata.array();
        logger.trace("Usermetadata deserialized. Size {}", userMetadata.capacity());
      }

      getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.Blob,
          partitionRequestInfos, getOption);
      logger.trace("Get Request to get blob : {}", getRequest);
      getResponse = getGetResponseFromStream(connectedChannel, getRequest, clusterMap);
      if (getResponse == null) {
        logger.error(" Get Response from Stream to verify replica blob is null ");
        logger.trace("{} STATE FAILED", blobId);
        throw new IOException("Get Response from Stream to verify replica blob is null");
      }
      logger.trace("Get Response to get blob : {}", getResponse.getError());
      serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        logger.trace("blob get error on response {} error code on partition {}" + " ambryReplica {} port {} blobId {}",
            getResponse.getError(), serverResponseCode, replicaId.getDataNodeId().getHostname(), port.toString(),
            blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        } else if (serverResponseCode == ServerErrorCode.Blob_Expired) {
          if (getOption != GetOption.Include_Expired_Blobs) {
            replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
            return serverResponseCode;
          }
        } else {
          replicaBlobContentMap.put(replicaId, new ServerResponse(serverResponseCode, null, null, null));
          return serverResponseCode;
        }
      } else {
        BlobData blobData = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
        byte[] blobFromAmbry = new byte[(int) blobData.getSize()];
        int blobSizeToRead = (int) blobData.getSize();
        int blobSizeRead = 0;
        while (blobSizeRead < blobSizeToRead) {
          blobSizeRead += blobData.getStream().read(blobFromAmbry, blobSizeRead, blobSizeToRead - blobSizeRead);
        }
        logger.trace("ServerResponse deserialized. Size {}", blobData.getSize());
        blobContentInBytes = blobFromAmbry;
      }
      replicaBlobContentMap.put(replicaId,
          new ServerResponse(ServerErrorCode.No_Error, properties, userMetadataInBytes, blobContentInBytes));
      return ServerErrorCode.No_Error;
    } catch (MessageFormatException mfe) {
      logger.error("MessageFormat Exception Error ", mfe);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw mfe;
    } catch (IOException e) {
      logger.error("IOException ", e);
      connectionPool.destroyConnection(connectedChannel);
      connectedChannel = null;
      throw e;
    } finally {
      if (connectedChannel != null) {
        connectionPool.checkInConnection(connectedChannel);
      }
    }
  }

  /**
   * Holds all the data pertaining to a blob obtained from a server like {@link BlobProperties}, UserMetata and the
   * actual blob content
   */
  class ServerResponse {
    final ServerErrorCode serverErrorCode;
    final BlobProperties blobProperties;
    final byte[] userMetadata;
    final byte[] blobData;

    public ServerResponse(ServerErrorCode serverErrorCode, BlobProperties blobProperties, byte[] userMetadata,
        byte[] blobData) {
      this.serverErrorCode = serverErrorCode;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.blobData = blobData;
    }

    /**
     * Compares this {@link ServerResponse} with the other
     * @param that the {@link ServerResponse} that needs to be compared with this {@link ServerResponse}
     * @return
     */
    public boolean equals(ServerResponse that) {
      if (serverErrorCode.equals(that.serverErrorCode)) {
        if (serverErrorCode.equals(ServerErrorCode.No_Error)) {
          return compareBlobProperties(that) && Arrays.equals(userMetadata, that.userMetadata) && Arrays.equals(
              blobData, that.blobData);
        } else {
          return true;
        }
      }
      return false;
    }

    private boolean compareBlobProperties(ServerResponse that) {
      if (blobProperties == null || that.blobProperties == null) {
        logger.error("Either of BlobProperties is null ");
        return false;
      }
      if (blobProperties.getBlobSize() != that.blobProperties.getBlobSize()) {
        logger.error("Size Mismatch " + blobProperties.getBlobSize() + ", " + that.blobProperties.getBlobSize());
        return false;
      }
      if (blobProperties.getTimeToLiveInSeconds() != that.blobProperties.getTimeToLiveInSeconds()) {
        logger.error("TTL Mismatch " + blobProperties.getTimeToLiveInSeconds() + ", "
            + that.blobProperties.getTimeToLiveInSeconds());
        return false;
      }
      if (blobProperties.getCreationTimeInMs() != that.blobProperties.getCreationTimeInMs()) {
        logger.error("CreationTime Mismatch " + blobProperties.getCreationTimeInMs() + ", "
            + that.blobProperties.getCreationTimeInMs());

        return false;
      }
      if (blobProperties.getContentType() != null && that.blobProperties.getContentType() != null
          && (!blobProperties.getContentType().equals(that.blobProperties.getContentType()))) {
        logger.error(
            "Content type Mismatch " + blobProperties.getContentType() + ", " + that.blobProperties.getContentType());
        return false;
      } else if (blobProperties.getContentType() == null || that.blobProperties.getContentType() == null) {
        logger.error(
            "ContentType Mismatch " + blobProperties.getContentType() + ", " + that.blobProperties.getContentType());
        return false;
      }

      if (blobProperties.getOwnerId() != null && that.blobProperties.getOwnerId() != null
          && (!blobProperties.getOwnerId().equals(that.blobProperties.getOwnerId()))) {
        logger.error("OwnerId Mismatch " + blobProperties.getOwnerId() + ", " + that.blobProperties.getOwnerId());
        return false;
      } else if (blobProperties.getOwnerId() == null || that.blobProperties.getOwnerId() == null) {
        logger.error("OwnerId Mismatch " + blobProperties.getOwnerId() + ", " + that.blobProperties.getOwnerId());
        return false;
      }

      if (blobProperties.getServiceId() != null && that.blobProperties.getServiceId() != null
          && (!blobProperties.getServiceId().equals(that.blobProperties.getServiceId()))) {
        logger.error("ServiceId Mismatch " + blobProperties.getServiceId() + ", " + that.blobProperties.getServiceId());
        return false;
      } else if (blobProperties.getServiceId() == null || that.blobProperties.getServiceId() == null) {
        logger.error("ServiceId Mismatch " + blobProperties.getServiceId() + ", " + that.blobProperties.getServiceId());
        return false;
      }
      return true;
    }

    public String toString() {
      return serverErrorCode + ", " + blobProperties + ", UserMetadata size " + ((userMetadata != null)
          ? userMetadata.length : 0) + ", " + "blobContent size " + ((blobData != null) ? blobData.length : 0);
    }
  }

  /**
   * Method to send request and receive response to and from a blocking channel. If it fails, blocking channel is destroyed
   *
   * @param blockingChannel
   * @param getRequest
   * @param clusterMap
   * @return
   */
  public static GetResponse getGetResponseFromStream(ConnectedChannel blockingChannel, GetRequest getRequest,
      ClusterMap clusterMap) {
    GetResponse getResponse = null;
    try {
      blockingChannel.send(getRequest);
      ChannelOutput channelOutput = blockingChannel.receive();
      InputStream stream = channelOutput.getInputStream();
      getResponse = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    } catch (Exception exception) {
      blockingChannel = null;
      exception.printStackTrace();
      logger.error("Exception Error", exception);
      return null;
    }
    return getResponse;
  }
}
