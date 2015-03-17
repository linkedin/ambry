package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
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

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt = parser.accepts("typeOfOperation",
          "The type of operation to execute - LIST_REPLICAS/GET_BLOB/GET_BLOB_PROPERTIES/GET_USERMETADATA")
          .withRequiredArg().describedAs("The type of file").ofType(String.class).defaultsTo("GET");

      ArgumentAcceptingOptionSpec<String> ambryBlobIdOpt =
          parser.accepts("ambryBlobId", "The blob id to execute get on").withRequiredArg().describedAs("The blob id")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> includeExpiredBlobsOpt =
          parser.accepts("includeExpiredBlob", "Included expired blobs too").withRequiredArg()
              .describedAs("Whether to include expired blobs while querying or not").defaultsTo("false")
              .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(typeOfOperationOpt);
      listOpt.add(ambryBlobIdOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.out.println("AdminTool --hardwareLayout hl --partitionLayout pl --typeOfOperation "
              + "LIST_REPLICAS/GET_BLOB/GET_BLOB_PROPERTIES/GET_USERMETADATA --ambryBlobId blobId");
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));

      String blobIdStr = options.valueOf(ambryBlobIdOpt);
      AdminTool adminTool = new AdminTool();
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
        blobProperties =
            getBlobProperties(blobId, map, replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
                expiredBlobs);
        break;
      } catch (Exception e) {
      }
    }
    return blobProperties;
  }

  public BlobProperties getBlobProperties(BlobId blobId, ClusterMap clusterMap, String replicaHost, int replicaPort,
      boolean expiredBlobs)
      throws MessageFormatException, IOException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    BlockingChannel blockingChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOptions getOptions = (expiredBlobs) ? GetOptions.Include_Expired_Blobs : GetOptions.None;

    try {
      blockingChannel = new BlockingChannel(replicaHost, replicaPort, 20000000, 20000000, 10000, 2000);
      blockingChannel.connect();

      GetRequest getRequest =
          new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobProperties,
              partitionRequestInfos, getOptions);
      System.out.println("Get Request to verify replica blob properties : " + getRequest);
      GetResponse getResponse = null;

      getResponse = BlobValidator.getGetResponseFromStream(blockingChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob properties is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return null;
      }
      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      System.out.println("Get Response from Stream to verify replica blob properties : " + getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println("getBlobProperties error on response " + getResponse.getError() +
            " error code on partition " + serverResponseCode +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
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
                +
                ", Size : " + properties.getBlobSize() + ", CreationTimeInMs : " + properties.getCreationTimeInMs() +
                ", ServiceId : " + properties.getServiceId() + ", TTL : " + properties.getTimeToLiveInSeconds());
        return properties;
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      throw e;
    } finally {
      if (blockingChannel != null) {
        blockingChannel.disconnect();
      }
    }
  }

  public BlobOutput getBlob(BlobId blobId, ClusterMap map, boolean expiredBlobs)
      throws MessageFormatException, IOException {
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    BlobOutput blobOutput = null;
    for (ReplicaId replicaId : replicas) {
      try {
        blobOutput = getBlob(blobId, map, replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
            expiredBlobs);
        break;
      } catch (Exception e) {
      }
    }
    return blobOutput;
  }

  public BlobOutput getBlob(BlobId blobId, ClusterMap clusterMap, String replicaHost, int replicaPort,
      boolean expiredBlobs)
      throws MessageFormatException, IOException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    BlockingChannel blockingChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOptions getOptions = (expiredBlobs) ? GetOptions.Include_Expired_Blobs : GetOptions.None;

    try {
      blockingChannel = new BlockingChannel(replicaHost, replicaPort, 20000000, 20000000, 10000, 2000);
      blockingChannel.connect();

      GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.Blob,
          partitionRequestInfos, getOptions);
      System.out.println("Get Request to get blob : " + getRequest);
      GetResponse getResponse = null;
      getResponse = BlobValidator.getGetResponseFromStream(blockingChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return null;
      }
      System.out.println("Get Response to get blob : " + getResponse.getError());
      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println("blob get error on response " + getResponse.getError() +
            " error code on partition " + serverResponseCode +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
        if (serverResponseCode == ServerErrorCode.Blob_Not_Found) {
          return null;
        } else if (serverResponseCode == ServerErrorCode.Blob_Deleted) {
          return null;
        } else {
          return null;
        }
      } else {
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
        byte[] blobFromAmbry = new byte[(int) blobOutput.getSize()];
        int blobSizeToRead = (int) blobOutput.getSize();
        int blobSizeRead = 0;
        blobOutput.getStream().mark(blobSizeToRead);
        while (blobSizeRead < blobSizeToRead) {
          blobSizeRead += blobOutput.getStream().read(blobFromAmbry, blobSizeRead, blobSizeToRead - blobSizeRead);
        }
        blobOutput.getStream().reset();
        System.out.println("BlobContent deserialized. Size " + blobOutput.getSize());
        return blobOutput;
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      throw e;
    } finally {
      if (blockingChannel != null) {
        blockingChannel.disconnect();
      }
    }
  }

  public ByteBuffer getUserMetadata(BlobId blobId, ClusterMap map, boolean expiredBlobs)
      throws MessageFormatException, IOException {
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    ByteBuffer userMetadata = null;
    for (ReplicaId replicaId : replicas) {
      try {
        userMetadata =
            getUserMetadata(blobId, map, replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
                expiredBlobs);
        break;
      } catch (Exception e) {
      }
    }
    return userMetadata;
  }

  public ByteBuffer getUserMetadata(BlobId blobId, ClusterMap clusterMap, String replicaHost, int replicaPort,
      boolean expiredBlobs)
      throws MessageFormatException, IOException {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    BlockingChannel blockingChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOptions getOptions = (expiredBlobs) ? GetOptions.Include_Expired_Blobs : GetOptions.None;

    try {
      blockingChannel = new BlockingChannel(replicaHost, replicaPort, 20000000, 20000000, 10000, 2000);
      blockingChannel.connect();

      GetRequest getRequest =
          new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobUserMetadata,
              partitionRequestInfos, getOptions);
      System.out.println("Get Request to check blob usermetadata : " + getRequest);
      GetResponse getResponse = null;
      getResponse = BlobValidator.getGetResponseFromStream(blockingChannel, getRequest, clusterMap);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob usermetadata is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return null;
      }
      System.out.println("Get Response to check blob usermetadata : " + getResponse.getError());

      ServerErrorCode serverResponseCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      if (getResponse.getError() != ServerErrorCode.No_Error || serverResponseCode != ServerErrorCode.No_Error) {
        System.out.println("usermetadata get error on response " + getResponse.getError() +
            " error code on partition " + serverResponseCode +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
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
      throw mfe;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      throw e;
    } finally {
      if (blockingChannel != null) {
        blockingChannel.disconnect();
      }
    }
  }
}
