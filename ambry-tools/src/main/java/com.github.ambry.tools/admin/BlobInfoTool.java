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
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
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
 * Tool to perform blob operations direclty with server
 */
public class BlobInfoTool {

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
          "The type of operation to execute - GET_BLOB_FROM_REPLICA/"
              + "/GET_BLOB_FROM_LOCAL_REPLICA/GET_BLOB_FROM_ALL_REPLICAS/LIST_REPLICAS").withRequiredArg()
          .describedAs("The type of file").ofType(String.class).defaultsTo("GET");

      ArgumentAcceptingOptionSpec<String> ambryBlobIdOpt =
          parser.accepts("ambryBlobId", "The blob id to execute get on").withRequiredArg().describedAs("The blob id")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> replicaHostOpt =
          parser.accepts("replicaHost", "The replica host to execute get on").withRequiredArg()
              .describedAs("The host name").defaultsTo("localhost").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> replicaPortOpt =
          parser.accepts("replicaPort", "The replica port to execute get on").withRequiredArg()
              .describedAs("The host name").defaultsTo("15088").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> fabricOpt =
          parser.accepts("fabric", "Fabric for which the replicas should be chosen from").withRequiredArg()
              .describedAs("The file name with absolute path").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> expiredBlobsOpt =
          parser.accepts("includeExpiredBlob", "Included expired blobs too").withRequiredArg()
              .describedAs("Whether to include expired blobs while querying or not").defaultsTo("false")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> verboseOpt =
          parser.accepts("verbose", "Verbosity").withRequiredArg().describedAs("Verbosity").defaultsTo("false")
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
          System.out.println("BlobInfoTool --hardwareLayout hl --partitionLayout pl --typeOfOperation " +
              "/GET_BLOB_FROM_REPLICA/GET_BLOB_FROM_LOCAL_REPLICA/GET_BLOB_FROM_ALL_REPLICA/" +
              "LIST_REPLICAS -- ambryBlobId blobId --fabric fabric --replicaHost replicaHost " +
              "--replicaPort replicaPort --includeExpiredBlob true/false");
          System.exit(1);
        }
      }

      boolean verbose = Boolean.parseBoolean(options.valueOf(verboseOpt));
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      if (verbose) {
        System.out.println("Hardware layout and partition layout parsed");
      }
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));

      String blobIdStr = options.valueOf(ambryBlobIdOpt);
      if (verbose) {
        System.out.println("Blob Id " + blobIdStr);
      }
      String fabric = options.valueOf(fabricOpt);
      if (verbose) {
        System.out.println("Fabric " + fabric);
      }
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      if (verbose) {
        System.out.println("Type of Operation " + typeOfOperation);
      }
      String replicaHost = options.valueOf(replicaHostOpt);
      if (verbose) {
        System.out.println("ReplciaHost " + replicaHost);
      }
      ;
      boolean expiredBlobs = Boolean.parseBoolean(options.valueOf(expiredBlobsOpt));
      if (verbose) {
        System.out.println("Exp blobs " + expiredBlobs);
      }
      int replicaPort = Integer.parseInt(options.valueOf(replicaPortOpt));
      if (verbose) {
        System.out.println("ReplicPort " + replicaPort);
      }

      BlobInfoTool obj = new BlobInfoTool();
      if (verbose) {
        System.out.println("Blob Id " + blobIdStr);
      }
      BlobId blobId = new BlobId(blobIdStr, map);
      if (typeOfOperation.equalsIgnoreCase("GET_BLOB_FROM_REPLICA")) {
        obj.validate(new String[]{replicaHost});
        if (obj.getBlob(blobId, map, replicaHost, replicaPort, expiredBlobs)) {
          System.out.println("Successfully read the blob");
        } else {
          System.out.println("Failed to read the blob");
        }
      } else if (typeOfOperation.equalsIgnoreCase("GET_BLOB_FROM_LOCAL_REPLICA")) {
        obj.validate(new String[]{replicaHost, fabric});
        obj.getBlob(blobId, map, fabric, expiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("GET_BLOB_FROM_ALL_REPLICAS")) {
        obj.getBlob(blobId, map, expiredBlobs);
      } else if (typeOfOperation.equalsIgnoreCase("LIST_REPLICAS")) {
        obj.printReplicas(blobId);
      } else {
        System.out.println("Invalid Type of Operation ");
        System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  }

  public void validate(String[] values) {
    for (String value : values) {
      if (value == null) {
        System.out.println("Value " + value + " has to be set");
        System.exit(0);
      }
    }
  }

  public void printReplicas(BlobId blobId) {
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      System.out.println(replicaId);
    }
  }

  private void getBlob(BlobId blobId, ClusterMap clusterMap, boolean expiredBlobs) {
    List<ReplicaId> failedReplicas = new ArrayList<ReplicaId>();
    List<ReplicaId> passedReplicas = new ArrayList<ReplicaId>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      if (getBlob(blobId, clusterMap, replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
          expiredBlobs)) {
        passedReplicas.add(replicaId);
        System.out.println("Successfully read the blob from " + replicaId);
      } else {
        failedReplicas.add(replicaId);
        System.out.println("Failed to read the blob from " + replicaId);
      }
    }
    System.out.println("\nSummary ");
    System.out.println("Passed Replicas : " + passedReplicas);
    System.out.println("Failed Replicas : " + failedReplicas);
  }

  private void getBlob(BlobId blobId, ClusterMap clusterMap, String fabric, boolean expiredBlobs) {
    List<ReplicaId> failedReplicas = new ArrayList<ReplicaId>();
    List<ReplicaId> passedReplicas = new ArrayList<ReplicaId>();
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      if (replicaId.getDataNodeId().getDatacenterName().equalsIgnoreCase(fabric)) {
        if (getBlob(blobId, clusterMap, replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
            expiredBlobs)) {
          passedReplicas.add(replicaId);
          System.out.println("Successfully read the blob from " + replicaId);
        } else {
          failedReplicas.add(replicaId);
          System.out.println("Failed to read the blob from " + replicaId);
        }
      }
    }
    System.out.println("\nSummary ");
    System.out.println("Passed Replicas : " + passedReplicas);
    System.out.println("Failed Replicas : " + failedReplicas);
  }

  private boolean getBlob(BlobId blobId, ClusterMap clusterMap, String replicaHost, int replicaPort,
      boolean expiredBlobs) {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
    blobIds.add(blobId);
    BlockingChannel blockingChannel = null;
    AtomicInteger correlationId = new AtomicInteger(1);

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    ArrayList<PartitionRequestInfo> partitionRequestInfos = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfos.add(partitionRequestInfo);

    GetOptions getOptions = (expiredBlobs) ? GetOptions.Include_Expired_Blobs : GetOptions.None;

    try {
      boolean isSuccess = true;
      String failure = "";
      blockingChannel = new BlockingChannel(replicaHost, replicaPort, 20000000, 20000000, 10000, 2000);
      blockingChannel.connect();

      GetRequest getRequest =
          new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobProperties,
              partitionRequestInfos, getOptions);
      System.out.println("Get Request to verify replica blob properties : " + getRequest);
      GetResponse getResponse = null;

      getResponse =
          getGetResponseFromStream(blockingChannel, getRequest, clusterMap, MessageFormatFlags.BlobProperties);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob properties is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return false;
      }

      System.out.println("Get Response from Stream to verify replica blob properties : " + getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error
          || getResponse.getPartitionResponseInfoList().get(0).getErrorCode() != ServerErrorCode.No_Error) {
        System.out.println("getBlobProperties error on response " + getResponse.getError() +
            " error code on partition " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode() +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
        if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          System.out.println("Blob not found ");
          System.out.println(blobId + " STATE SUCCESS");
          return false;
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted) {
          System.out.println("Blob Deleted ");
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Expired) {
          System.out.println("Blob Expired ");
        } else {
          isSuccess = false;
          failure = "FAILED on Blob Props " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        }
      } else {
        BlobProperties properties = MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());

        System.out.println(
            "Blob Properties : Content Type : " + properties.getContentType() + ", OwnerId : " + properties.getOwnerId()
                +
                ", Size : " + properties.getBlobSize() + ", CreationTimeInMs : " + properties.getCreationTimeInMs() +
                ", ServiceId : " + properties.getServiceId() + ", TTL : " + properties.getTimeToLiveInSeconds());
      }

      getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.BlobUserMetadata,
          partitionRequestInfos, getOptions);
      System.out.println("Get Request to check blob usermetadata : " + getRequest);
      getResponse = null;
      getResponse =
          getGetResponseFromStream(blockingChannel, getRequest, clusterMap, MessageFormatFlags.BlobUserMetadata);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob usermetadata is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return false;
      }
      System.out.println("Get Response to check blob usermetadata : " + getResponse.getError());

      System.out.println("Response from get user metadata " + getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error
          || getResponse.getPartitionResponseInfoList().get(0).getErrorCode() != ServerErrorCode.No_Error) {
        System.out.println("usermetadata get error on response " + getResponse.getError() +
            " error code on partition " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode() +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
        if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          System.out.println("Blob not found ");
          System.out.println(blobId + " STATE SUCCESS");
          return false;
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted) {
          System.out.println("Blob Deleted ");
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Expired) {
          System.out.println("Blob Expired ");
        } else {
          isSuccess = false;
          failure = "FAILED on usermetadata " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        }
      } else {
        ByteBuffer userMetadata = MessageFormatRecord.deserializeUserMetadata(getResponse.getInputStream());
                /*StringMapCodec codec = null;
                Map<String, String> ambryMetadata = codec.decode(new String(userMetadata.array(), "UTF-8"));
                System.out.println("UserMetadata " + ambryMetadata);*/
        System.out.println("Usermetadata deserialized. Size " + userMetadata.capacity());
      }

      getRequest = new GetRequest(correlationId.incrementAndGet(), "readverifier", MessageFormatFlags.Blob,
          partitionRequestInfos, getOptions);
      System.out.println("Get Request to get blob : " + getRequest);
      getResponse = null;
      getResponse = getGetResponseFromStream(blockingChannel, getRequest, clusterMap, MessageFormatFlags.Blob);
      if (getResponse == null) {
        System.out.println(" Get Response from Stream to verify replica blob is null ");
        System.out.println(blobId + " STATE FAILED");
        blockingChannel = null;
        return false;
      }
      System.out.println("Get Response to get blob : " + getResponse.getError());
      if (getResponse.getError() != ServerErrorCode.No_Error
          || getResponse.getPartitionResponseInfoList().get(0).getErrorCode() != ServerErrorCode.No_Error) {
        System.out.println("blob get error on response " + getResponse.getError() +
            " error code on partition " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode() +
            " ambryReplica " + replicaHost + " port " + replicaPort +
            " blobId " + blobId);
        if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          System.out.println("Blob not found ");
          System.out.println(blobId + " STATE SUCCESS");
          return false;
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted) {
          System.out.println("Blob Deleted ");
        } else if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Expired) {
          System.out.println("Blob Expired ");
        } else {
          isSuccess = false;
          failure = "FAILED on Blob " + getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        }
      } else {
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
        byte[] blobFromAmbry = new byte[(int) blobOutput.getSize()];
        int blobSizeToRead = (int) blobOutput.getSize();
        int blobSizeRead = 0;
        while (blobSizeRead < blobSizeToRead) {
          blobSizeRead += blobOutput.getStream().read(blobFromAmbry, blobSizeRead, blobSizeToRead - blobSizeRead);
        }
        System.out.println("BlobContent deserialized. Size " + blobOutput.getSize());
      }
      if (isSuccess) {
        System.out.println(blobId + " STATE SUCCESS ");
        return true;
      } else {
        System.out.println(blobId + " STATE " + failure);
        return false;
      }
    } catch (MessageFormatException mfe) {
      System.out.println("MessageFormat Exception Error " + mfe);
      System.out.println(blobId + " STATE FAILED");
      return false;
    } catch (IOException e) {
      System.out.println("IOException " + e);
      return false;
    } finally {
      if (blockingChannel != null) {
        blockingChannel.disconnect();
      }
    }
  }

  /**
   * Method to send request and receive response to and from a blocking channel. If it fails, blocking channel is destroyed
   *
   * @param blockingChannel
   * @param getRequest
   * @param clusterMap
   * @param messageFormatFlags
   * @return
   */
  private static GetResponse getGetResponseFromStream(ConnectedChannel blockingChannel, GetRequest getRequest,
      ClusterMap clusterMap, MessageFormatFlags messageFormatFlags) {
    GetResponse getResponse = null;
    try {
      long startTimeDbCall = System.currentTimeMillis();
      blockingChannel.send(getRequest);
      ChannelOutput channelOutput = blockingChannel.receive();
      InputStream stream = channelOutput.getInputStream();
      getResponse = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      long dbCallLatency = System.currentTimeMillis() - startTimeDbCall;
    } catch (Exception exception) {
      blockingChannel = null;
      exception.printStackTrace();
      System.out.println("Exception Error" + exception);
      return null;
    }
    return getResponse;
  }
}
