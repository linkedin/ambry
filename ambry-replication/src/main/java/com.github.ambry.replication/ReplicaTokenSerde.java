package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to serialize and deserialize replica tokens
 */
public class ReplicaTokenSerde {
  private static final Logger logger = LoggerFactory.getLogger(ReplicaTokenSerde.class);
  private static final short Crc_Size = 8;
  private final ClusterMap clusterMap;
  private final FindTokenFactory tokenfactory;
  private final short version = 0;

  public ReplicaTokenSerde(ClusterMap clusterMap, FindTokenFactory tokenfactory) {
    this.clusterMap = clusterMap;
    this.tokenfactory = tokenfactory;
  }

  public void write(List<ReplicaTokenInfo> tokenInfoList, OutputStream outputStream) throws IOException {
    CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
    DataOutputStream writer = new DataOutputStream(crcOutputStream);
    try {
      // write the current version
      writer.writeShort(version);
      for (ReplicaTokenInfo replicaTokenInfo : tokenInfoList) {
        writer.write(replicaTokenInfo.getPartitionId().getBytes());
        // Write hostname
        writer.writeInt(replicaTokenInfo.getHostname().getBytes().length);
        writer.write(replicaTokenInfo.getHostname().getBytes());
        // Write replica path
        writer.writeInt(replicaTokenInfo.getReplicaPath().getBytes().length);
        writer.write(replicaTokenInfo.getReplicaPath().getBytes());
        // Write port
        writer.writeInt(replicaTokenInfo.getPort());
        writer.writeLong(replicaTokenInfo.getTotalBytesReadFromLocalStore());
        writer.write(replicaTokenInfo.getReplicaToken().toBytes());
      }
      long crcValue = crcOutputStream.getValue();
      writer.writeLong(crcValue);
    } catch (IOException e) {
      logger.error("IO error while serializing replica tokens");
      throw e;
    } finally {
      writer.close();
    }
  }

  public List<ReplicaTokenInfo> read(InputStream inputStream) throws IOException, ReplicationException {
    CrcInputStream crcStream = new CrcInputStream(inputStream);
    DataInputStream stream = new DataInputStream(crcStream);
    List<ReplicaTokenInfo> tokenInfoList = new ArrayList<>();
    try {
      short version = stream.readShort();
      switch (version) {
        case 0:
          while (stream.available() > Crc_Size) {
            // read partition id
            PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
            // read remote node host name
            String hostname = Utils.readIntString(stream);
            // read remote replica path
            String replicaPath = Utils.readIntString(stream);
            // read remote port
            int port = stream.readInt();
            // read total bytes read from local store
            long totalBytesReadFromLocalStore = stream.readLong();
            // read replica token
            FindToken token = tokenfactory.getFindToken(stream);
            tokenInfoList.add(
                new ReplicaTokenInfo(partitionId, hostname, replicaPath, port, totalBytesReadFromLocalStore, token));
          }

          long computedCrc = crcStream.getValue();
          long readCrc = stream.readLong();
          if (computedCrc != readCrc) {
            throw new ReplicationException(
                "Crc mismatch during replica token deserialization, computed " + computedCrc + ", read " + readCrc);
          }
          return tokenInfoList;
        default:
          throw new ReplicationException("Invalid version found during replica token deserialization: " + version);
      }
    } catch (IOException e) {
      throw new ReplicationException("IO error deserializing replica tokens", e);
    } finally {
      stream.close();
    }
  }
}
