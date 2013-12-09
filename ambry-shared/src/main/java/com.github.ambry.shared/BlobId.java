package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import javax.xml.bind.DatatypeConverter;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * The Id used to represent a blob uniquely
 */
public class BlobId extends StoreKey {

  private PartitionId partitionId;
  private String uuid;
  private static int UUId_Size_In_Bytes = 4;
  private Short version = 1;
  private static short Version_Size_In_Bytes = 2;

  public BlobId(PartitionId partitionId) {
    this.partitionId = partitionId;
    this.uuid = UUID.randomUUID().toString();
  }

  public BlobId(String id, ClusterMap map)  throws IOException {
    //this(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(id)))), map);
    this(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(DatatypeConverter.parseHexBinary(id)))), map);
  }

  public BlobId(DataInputStream stream, ClusterMap map) throws IOException {
    this.version = stream.readShort();
    this.partitionId = map.getPartitionIdFromStream(stream);
    uuid = Utils.readIntString(stream);
  }

  public short sizeInBytes() {
    return (short)(Version_Size_In_Bytes + partitionId.getBytes().length + UUId_Size_In_Bytes + uuid.length());
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort(version);
    idBuf.put(partitionId.getBytes());
    idBuf.putInt(uuid.getBytes().length);
    idBuf.put(uuid.getBytes());
    return idBuf.array();
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{version, partitionId, uuid});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BlobId other = (BlobId) obj;
    if (version != other.version)
      return false;
    if  (partitionId == null) {
      if (other.partitionId != null)
        return false;
    }
    if (uuid == null) {
      if (other.uuid != null)
        return false;
    }
    else if (!partitionId.equals(other.partitionId))
      return false;
    else if (!uuid.equals(other.uuid))
      return false;
    return true;
  }

  @Override
  public String toString() {
    //return DatatypeConverter.printBase64Binary(toBytes());
    return DatatypeConverter.printHexBinary(toBytes());
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null)
      throw new NullPointerException("input argument null");
    BlobId other = (BlobId) o;

    int result = version.compareTo(other.version);
    if (result == 0) {
      result = partitionId.compareTo(other.partitionId);
      if (result == 0)
      {
        result = uuid.compareTo(other.uuid);
      }
    }
    return result;
  }
}