package com.github.ambry.shared;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Id used to represent a blob uniquely
 */
public class BlobId extends StoreKey{

  private String id;
  private static int Id_Size_In_Bytes = 2;

  public BlobId(String id) {
    this.id = id;
  }

  public BlobId(DataInputStream stream) throws IOException {
    this.id = Utils.readShortString(stream);
  }

  public short sizeInBytes() {
    return (short)(id.length());
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(Id_Size_In_Bytes + id.getBytes().length);
    idBuf.putShort(sizeInBytes());
    idBuf.put(id.getBytes());
    return idBuf.array();
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{id});
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
    if (id  == null) {
      if (other.id != null)
        return false;
    }
    else if (!id.equals(other.id))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null)
      throw new NullPointerException("input argument null");
    BlobId other = (BlobId) o;
    return id.compareTo(other.id);
  }
}