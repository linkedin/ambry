package com.github.ambry.shared;

import com.github.ambry.store.IndexKey;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;

/**
 * The Id used to represent a blob uniquely
 */
public class BlobId implements IndexKey, Comparable<BlobId>{

  public static final int size = 24;

  private String id;

  public BlobId(String id) {
    this.id = id;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(id.getBytes());
  }

  @Override
  public int compareTo(BlobId o) {
    return id.compareTo(o.id);
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
}