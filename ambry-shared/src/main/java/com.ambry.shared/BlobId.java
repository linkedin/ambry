package com.ambry.shared;

import com.github.ambry.IndexKey;
import com.github.ambry.Utils;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/26/13
 * Time: 3:43 PM
 * To change this template use File | Settings | File Templates.
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