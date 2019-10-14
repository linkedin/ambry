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
package com.github.ambry.store;

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;


public class MockId extends StoreKey {

  private String id;
  private final short accountId;
  private final short containerId;
  private final String uuidStr;
  private static final int Id_Size_In_Bytes = 2;
  private static final short UUID_SIZE_FIELD_LENGTH_IN_BYTES = Integer.BYTES;

  public MockId(String id) {
    this(id, Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
  }

  public MockId(String id, short accountId, short containerId) {
    this(id, accountId, containerId, UUID.randomUUID().toString());
  }

  public MockId(String id, short accountId, short containerId, String uuidStr) {
    this.id = id;
    this.accountId = accountId;
    this.containerId = containerId;
    this.uuidStr = uuidStr;
  }

  public MockId(DataInputStream stream) throws IOException {
    id = Utils.readShortString(stream);
    accountId = stream.readShort();
    containerId = stream.readShort();
    uuidStr = Utils.readIntString(stream);
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort((short) id.length());
    idBuf.put(id.getBytes());
    idBuf.putShort(accountId);
    idBuf.putShort(containerId);
    byte[] uuidBytes = uuidStr.getBytes();
    idBuf.putInt(uuidBytes.length);
    idBuf.put(uuidBytes);
    return idBuf.array();
  }

  @Override
  public byte[] getUuidBytesArray() {
    byte[] uuidBytes = uuidStr.getBytes();
    ByteBuffer uuidBuf = ByteBuffer.allocate(UUID_SIZE_FIELD_LENGTH_IN_BYTES + (short) uuidBytes.length);
    uuidBuf.putInt(uuidBytes.length);
    uuidBuf.put(uuidBytes);
    return uuidBuf.array();
  }

  @Override
  public String getID() {
    return id;
  }

  @Override
  public String getLongForm() {
    return getID();
  }

  @Override
  public short sizeInBytes() {
    return (short) (Id_Size_In_Bytes + id.length() + Short.BYTES + Short.BYTES + UUID_SIZE_FIELD_LENGTH_IN_BYTES
        + (short) uuidStr.getBytes().length);
  }

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  @Override
  public boolean isAccountContainerMatch(short accountId, short containerId) {
    return accountId == this.accountId && containerId == this.containerId;
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null) {
      throw new NullPointerException();
    }
    MockId otherId = (MockId) o;
    int result = id.compareTo(otherId.id);
    if (result == 0) {
      result = uuidStr.compareTo(otherId.uuidStr);
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{id});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MockId other = (MockId) obj;

    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getID();
  }
}
