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

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class MockId extends StoreKey {

  private String id;
  private static final int Id_Size_In_Bytes = 2;

  public MockId(String id) {
    this.id = id;
  }

  public MockId(DataInputStream stream) throws IOException {
    id = Utils.readShortString(stream);
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(Id_Size_In_Bytes + id.length());
    idBuf.putShort((short) id.length());
    idBuf.put(id.getBytes());
    return idBuf.array();
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
    return (short) (Id_Size_In_Bytes + id.length());
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null) {
      throw new NullPointerException();
    }
    MockId otherId = (MockId) o;
    return id.compareTo(otherId.id);
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
