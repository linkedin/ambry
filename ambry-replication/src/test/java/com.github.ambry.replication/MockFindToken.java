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
package com.github.ambry.replication;

import com.github.ambry.utils.PeekableInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class MockFindToken implements FindToken {
  int index;
  long bytesRead;
  FindTokenType type;
  short version;

  public MockFindToken(int index, long bytesRead) {
    this.index = index;
    this.bytesRead = bytesRead;
    this.type = FindTokenType.IndexBased;
    this.version = 0;
  }

  public MockFindToken(PeekableInputStream inputStream) throws IOException {
    DataInputStream stream = new DataInputStream(inputStream);
    this.version = stream.readShort();
    this.type = FindTokenType.values()[stream.readShort()];
    this.index = stream.readInt();
    this.bytesRead = stream.readLong();
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES *2 + Integer.BYTES + Long.BYTES);
    byteBuffer.putShort(version);
    byteBuffer.putShort((short) type.ordinal());
    byteBuffer.putInt(index);
    byteBuffer.putLong(bytesRead);
    return byteBuffer.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockFindToken that = (MockFindToken) o;
    return index == that.index && bytesRead == that.bytesRead;
  }

  @Override
  public FindTokenType getType() {
    return type;
  }

  @Override
  public short getVersion() {
    return version;
  }

  public int getIndex() {
    return index;
  }

  public long getBytesRead() {
    return this.bytesRead;
  }

  public static class MockFindTokenFactory implements FindTokenFactory {

    @Override
    public FindToken getFindToken(PeekableInputStream stream) throws IOException {
      return new MockFindToken(stream);
    }

    @Override
    public FindToken getNewFindToken() {
      return new MockFindToken(0, 0);
    }
  }
}
