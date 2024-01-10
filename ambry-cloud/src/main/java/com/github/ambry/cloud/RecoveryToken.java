/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class RecoveryToken implements FindToken {
  private final String token;
  private final long bytesRead;

  private static final int TOKEN_LEN_FIELD_SIZE = 4;
  private static final int BYTES_READ_FIELD_SIZE = 8;

  public RecoveryToken(String azureStorageContinuationToken, long bytesRead) {
    this.token = azureStorageContinuationToken;
    this.bytesRead = bytesRead;
  }

  public String getToken() {
    return token;
  }

  public static RecoveryToken fromBytes(DataInputStream dataInputStream) throws IOException {
    long bytesRead = dataInputStream.readLong();
    int tokenLen = dataInputStream.readInt();
    String token = dataInputStream.readUTF();
    return new RecoveryToken(token, bytesRead);
  }

  @Override
  public byte[] toBytes() {
    // Used by token writer
    int size = BYTES_READ_FIELD_SIZE + TOKEN_LEN_FIELD_SIZE + token.length();
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putLong(bytesRead);
    bufWrap.putInt(token.length());
    bufWrap.put(token.getBytes());
    return buf;
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public FindTokenType getType() {
    return FindTokenType.CloudBased;
  }

  @Override
  public short getVersion() {
    return 0;
  }
}
