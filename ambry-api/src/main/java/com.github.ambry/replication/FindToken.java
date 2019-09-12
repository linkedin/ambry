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

/**
 * The find token used to search entries in the store
 */
public interface FindToken {
  /**
   * Returns the contents of the token in bytes
   * @return The byte array representing the token
   */
  byte[] toBytes();

  /**
   *  Returns the total bytes read so far until this token
   * @return The total bytes read so far until this token
   */
  public long getBytesRead();

  /**
   * Returns the type of {@code FindToken}
   * @return the type of the token
   */
  public FindTokenType getType();

  /**
   * Returns the version of the {@link FindToken}
   * @return the version of the {@link FindToken}
   */
  public short getVersion();
}
