/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

public abstract class FindTokenImpl implements FindToken {

  // refers to the version of the StoreFindToken
  protected final short version;

  // refers to the type of the token
  protected final FindTokenType type;

  public FindTokenImpl(short version, FindTokenType type) {
    this.version = version;
    this.type = type;
  }

  /**
   * Returns the type of {@code FindToken}
   * @return the type of the token
   */
  public FindTokenType getType() {
    return type;
  }

  /**
   * Returns the version of the {@link FindToken}
   * @return the version of the {}@link {@link FindToken}
   */
  public short getVersion() {
    return version;
  }
}
