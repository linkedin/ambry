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
package com.github.ambry.rest;

/**
 * Implementation of {@link NioServer} that can be used in tests.
 * <p/>
 * Does no network I/O for now and is useful only for start(), shutdown() tests.
 */
public class MockNioServer implements NioServer {
  // Defines whether this server is faulty (mainly to check for behaviour under NioServer failures).
  private boolean isFaulty;

  public MockNioServer(boolean isFaulty) {
    this.isFaulty = isFaulty;
  }

  @Override
  public void start() throws InstantiationException {
    if (isFaulty) {
      throw new InstantiationException("This is a faulty MockNioServer");
    }
  }

  @Override
  public void shutdown() {
    if (isFaulty) {
      throw new RuntimeException("This is a faulty MockNioServer");
    }
  }

  /**
   * Makes the MockNioServer faulty.
   */
  public void breakdown() {
    isFaulty = true;
  }

  /**
   * Fixes the MockNioServer (not faulty anymore).
   */
  public void fix() {
    isFaulty = false;
  }
}
