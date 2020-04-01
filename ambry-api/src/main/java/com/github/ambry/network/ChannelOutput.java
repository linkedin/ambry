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
package com.github.ambry.network;

import java.io.InputStream;


/**
 * The receive on the connected channel provides a ChannelOutput that
 * can be used to read the output from.
 */
public class ChannelOutput {

  private InputStream inputStream;

  private long streamSize;

  public ChannelOutput(InputStream inputStream, long streamSize) {
    this.inputStream = inputStream;
    this.streamSize = streamSize;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public long getStreamSize() {
    return streamSize;
  }
}
