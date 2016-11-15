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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;


/**
 * Holds multiple Send instances and sends them over the network
 */
public class CompositeSend implements Send {

  private final List<Send> compositSendList;
  private long totalSizeToWrite;
  private int currentIndexInProgress;

  public CompositeSend(List<Send> compositSendList) {
    this.compositSendList = compositSendList;
    this.currentIndexInProgress = 0;
    for (Send messageFormatSend : compositSendList) {
      totalSizeToWrite += messageFormatSend.sizeInBytes();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (currentIndexInProgress < compositSendList.size()) {
      written = compositSendList.get(currentIndexInProgress).writeTo(channel);
      if (compositSendList.get(currentIndexInProgress).isSendComplete()) {
        currentIndexInProgress++;
      }
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return currentIndexInProgress == compositSendList.size();
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
