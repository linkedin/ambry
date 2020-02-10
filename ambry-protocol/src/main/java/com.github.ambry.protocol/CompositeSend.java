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
package com.github.ambry.protocol;

import com.github.ambry.network.Send;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;


/**
 * Holds multiple Send instances and sends them over the network
 */
public class CompositeSend implements Send {

  private final List<Send> compositeSendList;
  private long totalSizeToWrite;
  private int currentIndexInProgress;

  public CompositeSend(List<Send> compositeSendList) {
    this.compositeSendList = compositeSendList;
    this.currentIndexInProgress = 0;
    for (Send messageFormatSend : compositeSendList) {
      totalSizeToWrite += messageFormatSend.sizeInBytes();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (currentIndexInProgress < compositeSendList.size()) {
      written = compositeSendList.get(currentIndexInProgress).writeTo(channel);
      if (compositeSendList.get(currentIndexInProgress).isSendComplete()) {
        currentIndexInProgress++;
      }
    }
    return written;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    int lastIndex = compositeSendList.size() - 1;
    int i = 0;
    // This callback technically won't be set to the correct value since it will only reflect the size of the last send,
    // not all sends in the batch. This may not currently be a problem but is something to look out for.
    for (Send send : compositeSendList) {
      if (i == lastIndex) {
        // only the last one pass in callback
        send.writeTo(channel, callback);
      } else {
        //TODO: stop writing to the channel whenever there is an exception here and stop the for loop.
        send.writeTo(channel, null);
      }
      i++;
    }
  }

  @Override
  public boolean isSendComplete() {
    return currentIndexInProgress == compositeSendList.size();
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
