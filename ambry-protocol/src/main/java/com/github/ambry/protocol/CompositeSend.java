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
import com.github.ambry.commons.Callback;
import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Holds multiple Send instances and sends them over the network
 */
public class CompositeSend extends AbstractByteBufHolder<CompositeSend> implements Send {

  private final List<Send> compositeSendList;
  private long totalSizeToWrite;
  private int currentIndexInProgress;
  private ByteBuf compositeSendContent;

  public CompositeSend(List<Send> compositeSendList) {
    this.compositeSendList = compositeSendList;
    this.currentIndexInProgress = 0;
    for (Send messageFormatSend : compositeSendList) {
      totalSizeToWrite += messageFormatSend.sizeInBytes();
    }
    if (compositeSendList.isEmpty()) {
      compositeSendContent = Unpooled.EMPTY_BUFFER;
    } else {
      if (compositeSendList.size() == 1) {
        compositeSendContent = compositeSendList.get(0).content();
      } else {
        int numComponents = 0;
        boolean allContentPresent = true;
        List<ByteBuf> byteBufs = new ArrayList<>(compositeSendList.size());
        for (Send send : compositeSendList) {
          ByteBuf content = send.content();
          if (content == null) {
            allContentPresent = false;
            break;
          }
          if (content instanceof CompositeByteBuf) {
            numComponents += ((CompositeByteBuf) content).numComponents();
          } else {
            numComponents++;
          }
          byteBufs.add(content);
        }
        if (allContentPresent) {
          compositeSendContent = byteBufs.get(0).alloc().compositeHeapBuffer(numComponents);
          for (ByteBuf content : byteBufs) {
            if (content instanceof CompositeByteBuf) {
              Iterator<ByteBuf> iterator = ((CompositeByteBuf) content).iterator();
              while (iterator.hasNext()) {
                ((CompositeByteBuf) compositeSendContent).addComponent(true, iterator.next());
              }
            } else {
              ((CompositeByteBuf) compositeSendContent).addComponent(true, content);
            }
          }
        } else {
          compositeSendContent = null;
        }
      }
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

  @Override
  public ByteBuf content() {
    return compositeSendContent;
  }

  @Override
  public CompositeSend replace(ByteBuf content) {
    return null;
  }
}
