/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.cloud.CloudBlobStore.CloudWriteChannel;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.store.MessageInfo;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A message write set that writes to the underlying cloud implementation of write interface
 */
public class CloudMessageFormatWriteSet extends MessageFormatWriteSet {
  public CloudMessageFormatWriteSet(InputStream streamToWrite, List<MessageInfo> streamInfo, boolean materializeStream)
      throws IOException {
    super(streamToWrite, streamInfo, materializeStream);
  }

  /**
   * Write the messages in this set to the given write channel asynchronously
   * @param writeChannel The write interface to write the messages to
   * @return a {@link CompletableFuture} that will eventually contain the size in bytes that was written to the
   *         write interface or the {@link CloudStorageException} if an error occurred
   */
  CompletableFuture<Long> writeAsyncTo(CloudWriteChannel writeChannel) {
    ReadableByteChannel readableByteChannel = Channels.newChannel(streamToWrite);
    AtomicLong sizeWritten = new AtomicLong();
    List<CompletableFuture<Void>> operationFutures = new ArrayList<>();
    for (MessageInfo info : streamInfo) {
      operationFutures.add(writeChannel.appendAsyncFrom(readableByteChannel, info.getSize())
          .thenRun(() -> sizeWritten.addAndGet(info.getSize())));
    }
    return CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0]))
        .thenApply(unused -> sizeWritten.get());
  }
}
