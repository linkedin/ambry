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
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;


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
   *         write interface or the {@link StoreException} if an error occurred
   */
  public CompletableFuture<Long> writeAsyncTo(CloudWriteChannel writeChannel) {
    return Utils.completedExceptionally(new UnsupportedOperationException("Async write operation not supported"));
  }
}
