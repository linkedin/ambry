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
