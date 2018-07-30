package com.github.ambry.cloud;

public interface BlobEventSource { // extend Iterator?

  // TODO: provide backoff/retry options
  void subscribe(BlobEventConsumer consumer);

  void unsubscribe(BlobEventConsumer consumer);

}
