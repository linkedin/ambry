/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;


/**
 * Observes frontend blob payload transfers. Netty calls the listener at most once per request, after successful
 * request handling and a successful final response write. This confirms a server-side write, not receipt by the client.
 * An event requires a server-resolved account and container. Transfers assigned to the unknown account do not emit
 * events. Control and metadata requests do not emit events.
 * Implementations must be thread-safe and must not block the I/O thread.
 */
@FunctionalInterface
public interface BlobPayloadTransferListener {
  enum Direction {
    UPLOAD, DOWNLOAD
  }

  /**
   * @param direction the direction of the blob payload transfer.
   * @param account the resolved target account.
   * @param container the resolved target container.
   * @param bytes blob payload bytes, excluding HTTP framing. For a range response, this is only the returned payload.
   */
  void onTransferComplete(Direction direction, Account account, Container container, long bytes);
}
