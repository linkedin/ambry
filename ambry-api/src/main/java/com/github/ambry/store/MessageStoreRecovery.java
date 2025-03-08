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
package com.github.ambry.store;

import java.util.Collections;
import java.util.List;


/**
 * The recovery component used by the store to recover the index from the log.
 */
public interface MessageStoreRecovery {
  /**
   * Recovers the messages from the underlying store using the read interface. It recovers from the startOffset
   * till the endOffset. The expectation of this interface is that read never blocks and all the messages between
   * {@code startOffset} and {@code endOffset} should be valid messages. If any message is invalid, the recovery
   * implementation should stop right away and return the corresponding {@link RecoveryResult}.
   * @param read The read interface that represents the underlying store
   * @param startOffset The start offset from where the recovery needs to begin
   * @param endOffset The end offset where the recovery needs to complete. The recovery has to stop before this offset if
   *                  there were malformed messages
   * @param factory The store key factory used to create the store key
   * @return an object of {@link RecoveryResult}.
   */
  RecoveryResult recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory);

  /**
   * A data class to represent the result of a {@link MessageStoreRecovery}. All the fields in this class is public and
   * immutable.
   */
  class RecoveryResult {
    /**
     * The valid messages recovered from the {@link MessageStoreRecovery}. The messages in the list should be ordered as
     * the same order from the underlying storage.
     */
    public final List<MessageInfo> recovered;
    /**
     * Any exception encountered when recovering the messages. Once encountering exception, the recovery process should stop
     * and return with proper start offset. Exception includes, but not limited to
     * 1. IOException
     * 2. Invalid messages, including format error, CRC error etc.
     * If there is no exception while recovering messages, this field should be Null and the {@link #currentStartOffset} should
     * equals to the given {@code endOffset}.
     */
    public final StoreException recoveryException;
    /**
     * The start offset of current message. This is only useful when there is an exception while recovering messages. This field
     * should be the start offset of the invalid message.
     */
    public final long currentStartOffset;

    public RecoveryResult(List<MessageInfo> recovered, StoreException recoveryException, long currentStartOffset) {
      this.recovered = Collections.unmodifiableList(recovered);
      this.recoveryException = recoveryException;
      this.currentStartOffset = currentStartOffset;
    }
  }
}
