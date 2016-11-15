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

import java.io.IOException;
import java.util.List;


/**
 * The recovery component used by the store to recover
 * the index from the log.
 */
public interface MessageStoreRecovery {
  /**
   * Recovers the messages from the underlying store using the read interface. It recovers from the startOffset
   * till the endOffset. The expectation of this interface is that read never blocks
   * @param read The read interface that represents the underlying store
   * @param startOffset The start offset from where the recovery needs to begin
   * @param endOffset The end offset where the recovery needs to complete. The recovery can end before this offset if
   *                  there were malformed messages
   * @param factory The store key factory used to create the store key
   * @return A list of messages that were successfully recovered
   * @throws IOException
   */
  List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException;
}
