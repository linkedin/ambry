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
import java.util.Iterator;
import java.util.List;


/**
 * The component used by the store to hard delete entries in the log.
 */
public interface MessageStoreHardDelete {
  /**
   * Returns an iterator over the HardDeleteInfo of the messages in the readSet.
   * @param readSet The set of messages to be replaced.
   * @param factory the store key factory.
   * @param recoveryInfoList An optional list of recoveryInfo messages.
   * @return iterator over the HardDeleteInfo for the messages in the readSet.
   */
  Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
      List<byte[]> recoveryInfoList) throws IOException;
}
