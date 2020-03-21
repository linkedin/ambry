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

import java.util.List;


/**
 * The message set that needs to be written to a write interface
 */
public interface MessageWriteSet {

  /**
   * Write the messages in this set to the given write channel
   * @param writeChannel The write interface to write the messages to
   * @return The size in bytes that was written to the write interface
   */
  long writeTo(Write writeChannel) throws StoreException;

  /**
   * Returns info about the messages contained in this write set. The messages
   * need not be ordered in any specific format
   * @return The list of message info about the message set
   */
  List<MessageInfo> getMessageSetInfo();

  /**
   * Set life Version for {@link StoreKey}.
   */
}