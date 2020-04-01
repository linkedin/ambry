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

import java.io.DataInputStream;
import java.io.IOException;


/**
 * Factory to create an index key
 */
public interface StoreKeyFactory {

  /**
   * The store key created using the stream provided
   * @param stream The stream used to create the store key
   * @return The store key created from the stream
   */
  StoreKey getStoreKey(DataInputStream stream) throws IOException;

  /**
   * The store key created using the input provided
   * @param input The string used to create the store key
   * @return The store key created from the string
   */
  StoreKey getStoreKey(String input) throws IOException;
}

