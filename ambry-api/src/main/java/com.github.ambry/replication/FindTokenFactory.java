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
package com.github.ambry.replication;

import java.io.DataInputStream;
import java.io.IOException;


/**
 * A factory interface to create the FindToken
 */
public interface FindTokenFactory {
  /**
   * The find token created using the input stream provided
   * @param stream The stream that is used to create the find token
   * @return The find token created from the stream
   */
  FindToken getFindToken(DataInputStream stream) throws IOException;

  /**
   * Provides a new token to bootstrap the find operation
   * @return A new find token that helps to bootstrap the find operation
   */
  FindToken getNewFindToken();
}
