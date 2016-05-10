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

/**
 * The list of options that can be used while reading messages
 * from the store
 */
public enum StoreGetOptions {
  /**
   * This option indicates that the store needs to return the message even if it is expired
   * as long as the message has not been physically deleted from the store.
   */
  Store_Include_Expired,
  /**
   * This option indicates that the store needs to return the message even if it has been
   * marked for deletion as long as the message has not been physically deleted from the
   * store.
   */
  Store_Include_Deleted
}
