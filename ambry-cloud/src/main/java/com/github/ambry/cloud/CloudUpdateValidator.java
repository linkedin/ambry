/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import java.util.Map;


/**
 * Interface for Ambry validation logic for updates requested from cloud destination.
 */
public interface CloudUpdateValidator {
  /**
   * Validate the sanity of update operation on given {@code updateFields} against existing {@link CloudBlobMetadata} in
   * cloud destination for the blob with key {@code key}. This can be used to hook Ambry store related validation logic
   * during {@link CloudDestination} specific get-check-update flow. Note that this method can also update some fields
   * within the map.
   * @param metadata {@link CloudBlobMetadata} object obtained from cloud destination.
   * @param key {@link StoreKey} of the blob being updated.
   * @param updateFields {@link Map} of fields and new values requested for update.
   * @return true if the operation can proceed, or false if the operation is not needed.
   * @throws StoreException if validation fails.
   */
  boolean validateUpdate(CloudBlobMetadata metadata, StoreKey key, Map<String, Object> updateFields)
      throws StoreException;
}
