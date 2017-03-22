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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;


/**
 * The object store's underlying store
 */
public interface Store {

  /**
   * Starts the store
   * @throws StoreException
   */
  void start() throws StoreException;

  /**
   * Returns the store info for the given ids
   * @param ids The list of ids whose messages need to be retrieved
   * @param storeGetOptions A set of additional options that the store needs to use while getting the message
   * @return The store info for the given ids
   * @throws StoreException
   */
  StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException;

  /**
   * Puts a set of messages into the store
   * @param messageSetToWrite The message set to write to the store
   * @throws StoreException
   */
  void put(MessageWriteSet messageSetToWrite) throws StoreException;

  /**
   * Deletes all the messages that are part of the message set
   * @param messageSetToDelete The list of messages that need to be deleted
   * @throws StoreException
   */
  void delete(MessageWriteSet messageSetToDelete) throws StoreException;

  /**
   * Finds all the entries from the store given a find token
   * @param token The token that acts as a bookmark to make subsequent searches
   * @param maxTotalSizeOfEntries The maximum total size of entries that needs to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value.
   * @return The FindInfo instance that contains the entries found and the new token for future searches
   * @throws StoreException
   */
  FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException;

  /**
   * Finds all the keys that are not present in the store from the input keys
   * @param keys The list of keys that need to be checked for existence
   * @return The list of keys that are not present in the store
   * @throws StoreException
   */
  Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException;

  /**
   * Get the corresponding {@link StoreStats} instance for this store.
   * @return a {@link StoreStats} instance which can be used to fetch store related stats
   * @throws StoreException
   */
  StoreStats getStoreStats();

  /**
   * Checks if the key is deleted. Returns true is the key is deleted. Returns false if the
   * key is present, not available, ttl expired.
   * @param key The key that needs to be checked for deletion state
   * @return True, if the key is deleted, false otherwise
   * @throws StoreException
   */
  boolean isKeyDeleted(StoreKey key) throws StoreException;

  /**
   * Returns the size of the store in bytes
   * @return The size of the store in bytes
   */
  long getSizeInBytes();

  /**
   * Shutsdown the store
   */
  void shutdown() throws StoreException;
}
