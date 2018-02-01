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
 * Represents the index key. To make an object part of an index key,
 * this interface can be implemented
 */
public abstract class StoreKey implements Comparable<StoreKey> {

  /**
   * The byte version of this key
   * @return A byte buffer that represents the key
   */
  public abstract byte[] toBytes();

  /**
   * The size of the serialized version of the key
   * @return The size of the key
   */
  public abstract short sizeInBytes();

  /**
   * Get the key in String form
   * @return the key in String form
   */
  public abstract String getID();

  /**
   * Get accountId of StoreKey, return -1 if unknown.
   * @return accountId
   */
  public abstract short getAccountId();

  /**
   * Get containerId of StoreKey, return -1 if unknown.
   * @return containerId
   */
  public abstract short getContainerId();

  /**
   * Get a long form of the key for printing.
   * @return the long form of the key
   */
  public abstract String getLongForm();
}
