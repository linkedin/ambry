/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.rest;

import org.json.JSONObject;


/**
 * A representation of a container. A container virtually groups a number of ambry blobs.
 */
public class Container {
  private final short id;
  private final String name;
  private final JSONObject metadata;
  private final Account parentAccount;

  /**
   * Constructor.
   * @param id The id of the container.
   * @param name The name of the container.
   * @param metadata The metadata of the container in JSON.
   * @param parentAccount The parent {@link Account} of the container.
   */
  Container(short id, String name, JSONObject metadata, Account parentAccount) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null");
    }
    if (parentAccount == null) {
      throw new IllegalArgumentException("parentAccount cannot be null");
    }
    this.id = id;
    this.name = name;
    this.metadata = metadata;
    this.parentAccount = parentAccount;
  }

  /**
   * Gets the id of the container.
   * @return The parentAccount id of the parentAccount.
   */
  public short id() {
    return id;
  }

  /**
   * Gets the String name of the container.
   * @return The String name of the container.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the metadata of the container in JSON format.
   * @return The metadata of the container in JSON format.
   */
  public JSONObject metadata() {
    return metadata;
  }

  /**
   * Gets the parent {@link Account} of the container.
   * @return The parent {@link Account} of the container.
   */
  public Account parentAccount() {
    return parentAccount;
  }
}