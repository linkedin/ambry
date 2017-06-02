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
package com.github.ambry.account;

import static com.github.ambry.account.Container.*;


/**
 * A builder class for {@link Container}. Since {@link Container} is immutable, modifying a {@link Container} needs to
 * build a new {@link Container} object with updated fields through this builder. A {@link Container} can be built
 * in two ways: 1) from an existing {@link Container} object; and 2) by supplying required fields of a {@link Container}.
 * This class is not thread safe.
 */
public class ContainerBuilder {
  private Short id;
  private String name;
  private ContainerStatus status;
  private String description;
  private Boolean isPrivate;
  private Short parentAccountId;

  /**
   * Constructor. This will allow building a new {@link Container} from an existing {@link Container}. The builder will
   * include all the information of the existing {@link Container}.
   * @param origin The {@link Container} to build from.
   */
  public ContainerBuilder(Container origin) {
    if (origin == null) {
      throw new IllegalArgumentException("origin cannot be null.");
    }
    id = origin.getId();
    name = origin.getName();
    status = origin.getStatus();
    description = origin.getDescription();
    isPrivate = origin.isPrivate();
    parentAccountId = origin.getParentAccountId();
  }

  /**
   * Constructor for a {@link ContainerBuilder} taking individual arguments.
   * @param id The id of the {@link Container} to build.
   * @param name The name of the {@link Container}.
   * @param status The status of the {@link Container}.
   * @param description The description of the {@link Container}.
   * @param isPrivate {@code true} if the {@link Container} is public, {@code false} otherwise.
   * @param parentAccountId The id of the parent {@link Account} of the {@link Container} to build.
   */
  public ContainerBuilder(Short id, String name, ContainerStatus status, String description, Boolean isPrivate,
      Short parentAccountId) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.isPrivate = isPrivate;
    this.parentAccountId = parentAccountId;
  }

  /**
   * Sets the id of the {@link Container} to build.
   * @param id The id to set.
   * @return This builder.
   */
  public ContainerBuilder setId(Short id) {
    this.id = id;
    return this;
  }

  /**
   * Sets the name of the {@link Container} to build.
   * @param name The name to set.
   * @return This builder.
   */
  public ContainerBuilder setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets the status of the {@link Container} to build.
   * @param status The status to set.
   * @return This builder.
   */
  public ContainerBuilder setStatus(ContainerStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Sets the description of the {@link Container} to build.
   * @param description The description to set.
   * @return This builder.
   */
  public ContainerBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  /**
   * Sets privacy setting of the {@link Account} to build.
   * @param isPrivate The privacy setting to set.
   * @return This builder.
   */
  public ContainerBuilder setIsPrivate(Boolean isPrivate) {
    this.isPrivate = isPrivate;
    return this;
  }

  /**
   * Sets the id of the parent {@link Account} of the {@link Container} to build.
   * @param parentAccountId The parent {@link Account} id to set.
   * @return This builder.
   */
  public ContainerBuilder setParentAccountId(Short parentAccountId) {
    this.parentAccountId = parentAccountId;
    return this;
  }

  /**
   * Builds a {@link Container} object. {@code id}, {@code name}, {@code status}, {@code isPrivate}, and
   * {@code parentAccountId} are required before build.
   * @return A {@link Container} object.
   * @throws IllegalStateException If any required fields is not set.
   */
  public Container build() {
    return new Container(id, name, status, description, isPrivate, parentAccountId);
  }
}
