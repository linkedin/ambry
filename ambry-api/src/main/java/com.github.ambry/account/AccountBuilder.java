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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;


/**
 * A builder class for {@link Account}. Since {@link Account} is immutable, modifying an {@link Account} needs to
 * build a new {@link Account} object with updated fields through this builder. An {@link Account} can be built
 * in two ways: 1) from an existing {@link Account} object; and 2) by supplying required fields of an {@link Account}.
 */
public class AccountBuilder {
  private Short id;
  private String name;
  private Account.AccountStatus status;
  private short version;
  private Map<Short, Container> idToContainerMetadataMap = new HashMap<>();

  /**
   * Constructor. This will allow building a new {@link Account} from an existing {@link Account}. The builder will
   * include all the information including the {@link Container}s of the existing {@link Account}.
   * @param origin The {@link Account} to build from.
   */
  public AccountBuilder(Account origin) {
    if (origin == null) {
      throw new IllegalArgumentException("origin cannot be null.");
    }
    this.id = origin.getId();
    this.name = origin.getName();
    this.status = origin.getStatus();
    for (Container container : origin.getAllContainers()) {
      idToContainerMetadataMap.put(container.getId(), container);
    }
    this.version = ACCOUNT_METADATA_VERSION_1;
  }

  /**
   * Constructor. The builder will not include any {@link Container} information.
   * @param id The id of the {@link Account} to build.
   * @param name The name of the {@link Account}. Cannot be {@code null}.
   * @param status The status of the {@link Account}. Cannot be {@code null}.
   */
  public AccountBuilder(short id, String name, AccountStatus status) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.version = ACCOUNT_METADATA_VERSION_1;
  }

  /**
   * Sets the id of the {@link Account} to build.
   * @param id The id to set.
   * @return This builder.
   */
  public AccountBuilder setId(short id) {
    this.id = id;
    return this;
  }

  /**
   * Sets the name of the {@link Account} to build.
   * @param name The name to set.
   * @return This builder.
   */
  public AccountBuilder setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets the status of the {@link Account} to build.
   * @param status The id to set.
   * @return This builder.
   */
  public AccountBuilder setStatus(AccountStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Sets a {@link Container} for the {@link Account} to build. If the builder already has a {@link Container} with
   * the same id as new {@link Container} to set, the new {@link Container} will replace the existing {@link Container}.
   * @param container The new {@link Container} to set.
   * @return This builder.
   */
  public AccountBuilder setContainer(Container container) {
    if (container != null) {
      idToContainerMetadataMap.put(container.getId(), container);
    }
    return this;
  }

  /**
   * Removes a {@link Container} in this builder, so that an {@link Account} to build will not have this
   * {@link Container}. It will be a no-op if no {@link Container} with the id exists in this builder.
   * @param container The {@link Container} to remove.
   * @return This builder.
   */
  public AccountBuilder removeContainer(Container container) {
    if (container != null) {
      short id = container.getId();
      Container containerToRemove = idToContainerMetadataMap.get(id);
      if (container.equals(containerToRemove)) {
        idToContainerMetadataMap.remove(id);
      }
    }
    return this;
  }

  /**
   * Sets a list of {@link Container}s for the {@link Account} to build. If any {@link Container} in the list already
   * exists in the builder, it will be replaced by the one to set.
   * @param containers A list of {@link Container}s to set.
   * @return This builder.
   */
  public AccountBuilder setContainers(List<Container> containers) {
    if (containers != null) {
      for (Container container : containers) {
        setContainer(container);
      }
    }
    return this;
  }

  /**
   * Builds an {@link Account} object. {@code id}, {@code name}, and {@code status} are required before building.
   * @return An {@link Account} object.
   * @throws IllegalStateException If any required fields is not set.
   */
  public Account build() throws JSONException {
    checkAccountIdInContainers();
    checkRequiredFields(version);
    switch (version) {
      case ACCOUNT_METADATA_VERSION_1:
        if (id == null) {
          throw new IllegalStateException("Cannot build account, id is not set");
        }
        if (name == null) {
          throw new IllegalStateException("Cannot build account, name is not set");
        }
        if (status == null) {
          throw new IllegalStateException("Cannot build account, status is not set");
        }
        JSONObject jsonMetadata = new JSONObject();
        jsonMetadata.put(ACCOUNT_METADATA_VERSION_KEY, version);
        jsonMetadata.put(ACCOUNT_ID_KEY, id);
        jsonMetadata.put(ACCOUNT_NAME_KEY, name);
        jsonMetadata.put(ACCOUNT_STATUS_KEY, status);
        JSONArray containerArray = new JSONArray();
        for (Container container : idToContainerMetadataMap.values()) {
          containerArray.put(container.toJson());
        }
        jsonMetadata.put(CONTAINERS_KEY, containerArray);
        return new Account(jsonMetadata);

      default:
        throw new IllegalStateException("Unsupported account metadata version=" + version);
    }
  }

  /**
   * Checks if required fields are missing to build.
   */
  private void checkAccountIdInContainers() {
    for (Container container : idToContainerMetadataMap.values()) {
      if (container.getParentAccountId() != id) {
        throw new IllegalStateException(
            "Container does not belong to this Account because containerId=" + container.getParentAccountId()
                + " is not the same as accountId=" + id);
      }
    }
  }

  /**
   * Checks required fields to build a {@link Account}.
   * @param version The version to build.
   */
  private void checkRequiredFields(short version) {
    switch (version) {
      case CONTAINER_METADATA_VERSION_1:
        if (id == null) {
          throw new IllegalStateException("Cannot build account, id is not set");
        }
        if (name == null) {
          throw new IllegalStateException("Cannot build account, name is not set");
        }
        if (status == null) {
          throw new IllegalStateException("Cannot build account, status is not set");
        }
        break;

      default:
        throw new IllegalStateException("Unsupported container metadata version=" + version);
    }
  }
}
