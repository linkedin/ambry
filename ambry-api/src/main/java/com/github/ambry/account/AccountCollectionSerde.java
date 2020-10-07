/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.frontend.Operations;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Serialize and deserialize json objects that represent a collection of accounts. Can be used to support REST APIs for
 * account management.
 */
public class AccountCollectionSerde {
  private static final String ACCOUNTS_KEY = Operations.ACCOUNTS;

  /**
   * Serialize a collection of accounts to a json object that can be used in requests/responses.
   * @param accounts the {@link Account}s to serialize.
   * @return the {@link JSONObject}
   */
  public static JSONObject accountsToJson(Collection<Account> accounts) {
    JSONArray accountArray = new JSONArray();
    accounts.stream().map(account -> account.toJson(false)).forEach(accountArray::put);
    return new JSONObject().put(ACCOUNTS_KEY, accountArray);
  }

  /**
   * Deserialize a json object representing a collection of accounts.
   * @param json the {@link JSONObject} to deserialize.
   * @return a {@link Collection} of {@link Account}s.
   */
  public static Collection<Account> accountsFromJson(JSONObject json) {
    JSONArray accountArray = json.optJSONArray(ACCOUNTS_KEY);
    if (accountArray == null) {
      return Collections.emptyList();
    } else {
      Collection<Account> accounts = new ArrayList<>();
      for (int i = 0; i < accountArray.length(); i++) {
        JSONObject accountJson = accountArray.getJSONObject(i);
        accounts.add(Account.fromJson(accountJson));
      }
      return accounts;
    }
  }

  /**
   * Serialize an account to a json object, stripping out its containers.
   * @param account the {@link Account}s to serialize.
   * @return the {@link JSONObject}
   */
  public static JSONObject accountToJsonNoContainers(Account account) {
    JSONObject jsonObject = account.toJson(false);
    jsonObject.remove(Account.CONTAINERS_KEY);
    return jsonObject;
  }

  /**
   * Serialize a collection of containers to a json object that can be used in requests/responses.
   * @param containers the {@link Container}s to serialize.
   * @return the {@link JSONObject}
   */
  public static JSONObject containersToJson(Collection<Container> containers) {
    JSONArray containerArray = new JSONArray();
    containers.stream().map(Container::toJson).forEach(containerArray::put);
    return new JSONObject().put(Account.CONTAINERS_KEY, containerArray);
  }

  /**
   * Deserialize a json object representing a collection of containers.
   * @param json the {@link JSONObject} to deserialize.
   * @param accountId  the parent account id of the containers.
   * @return a {@link Collection} of {@link Container}s.
   */
  public static Collection<Container> containersFromJson(JSONObject json, short accountId) {
    JSONArray containerArray = json.optJSONArray(Account.CONTAINERS_KEY);
    if (containerArray == null) {
      return Collections.emptyList();
    } else {
      Collection<Container> containers = new ArrayList<>();
      for (int i = 0; i < containerArray.length(); i++) {
        JSONObject containerJson = containerArray.getJSONObject(i);
        containers.add(Container.fromJson(containerJson, accountId));
      }
      return containers;
    }
  }
}
