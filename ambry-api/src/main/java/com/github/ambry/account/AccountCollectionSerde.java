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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.frontend.Operations;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
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

  public static byte[] serializeAccountsInJson(ObjectMapper objectMapper, Collection<Account> accounts)
      throws IOException {
    Map<String, Collection<Account>> resultObj = new HashMap<>();
    resultObj.put(ACCOUNTS_KEY, accounts);
    return objectMapper.writeValueAsBytes(resultObj);
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

  public static Collection<Account> accountsFromInputStreamInJson(ObjectMapper objectMapper, InputStream inputStream)
      throws IOException {
    Map<String, Collection<Account>> map =
        objectMapper.readValue(inputStream, new TypeReference<Map<String, Collection<Account>>>() {
        });
    return map.getOrDefault(ACCOUNTS_KEY, Collections.emptyList());
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

  public static byte[] serializeContainersInJson(ObjectMapper objectMapper, Collection<Container> containers)
      throws IOException {
    Map<String, Collection<Container>> resultObj = new HashMap<>();
    resultObj.put(Account.CONTAINERS_KEY, containers);
    return objectMapper.writeValueAsBytes(resultObj);
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

  public static Collection<Container> containersFromInputStreamInJson(ObjectMapper objectMapper,
      InputStream inputStream, short accountId) throws IOException {
    Map<String, Collection<Container>> map =
        objectMapper.readValue(inputStream, new TypeReference<Map<String, Collection<Container>>>() {
        });
    if (!map.containsKey(Account.CONTAINERS_KEY)) {
      return Collections.emptyList();
    } else {
      Collection<Container> containers = map.get(Account.CONTAINERS_KEY);
      return containers.stream()
          .map(c -> new ContainerBuilder(c).setParentAccountId(accountId).build())
          .collect(Collectors.toList());
    }
  }
}
