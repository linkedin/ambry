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
  public static JSONObject toJson(Collection<Account> accounts) {
    JSONArray accountArray = new JSONArray();
    accounts.stream().map(account -> account.toJson(false)).forEach(accountArray::put);
    return new JSONObject().put(ACCOUNTS_KEY, accountArray);
  }

  /**
   * Deserialize a json object representing a collection of accounts.
   * @param json the {@link JSONObject} to deserialize.
   * @return a {@link Collection} of {@link Account}s.
   */
  public static Collection<Account> fromJson(JSONObject json) {
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
}
