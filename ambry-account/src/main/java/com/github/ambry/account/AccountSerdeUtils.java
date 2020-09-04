/*
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
package com.github.ambry.account;

import org.json.JSONObject;

// TODO: merge with AccountCollectionSerde
public class AccountSerdeUtils {

  public static Account accountFromJson(String json) {
    return Account.fromJson(new JSONObject(json));
  }

  public static String accountToJson(Account account, boolean excludeContainers) {
    JSONObject jsonObject = account.toJson(false);
    if (excludeContainers) {
      jsonObject.remove(Account.CONTAINERS_KEY);
    }
    return jsonObject.toString();
  }

  public static Container containerFromJson(String json, short accountId) {
    return Container.fromJson(new JSONObject(json), accountId);
  }

  public static String containerToJson(Container container) {
    return container.toJson().toString();
  }
}
