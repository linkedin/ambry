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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.frontend.Operations;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Serialize and deserialize json objects that represent a collection of accounts. Can be used to support REST APIs for
 * account management.
 */
public class AccountCollectionSerde {
  private static final String ACCOUNTS_KEY = Operations.ACCOUNTS;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Use a mix in class to remove containers from serialized bytes
  @JsonIgnoreProperties({"containers"})
  abstract class AccountMixIn {
  }

  private static final ObjectMapper objectMapperWithoutContainer =
      new ObjectMapper().addMixIn(Account.class, AccountMixIn.class);

  /**
   * Serialize a collection of accounts to json bytes that can be used in requests/responses.
   * @param accounts the {@link Account}s to serialize.
   * @return the serialized bytes in json format.
   */
  public static byte[] serializeAccountsInJson(Collection<Account> accounts) throws IOException {
    Map<String, Collection<Account>> resultObj = new HashMap<>();
    resultObj.put(ACCOUNTS_KEY, accounts);
    return objectMapper.writeValueAsBytes(resultObj);
  }

  /**
   * Serialize an account to bytes in json, stripping out its containers.
   * @param account the {@link Account}s to serialize.
   * @return the serialized bytes in json format.
   */
  public static byte[] serializeAccountsInJsonNoContainers(Account account) throws IOException {
    return objectMapperWithoutContainer.writeValueAsBytes(account);
  }

  /**
   * Deserialize a collection of {@link Account} in json from given InputStream.
   * @param inputStream the {@link InputStream} that contains serialized json bytes.
   * @return a {@link Collection} of {@link Account}s.
   */
  public static Collection<Account> accountsFromInputStreamInJson(InputStream inputStream) throws IOException {
    Map<String, Collection<Account>> map =
        objectMapper.readValue(inputStream, new TypeReference<Map<String, Collection<Account>>>() {
        });
    return map.getOrDefault(ACCOUNTS_KEY, Collections.emptyList());
  }

  /**
   * Serialize a collection of containers to json bytes that can be used in requests/responses.
   * @param containers the {@link Container}s to serialize.
   * @return the serialized bytes in json format.
   */
  public static byte[] serializeContainersInJson(Collection<Container> containers) throws IOException {
    Map<String, Collection<Container>> resultObj = new HashMap<>();
    resultObj.put(Account.CONTAINERS_KEY, containers);
    return objectMapper.writeValueAsBytes(resultObj);
  }

  /**
   * Deserialize a collection of {@link Container}s in json from given InputStream.
   * @param inputStream the {@link InputStream} that contains serialized json bytes.
   * @param accountId the account id for these containers.
   * @return a {@link Collection} of {@link Container}s.
   */
  public static Collection<Container> containersFromInputStreamInJson(InputStream inputStream, short accountId)
      throws IOException {
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

  /**
   * Serialize the {@link Dataset} to json bytes that can be used in requests/responses.
   * @param dataset the {@link Dataset} to serialize.
   * @return the serialized bytes in json format.
   * @throws IOException
   */
  public static byte[] serializeDatasetsInJson(Dataset dataset) throws IOException {
    return objectMapper.writeValueAsBytes(dataset);
  }

  /**
   * Deserialize the {@link Dataset} in json from given InputStream.
   * @param inputStream the {@link InputStream} that contains serialized json bytes.
   * @return the {@link Dataset}
   * @throws IOException
   */
  public static Dataset datasetsFromInputStreamInJson(InputStream inputStream) throws IOException {
    Dataset dataset = objectMapper.readValue(inputStream, Dataset.class);
    return new DatasetBuilder(dataset).build();
  }
}
