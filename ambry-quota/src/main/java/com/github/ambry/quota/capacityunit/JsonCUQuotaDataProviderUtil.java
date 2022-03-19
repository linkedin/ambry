/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.capacityunit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Data provider for CU quota defined in {@link QuotaConfig}.
 */
public class JsonCUQuotaDataProviderUtil {

  /**
   * Util to parse the provided json string into resource quotas and create a {@link Map} of QuotaResource id and
   * {@link CapacityUnit} from json configs.
   * @param resourceCUQuotaInJson Json string containing resource quota values.
   * @param accountService The {@link AccountService} to use.
   * @throws IOException in case of any exception.
   */
  public static Map<String, CapacityUnit> getCUQuotasFromJson(String resourceCUQuotaInJson,
      AccountService accountService) throws IOException {
    Map<String, CapacityUnit> quota = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    if (resourceCUQuotaInJson != null && !resourceCUQuotaInJson.trim().isEmpty()) {
      Map<String, MapOrQuota> tempQuotas =
          objectMapper.readValue(resourceCUQuotaInJson, new TypeReference<Map<String, MapOrQuota>>() {
          });
      for (Map.Entry<String, MapOrQuota> entry : tempQuotas.entrySet()) {
        final Account account = accountService.getAccountById(Short.parseShort(entry.getKey()));
        if (account == null) {
          throw new IllegalStateException("No account id " + entry.getKey() + " is found in the account service");
        }
        if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT && !entry.getValue().isQuota
            || account.getQuotaResourceType() != QuotaResourceType.ACCOUNT && entry.getValue().isQuota) {
          throw new IllegalStateException(
              "Account " + entry.getKey() + " quota enforcement type is different from account metadata: "
                  + account.getQuotaResourceType());
        }
        if (entry.getValue().isQuota()) {
          quota.put(QuotaResource.fromAccount(account).getResourceId(), entry.getValue().getQuota());
        } else {
          for (Map.Entry<String, CapacityUnit> containerQuotaEntry : entry.getValue().getContainerQuotas().entrySet()) {
            String containerIdStr = containerQuotaEntry.getKey();
            Container container = account.getContainerById(Short.parseShort(containerIdStr));
            if (container == null) {
              throw new IllegalStateException(
                  "No container id " + containerIdStr + " is found in the account service under account "
                      + entry.getKey());
            }
            quota.put(QuotaResource.fromContainer(container).getResourceId(), containerQuotaEntry.getValue());
          }
        }
      }
    }
    return quota;
  }

  /**
   * Util to create frontend {@link CapacityUnit} capacity from the provided json string representation.
   * @param frontendCUCapacityInJson frontend capacity unit in json string.
   * @return CapacityUnit object representing frontend's CU capacity.
   * @throws IOException in case of any exception.
   */
  public static CapacityUnit getFeCUCapacityFromJson(String frontendCUCapacityInJson) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(frontendCUCapacityInJson, CapacityUnit.class);
  }

  /**
   * A helper class to represent a number or a map of string to number. This is used in deserializing json string from
   * configuration for account/container quota.
   */
  @JsonDeserialize(using = MapOrQuotaDeserializer.class)
  public static class MapOrQuota {
    private final Map<String, CapacityUnit> containerQuotas;
    private final CapacityUnit quota;
    private final boolean isQuota;

    /**
     * Constructor for {@link MapOrQuota} class.
     * @param quota {@link CapacityUnit} object.
     */
    public MapOrQuota(CapacityUnit quota) {
      containerQuotas = null;
      this.quota = quota;
      isQuota = true;
    }

    /**
     * Constructor for {@link MapOrQuota} class.
     * @param containerQuotas {@link Map} of container id to {@link CapacityUnit} object.
     */
    public MapOrQuota(Map<String, CapacityUnit> containerQuotas) {
      this.containerQuotas = containerQuotas;
      this.quota = null;
      this.isQuota = false;
    }

    /**
     * @return {@code true} if this object represents quota. {@code false} if this represents a Map.
     */
    public boolean isQuota() {
      return isQuota;
    }

    /**
     * @return CapacityUnit object.
     */
    public CapacityUnit getQuota() {
      return quota;
    }

    /**
     * @return Map of container id to {@link CapacityUnit}.
     */
    public Map<String, CapacityUnit> getContainerQuotas() {
      return containerQuotas;
    }
  }

  /**
   * Custom deserializer for {@link MapOrQuota}.
   */
  static class MapOrQuotaDeserializer extends StdDeserializer<MapOrQuota> {
    public MapOrQuotaDeserializer() {
      this(null);
    }

    public MapOrQuotaDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public MapOrQuota deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      if (CapacityUnit.isQuotaNode(node)) {
        return new MapOrQuota(new CapacityUnit(node));
      } else {
        Map<String, CapacityUnit> innerMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
          Map.Entry<String, JsonNode> entry = iterator.next();
          innerMap.put(entry.getKey(), new CapacityUnit(entry.getValue()));
        }
        return new MapOrQuota(innerMap);
      }
    }
  }
}
