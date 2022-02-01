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
package com.github.ambry.quota.storage;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * A JSON string implementation of {@link QuotaSource} interface. The entire storage quota is encoded as json
 * string in {@link StorageQuotaConfig#storageQuotaInJson}.
 */
public class JSONStringStorageQuotaSource implements QuotaSource, StorageQuotaEnforcer.AccountServiceSupplier {

  private final Map<String, Long> storageQuota;
  private final AccountService accountService;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSource}.
   * @param config The {@link QuotaSource}.
   * @param accountService The {@link AccountService} to use.e
   * @throws IOException
   */
  public JSONStringStorageQuotaSource(StorageQuotaConfig config, AccountService accountService) throws IOException {
    Map<String, Long> quota = new HashMap<>();
    if (config.storageQuotaInJson != null && !config.storageQuotaInJson.trim().isEmpty()) {
      ObjectMapper objectMapper = new ObjectMapper();
      Map<String, MapOrNumber> tempQuotas =
          objectMapper.readValue(config.storageQuotaInJson, new TypeReference<Map<String, MapOrNumber>>() {
          });
      for (Map.Entry<String, MapOrNumber> entry : tempQuotas.entrySet()) {
        Account account = accountService.getAccountById(Short.valueOf(entry.getKey()));
        if (account == null) {
          throw new IllegalStateException("No account id " + entry.getKey() + " is found in the account service");
        }
        if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT && !entry.getValue().isNumber()
            || account.getQuotaResourceType() != QuotaResourceType.ACCOUNT && entry.getValue().isNumber) {
          throw new IllegalStateException(
              "Account " + entry.getKey() + " quota enforcement type is different from account metadata: "
                  + account.getQuotaResourceType());
        }
        if (entry.getValue().isNumber()) {
          quota.put(QuotaResource.fromAccount(account).getResourceId(), entry.getValue().getQuota());
        } else {
          for (Map.Entry<String, Long> containerQuotaEntry : entry.getValue().getContainerQuotas().entrySet()) {
            String containerIdStr = containerQuotaEntry.getKey();
            Container container = account.getContainerById(Short.valueOf(containerIdStr));
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
    this.accountService = accountService;
    this.storageQuota = quota;
  }

  JSONStringStorageQuotaSource(Map<String, Long> storageQuota, AccountService accountService) {
    this.accountService = accountService;
    this.storageQuota = storageQuota;
  }

  @Override
  public void init() {
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    if (quotaName != QuotaName.STORAGE_IN_GB) {
      return null;
    }

    QuotaResourceType resourceType = quotaResource.getQuotaResourceType();
    if (resourceType != QuotaResourceType.ACCOUNT && resourceType != QuotaResourceType.CONTAINER) {
      throw new IllegalArgumentException("Unsupported quota resource type: " + resourceType);
    }
    String resourceId = quotaResource.getResourceId();
    if (storageQuota.containsKey(resourceId)) {
      long quotaValue = storageQuota.get(resourceId);
      return new Quota(quotaName, quotaValue, quotaResource);
    }
    return null;
  }

  @Override
  public void updateNewQuotaResources(Collection<Account> accounts) {
    // no-op
  }

  @Override
  public AccountService getAccountService() {
    return accountService;
  }

  @Override
  public float getUsage(QuotaResource quotaResource, QuotaName quotaName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getSystemResourceUsage(QuotaName quotaName) {
    return -1;
  }

  @Override
  public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) {
    // no op for storage quota.
  }

  @Override
  public void shutdown() {
    // no op
  }

  /**
   * A helper class to represent a number or a map of string to number. This is used in deserializing json string from
   * configuration for account/container quota.
   */
  @JsonDeserialize(using = MapOrNumberDeserializer.class)
  static class MapOrNumber {
    private final Map<String, Long> containerQuotas;
    private final long quota;
    private final boolean isNumber;

    public MapOrNumber(long value) {
      containerQuotas = null;
      quota = value;
      isNumber = true;
    }

    public MapOrNumber(Map<String, Long> containerQuotas) {
      this.containerQuotas = containerQuotas;
      this.quota = 0;
      this.isNumber = false;
    }

    public boolean isNumber() {
      return isNumber;
    }

    public long getQuota() {
      return quota;
    }

    public Map<String, Long> getContainerQuotas() {
      return containerQuotas;
    }
  }

  /**
   * Custom deserializer for {@link MapOrNumber}.
   */
  static class MapOrNumberDeserializer extends StdDeserializer<MapOrNumber> {
    public MapOrNumberDeserializer() {
      this(null);
    }

    public MapOrNumberDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public MapOrNumber deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      if (node instanceof NumericNode) {
        return new MapOrNumber(node.asLong());
      } else {
        Map<String, Long> subMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
          Map.Entry<String, JsonNode> entry = iterator.next();
          subMap.put(entry.getKey(), entry.getValue().asLong());
        }
        return new MapOrNumber(subMap);
      }
    }
  }
}

