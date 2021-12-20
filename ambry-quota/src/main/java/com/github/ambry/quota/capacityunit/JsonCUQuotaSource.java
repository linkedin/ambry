/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.quota.storage.JSONStringStorageQuotaSource;
import com.github.ambry.rest.RestRequest;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonCUQuotaSource implements QuotaSource {
  private static final Logger logger = LoggerFactory.getLogger(JsonCUQuotaSource.class);

  private static final EnumSet<QuotaName> SUPPORTED_QUOTA_NAMES =
      EnumSet.of(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT);
  private static final EnumSet<QuotaResourceType> SUPPORTED_QUOTA_RESOURCE_TYPES =
      EnumSet.of(QuotaResourceType.ACCOUNT, QuotaResourceType.CONTAINER);
  protected final CUQuota feUsage;
  protected final CUQuota feQuota;
  private final Map<String, CUQuota> cuQuota;
  private final Map<String, CUQuota> cuUsage;
  private final float maxFrontendCuUsageToAllowExceed;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSource}.
   * @param config The {@link QuotaSource}.
   * @param accountService The {@link AccountService} to use.
   * @throws IOException
   */
  public JsonCUQuotaSource(QuotaConfig config, AccountService accountService) throws IOException {
    Map<String, CUQuota> quota = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    feQuota = objectMapper.readValue(config.frontendCUCapacityInJson, CUQuota.class);
    if (config.resourceCUQuotaInJson != null && !config.resourceCUQuotaInJson.trim().isEmpty()) {
      Map<String, MapOrQuota> tempQuotas =
          objectMapper.readValue(config.resourceCUQuotaInJson, new TypeReference<Map<String, MapOrQuota>>() {
          });
      for (Map.Entry<String, MapOrQuota> entry : tempQuotas.entrySet()) {
        Account account = accountService.getAccountById(Short.parseShort(entry.getKey()));
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
          for (Map.Entry<String, CUQuota> containerQuotaEntry : entry.getValue().getContainerQuotas().entrySet()) {
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
    this.cuQuota = quota;
    this.cuUsage = new HashMap<>();
    this.maxFrontendCuUsageToAllowExceed = config.maxFrontendCuUsageToAllowExceed;
    cuQuota.keySet().forEach(key -> cuUsage.put(key, new CUQuota(0, 0)));
    feUsage = new CUQuota(0, 0);
  }

  @Override
  public Quota<Long> getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    checkSupported(quotaName, quotaResource);

    String resourceId = quotaResource.getResourceId();
    if (cuQuota.containsKey(resourceId)) {
      return new Quota<>(quotaName, cuQuota.get(resourceId).getQuotaValue(quotaName), quotaResource);
    }
    return null;
  }

  @Override
  public boolean isQuotaExceedAllowed(QuotaMethod quotaMethod) {
    if(quotaMethod == QuotaMethod.READ) {
      return ((feUsage.getRcu() * 100) / feQuota.getRcu()) < maxFrontendCuUsageToAllowExceed;
    } else {
      return ((feUsage.getWcu() * 100) / feQuota.getWcu()) < maxFrontendCuUsageToAllowExceed;
    }
  }

  public Quota<Long> getUsage(QuotaResource quotaResource, QuotaName quotaName) {
    checkSupported(quotaName, quotaResource);
    String resourceId = quotaResource.getResourceId();
    if (cuUsage.containsKey(resourceId)) {
      return new Quota<>(quotaName, cuUsage.get(resourceId).getQuotaValue(quotaName), quotaResource);
    }
    return null;
  }

  @Override
  public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
    quotaResources.forEach(quotaResource -> {
      cuQuota.put(quotaResource.getResourceId(), new CUQuota(0, 0));
      cuUsage.put(quotaResource.getResourceId(), new CUQuota(0, 0));
    });
  }

  public void charge(RestRequest restRequest, BlobInfo blobInfo, Map<QuotaName, Double> requestCostMap) throws QuotaException {
    String resourceId;
    try {
      resourceId = QuotaUtils.getQuotaResource(restRequest).getResourceId();
    } catch (QuotaException qEx) {
      logger.error(
          "Cannot chargeIfUsageWithinQuota request because could not create resourceId for request for blob {} due to exception {}",
          blobInfo.getBlobProperties().toString(), qEx.toString());
      throw qEx;
    }
    if (!cuQuota.containsKey(resourceId)) {
      return;
    }
    if (QuotaUtils.isReadRequest(restRequest) && requestCostMap.containsKey(QuotaName.READ_CAPACITY_UNIT)) {
      cuUsage.get(resourceId).rcu += requestCostMap.get(QuotaName.READ_CAPACITY_UNIT);
      feUsage.rcu += requestCostMap.get(QuotaName.READ_CAPACITY_UNIT);
    } else if (requestCostMap.containsKey(QuotaName.WRITE_CAPACITY_UNIT)) {
      cuUsage.get(resourceId).wcu += requestCostMap.get(QuotaName.WRITE_CAPACITY_UNIT);
      feUsage.wcu += requestCostMap.get(QuotaName.WRITE_CAPACITY_UNIT);
    }
  }

  public void updateNewQuota(QuotaResource quotaResource, long rcu, long wcu) {
    cuQuota.put(quotaResource.getResourceId(), new CUQuota(rcu, wcu));
    cuUsage.put(quotaResource.getResourceId(), new CUQuota(0, 0));
  }

  public Map<String, CUQuota> getAllQuota() {
    return cuQuota;
  }

  public Map<String, CUQuota> getAllQuotaUsage() {
    return cuUsage;
  }

  private void checkSupported(QuotaName quotaName, QuotaResource quotaResource) {
    if (!SUPPORTED_QUOTA_NAMES.contains(quotaName)) {
      throw new IllegalArgumentException("Unsupported quota name: " + quotaName.name());
    }
    if (!SUPPORTED_QUOTA_RESOURCE_TYPES.contains(quotaResource.getQuotaResourceType())) {
      throw new IllegalArgumentException("Unsupported quota resource type: " + quotaResource.getQuotaResourceType());
    }
  }

  public static class CUQuota {
    static final String RCU_FIELD_NAME = "rcu";
    static final String WCU_FIELD_NAME = "wcu";

    private long rcu;
    private long wcu;

    public CUQuota() {
    }

    @JsonIgnore
    public CUQuota(JsonNode jsonNode) {
      this.rcu = jsonNode.get(RCU_FIELD_NAME).asLong();
      this.wcu = jsonNode.get(WCU_FIELD_NAME).asLong();
    }

    @JsonIgnore
    public CUQuota(long rcu, long wcu) {
      this.rcu = rcu;
      this.wcu = wcu;
    }

    @JsonIgnore
    public static boolean isQuotaNode(JsonNode jsonNode) {
      return jsonNode.has(WCU_FIELD_NAME);
    }

    @JsonIgnore
    public long getQuotaValue(QuotaName quotaName) {
      switch (quotaName) {
        case READ_CAPACITY_UNIT:
          return rcu;
        case WRITE_CAPACITY_UNIT:
          return wcu;
        default:
          throw new IllegalArgumentException("Invalid quota name: " + quotaName.name());
      }
    }

    public long getRcu() {
      return rcu;
    }

    public void setRcu(long rcu) {
      this.rcu = rcu;
    }

    public long getWcu() {
      return wcu;
    }

    public void setWcu(long wcu) {
      this.wcu = wcu;
    }
  }

  /**
   * A helper class to represent a number or a map of string to number. This is used in deserializing json string from
   * configuration for account/container quota.
   */
  @JsonDeserialize(using = MapOrQuotaDeserializer.class)
  public static class MapOrQuota {
    private final Map<String, CUQuota> containerQuotas;
    private final CUQuota quota;
    private final boolean isQuota;

    public MapOrQuota(CUQuota quota) {
      containerQuotas = null;
      this.quota = quota;
      isQuota = true;
    }

    public MapOrQuota(Map<String, CUQuota> containerQuotas) {
      this.containerQuotas = containerQuotas;
      this.quota = null;
      this.isQuota = false;
    }

    public boolean isQuota() {
      return isQuota;
    }

    public CUQuota getQuota() {
      return quota;
    }

    public Map<String, CUQuota> getContainerQuotas() {
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
      if (CUQuota.isQuotaNode(node)) {
        return new MapOrQuota(new CUQuota(node));
      } else {
        Map<String, CUQuota> innerMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
          Map.Entry<String, JsonNode> entry = iterator.next();
          innerMap.put(entry.getKey(), new CUQuota(entry.getValue()));
        }
        return new MapOrQuota(innerMap);
      }
    }
  }
}
