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
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.quota.storage.JSONStringStorageQuotaSource;
import com.github.ambry.utils.Utils;
import com.google.common.util.concurrent.AtomicDouble;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * {@link QuotaSource} implementation backed in Json format.
 */
public class JsonBackedCUQuotaSource implements QuotaSource {
  private static final EnumSet<QuotaName> SUPPORTED_QUOTA_NAMES =
      EnumSet.of(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT);
  private static final EnumSet<QuotaResourceType> SUPPORTED_QUOTA_RESOURCE_TYPES =
      EnumSet.of(QuotaResourceType.ACCOUNT, QuotaResourceType.CONTAINER);
  private static final long DEFAULT_RCU_FOR_NEW_RESOURCE = 0;
  private static final long DEFAULT_WCU_FOR_NEW_RESOURCE = 0;
  private static final long QUOTA_AGGREGATION_WINDOW_IN_SECS = 10;
  protected final CUQuota feUsage;
  protected final CUQuota feQuota;
  private final Map<String, CUQuota> cuQuota;
  private final Map<String, CUQuota> cuUsage;
  private final ScheduledExecutorService usageRefresher;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSource}.
   * @param config The {@link QuotaSource}.
   * @param accountService The {@link AccountService} to use.
   * @throws IOException in case of any exception.
   */
  public JsonBackedCUQuotaSource(QuotaConfig config, AccountService accountService) throws IOException {
    Map<String, CUQuota> quota = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    feQuota = objectMapper.readValue(config.frontendCUCapacityInJson, CUQuota.class);
    if (config.resourceCUQuotaInJson != null && !config.resourceCUQuotaInJson.trim().isEmpty()) {
      Map<String, MapOrQuota> tempQuotas =
          objectMapper.readValue(config.resourceCUQuotaInJson, new TypeReference<Map<String, MapOrQuota>>() {
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
    cuQuota.keySet().forEach(key -> cuUsage.put(key, new CUQuota(0, 0)));
    feUsage = new CUQuota(0, 0);
    usageRefresher = Utils.newScheduler(1, true);
  }

  @Override
  public void init() {
    usageRefresher.scheduleAtFixedRate(this::refreshQuotaUsage, QUOTA_AGGREGATION_WINDOW_IN_SECS,
        QUOTA_AGGREGATION_WINDOW_IN_SECS, TimeUnit.SECONDS);
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public Quota<Long> getQuota(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    checkSupported(quotaName, quotaResource);

    String resourceId = quotaResource.getResourceId();
    if (cuQuota.containsKey(resourceId)) {
      return new Quota<>(quotaName, (long) cuQuota.get(resourceId).getQuotaValue(quotaName), quotaResource);
    }
    throw new QuotaException(String.format("Couldn't find quota for resource %s", quotaResource), false);
  }

  @Override
  public void updateNewQuotaResources(Collection<Account> accounts) {
    List<QuotaResource> quotaResources = new ArrayList<>();
    for (Account account : accounts) {
      if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
        quotaResources.add(QuotaResource.fromAccount(account));
      } else {
        for (Container container : account.getAllContainers()) {
          quotaResources.add(QuotaResource.fromContainer(container));
        }
      }
    }
    quotaResources.forEach(quotaResource -> {
      cuQuota.putIfAbsent(quotaResource.getResourceId(),
          new CUQuota(DEFAULT_RCU_FOR_NEW_RESOURCE, DEFAULT_WCU_FOR_NEW_RESOURCE));
      cuUsage.putIfAbsent(quotaResource.getResourceId(), new CUQuota(0, 0));
    });
  }

  @Override
  public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) {
    String resourceId = quotaResource.getResourceId();
    if (!cuQuota.containsKey(resourceId)) {
      return;
    }
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      cuUsage.get(resourceId).rcu.addAndGet(usageCost);
    } else {
      cuUsage.get(resourceId).wcu.addAndGet(usageCost);
    }
  }

  @Override
  public float getSystemResourceUsage(QuotaName quotaName) {
    return QuotaUtils.getUsagePercentage(feQuota.getQuotaValue(quotaName), feUsage.getQuotaValue(quotaName));
  }

  @Override
  public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) {
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      feUsage.rcu.addAndGet(usageCost);
    } else {
      feUsage.wcu.addAndGet(usageCost);
    }
  }

  @Override
  public float getUsage(QuotaResource quotaResource, QuotaName quotaName) {
    checkSupported(quotaName, quotaResource);
    String resourceId = quotaResource.getResourceId();
    if (!cuQuota.containsKey(resourceId)) {
      // If there is no quota for the resource then we treat it as no usage.
      return 0;
    }
    double usage = 0;
    if (cuUsage.containsKey(resourceId)) {
      usage = cuUsage.get(resourceId).getQuotaValue(quotaName);
    }
    return QuotaUtils.getUsagePercentage(cuQuota.get(resourceId).getQuotaValue(quotaName), usage);
  }

  @Override
  public void shutdown() {
    if(usageRefresher != null) {
      usageRefresher.shutdown();
    }
  }

  /**
   * @return A {@link Map} of quota resource id to {@link CUQuota} representing quota for all the resources that this
   * implementation knows about.
   */
  public Map<String, CUQuota> getAllQuota() {
    return cuQuota;
  }

  /**
   * @return A {@link Map} of quota resource id to {@link CUQuota} representing usage for all the resources that this
   * implementation knows about.
   */
  public Map<String, CUQuota> getAllQuotaUsage() {
    return cuUsage;
  }

  /**
   * Checks if the specified {@link QuotaName} and {@link QuotaResourceType} are handled by this quota source.
   * @param quotaName {@link QuotaName} object.
   * @param quotaResource {@link QuotaResource} object.
   */
  private void checkSupported(QuotaName quotaName, QuotaResource quotaResource) {
    if (!SUPPORTED_QUOTA_NAMES.contains(quotaName)) {
      throw new IllegalArgumentException("Unsupported quota name: " + quotaName.name());
    }
    if (!SUPPORTED_QUOTA_RESOURCE_TYPES.contains(quotaResource.getQuotaResourceType())) {
      throw new IllegalArgumentException("Unsupported quota resource type: " + quotaResource.getQuotaResourceType());
    }
  }

  /**
   * Atomically reset the usage of all the resources.
   */
  private synchronized void refreshQuotaUsage() {
    for (String resourceId : cuUsage.keySet()) {
      cuUsage.put(resourceId, new CUQuota(0, 0));
    }
    feUsage.setWcu(0);
    feUsage.setRcu(0);
  }

  /**
   * Class encapsulating the Read_Capacity_Unit and Write_Capacity_Unit quotas for resource.
   */
  public static class CUQuota {
    static final String RCU_FIELD_NAME = "rcu";
    static final String WCU_FIELD_NAME = "wcu";

    private final AtomicDouble rcu;
    private final AtomicDouble wcu;

    /**
     * Constructor for {@link CUQuota}.
     */
    public CUQuota() {
      rcu = new AtomicDouble(0);
      wcu = new AtomicDouble(0);
    }

    @JsonIgnore
    public CUQuota(JsonNode jsonNode) {
      this.rcu = new AtomicDouble(jsonNode.get(RCU_FIELD_NAME).asLong());
      this.wcu = new AtomicDouble(jsonNode.get(WCU_FIELD_NAME).asLong());
    }

    @JsonIgnore
    public CUQuota(long rcu, long wcu) {
      this.rcu = new AtomicDouble(rcu);
      this.wcu = new AtomicDouble(wcu);
    }

    @JsonIgnore
    public static boolean isQuotaNode(JsonNode jsonNode) {
      return jsonNode.has(WCU_FIELD_NAME);
    }

    @JsonIgnore
    public double getQuotaValue(QuotaName quotaName) {
      switch (quotaName) {
        case READ_CAPACITY_UNIT:
          return rcu.get();
        case WRITE_CAPACITY_UNIT:
          return wcu.get();
        default:
          throw new IllegalArgumentException("Invalid quota name: " + quotaName.name());
      }
    }

    /**
     * @return Read Capacity Unit quota value.
     */
    public double getRcu() {
      return rcu.get();
    }

    /**
     * Set the Read Capacity Unit quota to the specified value.
     */
    public void setRcu(long rcu) {
      this.rcu.set(rcu);
    }

    /**
     * @return Write Capacity Unit quota value.
     */
    public double getWcu() {
      return wcu.get();
    }

    /**
     * Set the Write Capacity Unit quota to the specified value.
     */
    public void setWcu(long wcu) {
      this.wcu.set(wcu);
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

    /**
     * Constructor for {@link MapOrQuota} class.
     * @param quota {@link CUQuota} object.
     */
    public MapOrQuota(CUQuota quota) {
      containerQuotas = null;
      this.quota = quota;
      isQuota = true;
    }

    /**
     * Constructor for {@link MapOrQuota} class.
     * @param containerQuotas {@link Map} of container id to {@link CUQuota} object.
     */
    public MapOrQuota(Map<String, CUQuota> containerQuotas) {
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
     * @return CUQuota object.
     */
    public CUQuota getQuota() {
      return quota;
    }

    /**
     * @return Map of container id to {@link CUQuota}.
     */
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
