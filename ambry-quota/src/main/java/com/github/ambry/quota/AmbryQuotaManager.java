/*
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
package com.github.ambry.quota;

import com.codahale.metrics.Timer;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link QuotaManager} implementation that instantiates the {@link QuotaEnforcer}s and {@link QuotaSource}s from the
 * factories specified in the {@link QuotaConfig#requestQuotaEnforcerSourcePairInfoJson}.
 *
 * The check and charge operations in this implementation are not atomic. For an atomic implementation of
 * {@link QuotaManager} see {@link AtomicAmbryQuotaManager}.
 *
 * For the quota and usage values, this implementation is only as thread safe as the individual {@link QuotaSource}
 * implementations.
 */
public class AmbryQuotaManager implements QuotaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmbryQuotaManager.class);
  protected final Set<QuotaEnforcer> quotaEnforcers;
  protected final QuotaMetrics quotaMetrics;
  private final QuotaRecommendationMergePolicy quotaRecommendationMergePolicy;
  private final QuotaConfig quotaConfig;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private volatile QuotaMode quotaMode;

  /**
   * Constructor for {@link AmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaRecommendationMergePolicy {@link QuotaRecommendationMergePolicy} object that makes the overall recommendation.
   * @param accountService {@link AccountService} object to get all the accounts and container information.
   * @param accountStatsStore {@link AccountStatsStore} object to get all the account stats related information.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public AmbryQuotaManager(QuotaConfig quotaConfig, QuotaRecommendationMergePolicy quotaRecommendationMergePolicy,
      AccountService accountService, AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) throws ReflectiveOperationException {
    List<Pair<String, String>> quotaEnforcerSourcePairs =
        parseQuotaEnforcerAndSourceInfo(quotaConfig.requestQuotaEnforcerSourcePairInfoJson);
    Map<String, QuotaSource> quotaSourceObjectMap =
        buildQuotaSources(quotaEnforcerSourcePairs.stream().map(Pair::getSecond).collect(Collectors.toList()),
            quotaConfig, accountService, quotaMetrics, routerConfig);
    quotaEnforcers = new HashSet<>();
    for (Pair<String, String> quotaEnforcerSourcePair : quotaEnforcerSourcePairs) {
      quotaEnforcers.add(((QuotaEnforcerFactory) Utils.getObj(quotaEnforcerSourcePair.getFirst(), quotaConfig,
          quotaSourceObjectMap.get(quotaEnforcerSourcePair.getSecond()), accountStatsStore,
          quotaMetrics)).getQuotaEnforcer());
    }
    if (quotaEnforcers.isEmpty()) {
      LOGGER.warn("No quota enforcers in AmbryQuotaManager. No quota will be tracked or enforced.");
    }
    this.quotaRecommendationMergePolicy = quotaRecommendationMergePolicy;
    this.quotaConfig = quotaConfig;
    this.quotaMetrics = quotaMetrics;
    this.quotaMode = quotaConfig.throttlingMode;
    accountService.addAccountUpdateConsumer(this::onAccountUpdateNotification);
  }

  @Override
  public synchronized void init() throws Exception {
    if (!initialized.get()) {
      Timer.Context timer = quotaMetrics.quotaManagerInitTime.time();
      try {
        for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
          quotaEnforcer.init();
        }
      } catch (Exception e) {
        LOGGER.error("Failed to init quotaEnforcer", e);
        throw e;
      } finally {
        timer.stop();
      }
      LOGGER.info("AmbryQuotaManager initialized.");
      initialized.set(true);
    } else {
      LOGGER.warn("AmbryQuotaManager already initialized.");
    }
  }

  /**
   * {@inheritDoc}
   *
   * If there are any {@link QuotaEnforcer#recommend} calls that throw exception, this implementation logs the exception,
   * and makes a best effort to gather as many recommendations as possible. Recommendations for some {@link QuotaName}s
   * will be missing in those cases.
   */
  @Override
  public ThrottlingRecommendation recommend(RestRequest restRequest) throws QuotaException {
    quotaMetrics.recommendRate.mark();
    if (quotaEnforcers.isEmpty()) {
      quotaMetrics.noQuotaRecommendationRate.mark();
      return null;
    }
    ThrottlingRecommendation throttlingRecommendation;
    Timer.Context timer = quotaMetrics.quotaRecommendationTime.time();
    try {
      List<QuotaRecommendation> quotaRecommendations = new ArrayList<>();
      for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
        try {
          quotaRecommendations.add(quotaEnforcer.recommend(restRequest));
        } catch (QuotaException quotaException) {
          LOGGER.warn("Could not get recommendation for quota {} due to exception: {}",
              quotaEnforcer.supportedQuotaNames(), quotaException.getMessage());
        }
      }
      if (quotaRecommendations.isEmpty()) {
        quotaMetrics.noRecommendationRate.mark();
      } else if (quotaRecommendations.size() < quotaEnforcers.size()) {
        quotaMetrics.partialQuotaRecommendationRate.mark();
      }
      throttlingRecommendation = quotaRecommendationMergePolicy.mergeEnforcementRecommendations(quotaRecommendations);
      if (throttlingRecommendation.shouldThrottle()) {
        quotaMetrics.quotaExceedRecommendationRate.mark();
      }
    } finally {
      timer.stop();
    }
    return throttlingRecommendation;
  }

  /**
   * {@inheritDoc}
   *
   * This implementation does check and charge in two phases.
   * In the first phase, it obtains recommendations from all the {@link QuotaEnforcer}s and uses the recommendations (in
   * conjunction with the parameters) to determine if the request should be charged. Any exception from any
   * {@link QuotaEnforcer}s in this phase are bubbled up to the caller and no charge is made.
   *
   * In the second phase, after it has determined that the charge needs to be done, it attempts to charge the individual
   * {@link QuotaEnforcer}s one by one. The behavior in this phase is to make a best effort to charge as many
   * {@link QuotaEnforcer}s as possible and send a {@link QuotaAction#ALLOW} recommendation. If there is an exception in
   * any {@link QuotaEnforcer#charge} call, that exception is logged and charge is still attempted for rest of the
   * {@link QuotaEnforcer}s. As such, there is a potential for partial charge to happen in this implementation.
   */
  @Override
  public QuotaAction chargeAndRecommend(RestRequest restRequest, Map<QuotaName, Double> requestCostMap,
      boolean shouldCheckIfQuotaExceedAllowed, boolean forceCharge) throws QuotaException {
    quotaMetrics.chargeAndRecommendRate.mark();
    if (quotaEnforcers.isEmpty()) {
      // If there are no enforcers that means quota is not enforced, which is treated as usage within quota.
      quotaMetrics.quotaNotChargedRate.mark();
      return QuotaAction.ALLOW;
    }
    Timer.Context timer = quotaMetrics.quotaChargeTime.time();
    QuotaAction recommendedQuotaAction = QuotaAction.ALLOW;
    try {
      if (!forceCharge) {
        // Check if request is allowed by all quota enforcers. If an enforcer recommends to throttle and
        // shouldCheckIfQuotaExceedAllowed is true, then allow request only if the enforcer allows to exceed quota.
        for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
          if (recommendedQuotaAction != QuotaAction.REJECT) {
            QuotaAction quotaAction = quotaEnforcer.recommend(restRequest).getQuotaAction();
            if (QuotaUtils.shouldThrottle(quotaAction)) {
              if (!(shouldCheckIfQuotaExceedAllowed && quotaEnforcer.isQuotaExceedAllowed(restRequest))) {
                // this means recommendation is to either delay or reject, and usage is not allowed to exceed quota.
                recommendedQuotaAction = quotaAction;
                quotaMetrics.throttleRate.mark();
              } else {
                quotaMetrics.quotaExceedAllowedRate.mark();
              }
            }
          }
        }
      } else {
        quotaMetrics.forcedChargeRate.mark();
      }
      // charge the cost if the request is not throttled.
      if (!QuotaUtils.shouldThrottle(recommendedQuotaAction)) {
        chargeQuotaUsage(restRequest, requestCostMap);
      }
      return recommendedQuotaAction;
    } finally {
      timer.stop();
    }
  }

  @Override
  public void shutdown() {
    if (initialized.compareAndSet(true, false)) {
      for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
        quotaEnforcer.shutdown();
      }
      LOGGER.info("AmbryQuotaManager shutdown complete.");
    } else {
      LOGGER.warn("AmbryQuotaManager already shutdown or not initialized.");
    }
  }

  @Override
  public QuotaConfig getQuotaConfig() {
    return quotaConfig;
  }

  @Override
  public QuotaMode getQuotaMode() {
    return quotaMode;
  }

  @Override
  public void setQuotaMode(QuotaMode mode) {
    this.quotaMode = mode;
  }

  /**
   * Notify {@link QuotaSource}s about creation of new Ambry {@link Account} or {@link com.github.ambry.account.Container}.
   * Note that this method can also get notification about changes to account or container unrelated to quota.
   * @param updatedAccounts {@link Collection} of {@link Account}s updated.
   */
  protected void onAccountUpdateNotification(Collection<Account> updatedAccounts) {
    quotaMetrics.accountUpdateNotificationCount.inc(updatedAccounts.size());
    quotaEnforcers.stream()
        .map(QuotaEnforcer::getQuotaSource)
        .filter(Objects::nonNull)
        .forEach(quotaSource -> quotaSource.updateNewQuotaResources(updatedAccounts));
  }

  /**
   * Charge the specified {@code requestCostMap} for the {@link RestRequest}.
   * @param restRequest {@link RestRequest} for which usage should be charged.
   * @param requestCostMap {@link Map} of {@link QuotaName} to usage to be charged.
   */
  protected void chargeQuotaUsage(RestRequest restRequest, Map<QuotaName, Double> requestCostMap) {
    boolean partialCharge = false;
    boolean noCharge = true;
    for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
      try {
        quotaEnforcer.charge(restRequest, requestCostMap);
        noCharge = false;
      } catch (QuotaException quotaException) {
        LOGGER.warn("Exception {} while charging for {} quotas.", quotaException, quotaEnforcer.supportedQuotaNames());
        partialCharge = true;
      }
    }
    if (noCharge) {
      quotaMetrics.noChargeRate.mark();
    } else if (partialCharge) {
      quotaMetrics.partialChargeRate.mark();
    }
  }

  /**
   * Parse the json config for {@link QuotaEnforcer} and {@link QuotaSource} factory pair and return them in a {@link Map}.
   * @param quotaEnforcerSourceJson json config string.
   * @return List of {@link QuotaEnforcer} and {@link QuotaSource} factory {@link Pair}s.
   */
  private List<Pair<String, String>> parseQuotaEnforcerAndSourceInfo(String quotaEnforcerSourceJson) {
    if (quotaEnforcerSourceJson.isEmpty()) {
      return Collections.emptyList();
    }
    List<Pair<String, String>> quotaEnforcerSourcePairs = new ArrayList<>();
    JSONObject root = new JSONObject(quotaEnforcerSourceJson);
    JSONArray all = root.getJSONArray(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR);
    for (int i = 0; i < all.length(); i++) {
      JSONObject entry = all.getJSONObject(i);
      String enforcer = entry.getString(QuotaConfig.ENFORCER_STR);
      String source = entry.getString(QuotaConfig.SOURCE_STR);
      quotaEnforcerSourcePairs.add(new Pair<>(enforcer, source));
    }
    return quotaEnforcerSourcePairs;
  }

  /**
   * Create the specified {@link QuotaSource} objects.
   * @param quotaSourceFactoryClasses {@link Collection} of {@link QuotaSourceFactory} classes.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   * @return Map of {@link QuotaSourceFactory} class names to {@link QuotaSource} objects.
   * @throws ReflectiveOperationException if the source objects could not be created.
   */
  private Map<String, QuotaSource> buildQuotaSources(Collection<String> quotaSourceFactoryClasses,
      QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics, RouterConfig routerConfig)
      throws ReflectiveOperationException {
    Map<String, QuotaSource> quotaSourceObjectMap = new HashMap<>();
    for (String quotaSourceFactoryClass : quotaSourceFactoryClasses) {
      if (!quotaSourceObjectMap.containsKey(quotaSourceFactoryClass)) {
        quotaSourceObjectMap.put(quotaSourceFactoryClass,
            ((QuotaSourceFactory) Utils.getObj(quotaSourceFactoryClass, quotaConfig, accountService,
                quotaMetrics, routerConfig)).getQuotaSource());
      }
    }
    return quotaSourceObjectMap;
  }
}
