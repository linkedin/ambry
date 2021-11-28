package com.github.ambry.quota;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcer;
import com.github.ambry.quota.capacityunit.JsonCUQuotaSource;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class AmbryCUQuotaEnforcerTest {
  private final static long WCU = 10;
  private final static long RCU = 10;
  private final static long FE_WCU = 1024;
  private final static long FE_RCU = 1024;
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, false);
  private static AmbryCUQuotaEnforcer AMBRY_QUOTA_ENFORCER;
  private static ExceptionQuotaSource QUOTA_SOURCE;
  private static Account ACCOUNT;

  @Before
  public void setup() throws IOException {
    ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.CU_QUOTA_IN_JSON,
        String.format("{\n" + "  \"%s\": {\n" + "    \"wcu\": %d,\n" + "    \"rcu\": %d\n" + "  }\n" + "}",
            String.valueOf(ACCOUNT.getId()), WCU, RCU));
    properties.setProperty(QuotaConfig.FRONTEND_BANDWIDTH_CAPACITY_IN_JSON,
        String.format("{\n" + "  \"wcu\": %d,\n" + "  \"rcu\": %d\n" + "}", FE_WCU, FE_RCU));
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    QUOTA_SOURCE = new ExceptionQuotaSource(quotaConfig, ACCOUNT_SERVICE);
    AMBRY_QUOTA_ENFORCER = new AmbryCUQuotaEnforcer(QUOTA_SOURCE, quotaConfig.maxFrontendCuUsageToAllowExceed);
  }

  @Test
  public void chargeAndRecommendTest() throws Exception {
    Container container = ACCOUNT.getAllContainers().iterator().next();
    BlobProperties blobProperties = new BlobProperties(10, "test", ACCOUNT.getId(), container.getId(), false);
    BlobInfo blobInfo = new BlobInfo(blobProperties, new byte[10]);
    Map<QuotaName, Double> readRequestCostMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 9.0);
    Map<QuotaName, Double> writeRequestCostMap = Collections.singletonMap(QuotaName.WRITE_CAPACITY_UNIT, 9.0);
    Map<String, JsonCUQuotaSource.CUQuota> usageMap = QUOTA_SOURCE.getAllQuotaUsage();

    // 1. Test that usage is updated and recommendation is serve when usage is within limit.
    QuotaRecommendation quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.GET), blobInfo,
            readRequestCostMap);
    assertEquals(quotaRecommendation.getQuotaName(), QuotaName.READ_CAPACITY_UNIT);
    assertEquals(quotaRecommendation.getQuotaUsagePercentage(), 90, 0.1);
    assertEquals(quotaRecommendation.getRecommendedHttpStatus(), 200);
    assertEquals(quotaRecommendation.shouldThrottle(), false);
    assertEquals(usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu(), 0); // make sure that correct quota is charged.

    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.POST), blobInfo,
            writeRequestCostMap);
    assertEquals(quotaRecommendation.getQuotaName(), QuotaName.WRITE_CAPACITY_UNIT);
    assertEquals(quotaRecommendation.getQuotaUsagePercentage(), 90, 0.1);
    assertEquals(quotaRecommendation.getRecommendedHttpStatus(), 200);
    assertEquals(quotaRecommendation.shouldThrottle(), false);
    assertEquals(usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu(), 9); // make sure that correct quota is charged.

    // 2. Test that recommendation is null when quota not found.
    Account newAccount = ACCOUNT_SERVICE.generateRandomAccount(QuotaResourceType.ACCOUNT);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.chargeAndRecommend(
        createRestRequest(newAccount, newAccount.getAllContainers().iterator().next(), RestMethod.GET), blobInfo,
        readRequestCostMap);
    assertNull(quotaRecommendation);

    // 3. Test that recommendation is serve in case of any error.
    QUOTA_SOURCE.throwException = true;
    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.GET), blobInfo,
            readRequestCostMap);
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(-1, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());
    QUOTA_SOURCE.throwException = false;

    // 4. Test that usage is updated and recommendation is deny when usage >= quota.
    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.GET), blobInfo,
            readRequestCostMap);
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(180, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(429, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(true, quotaRecommendation.shouldThrottle());
    assertEquals(9, usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.POST), blobInfo,
            writeRequestCostMap);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(180, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(429, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(true, quotaRecommendation.shouldThrottle());
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.DELETE), blobInfo,
            writeRequestCostMap);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(270, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(429, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(true, quotaRecommendation.shouldThrottle());
    assertEquals(27,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    quotaRecommendation =
        AMBRY_QUOTA_ENFORCER.chargeAndRecommend(createRestRequest(ACCOUNT, container, RestMethod.PUT), blobInfo,
            writeRequestCostMap);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(360, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(429, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(true, quotaRecommendation.shouldThrottle());
    assertEquals(36,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.
  }

  @Test
  public void getResourceRecommendationTest() throws Exception {
    // 1. Test that recommendation is serve when usage is within limit and correct quota is used based on rest method.
    QuotaRecommendation quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.POST));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.PUT));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.DELETE));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());

    // 2. Test that recommendation is null when quota not found.
    Account newAccount = ACCOUNT_SERVICE.generateRandomAccount(QuotaResourceType.ACCOUNT);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(newAccount, newAccount.getAllContainers().iterator().next(), RestMethod.GET));
    assertNull(quotaRecommendation);

    // 3. Test that recommendation is serve in case of any error.
    QUOTA_SOURCE.throwException = true;
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(-1, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(200, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(false, quotaRecommendation.shouldThrottle());
    QUOTA_SOURCE.throwException = false;

    // 4. Test that recommendation is deny when usage >= quota.
    Map<String, JsonCUQuotaSource.CUQuota> usageMap = QUOTA_SOURCE.getAllQuotaUsage();
    Map<String, JsonCUQuotaSource.CUQuota> quotaMap = QUOTA_SOURCE.getAllQuota();
    String id = String.valueOf(ACCOUNT.getId());
    usageMap.put(id, new JsonCUQuotaSource.CUQuota(quotaMap.get(id).getRcu() + 1, quotaMap.get(id).getWcu()));
    float usagePercentage = (usageMap.get(id).getRcu() * 100) / quotaMap.get(id).getRcu();
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.getResourceRecommendation(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(usagePercentage, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertEquals(429, quotaRecommendation.getRecommendedHttpStatus());
    assertEquals(true, quotaRecommendation.shouldThrottle());
  }

  @Test
  public void isQuotaExceedAllowedTest() throws Exception {
    // 1. Test that quota exceed is allowed if feusage is lesser than quota.
    assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 2. Test that quota exceed is not allowed if feusage is greater than quota.
    QUOTA_SOURCE.setFeUsage(QUOTA_SOURCE.getFeQuota().getRcu(), QUOTA_SOURCE.getFeQuota().getWcu());
    assertFalse(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 3. Test that quota exceed allowed doesn't depend upon resource's quota usage.
    Map<String, JsonCUQuotaSource.CUQuota> usageMap = QUOTA_SOURCE.getAllQuotaUsage();
    Map<String, JsonCUQuotaSource.CUQuota> quotaMap = QUOTA_SOURCE.getAllQuota();
    String id = String.valueOf(ACCOUNT.getId());
    usageMap.put(id, new JsonCUQuotaSource.CUQuota(quotaMap.get(id).getRcu() + 1, quotaMap.get(id).getWcu() + 1));
    QUOTA_SOURCE.setFeUsage(0, 0);
    assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 4. Test that quota exceed is allowed in case of any error.
    QUOTA_SOURCE.throwException = true;
    QUOTA_SOURCE.setFeUsage(QUOTA_SOURCE.getFeQuota().getRcu(), QUOTA_SOURCE.getFeQuota().getWcu());
    assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));
    QUOTA_SOURCE.throwException = false;
  }

  private RestRequest createRestRequest(Account account, Container container, RestMethod restMethod) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    data.put(MockRestRequest.URI_KEY, "/");
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    data.put(MockRestRequest.HEADERS_KEY, headers);
    RestRequest restRequest = new MockRestRequest(data, null);
    return restRequest;
  }

  static class ExceptionQuotaSource extends JsonCUQuotaSource {
    private boolean throwException = false;

    public ExceptionQuotaSource(QuotaConfig config, AccountService accountService) throws IOException {
      super(config, accountService);
    }

    @Override
    public boolean isReady() {
      throwExceptionIfNeeded();
      return super.isReady();
    }

    @Override
    public void charge(RestRequest restRequest, BlobInfo blobInfo, Map<QuotaName, Double> requestCostMap) {
      throwExceptionIfNeeded();
      super.charge(restRequest, blobInfo, requestCostMap);
    }

    @Override
    public QuotaRecommendation checkResourceUsage(RestRequest restRequest) {
      throwExceptionIfNeeded();
      return super.checkResourceUsage(restRequest);
    }

    @Override
    public QuotaRecommendation checkFrontendUsage(RestRequest restRequest) {
      throwExceptionIfNeeded();
      return super.checkFrontendUsage(restRequest);
    }

    @Override
    public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
      throwExceptionIfNeeded();
      return super.getQuota(quotaResource, quotaName);
    }

    @Override
    public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
      super.updateNewQuotaResources(quotaResources);
    }

    public CUQuota getFeQuota() {
      return feQuota;
    }

    public void setFeUsage(long rcu, long wcu) {
      feUsage.setRcu(rcu);
      feUsage.setWcu(wcu);
    }

    private void throwExceptionIfNeeded() {
      if (throwException) {
        throw new RuntimeException("test exception");
      }
    }
  }
}
