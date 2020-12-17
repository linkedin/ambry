package com.github.ambry.quota;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;


public class RejectQuotaEnforcer implements RequestQuotaEnforcer {
  private final QuotaSource quotaSource;

  public RejectQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
  }

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo) {
    return new RejectEnforcementRecommendation();
  }

  @Override
  public EnforcementRecommendation recommend(RestRequest restRequest) {
    return new RejectEnforcementRecommendation();
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {
  }
}
