package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;

/**
 * Factory to create {@link DummyQuotaSource}.
 */
public class DummyQuotaSourceFactory implements QuotaSourceFactory {

  /**
   * Constructor for {@link DummyQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   */
  public DummyQuotaSourceFactory(QuotaConfig quotaConfig) {

  }

  @Override
  public QuotaSource getQuotaSource() {
    return new DummyQuotaSource();
  }
}
