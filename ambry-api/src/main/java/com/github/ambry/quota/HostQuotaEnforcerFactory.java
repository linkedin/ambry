package com.github.ambry.quota;

/**
 * Factory to instantiate {@link HostQuotaEnforcer}.
 */
public interface HostQuotaEnforcerFactory {
  /**
   * Build and return {@link HostQuotaEnforcer} class.
   * @return HostQuotaEnforcer object.
   */
  HostQuotaEnforcer getHostQuotaEnforcer();
}
