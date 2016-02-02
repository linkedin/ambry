package com.github.ambry.config;

/**
 * Configuration for {@code AmbryOperationPolicy}. Parameter values are based on what type of the
 * operation is, which can be {@code PUT, GET, DELETE}.
 */
public class AmbryPolicyConfig {
  @Config("router.put.policy.local.only")
  @Default("false")
  public final boolean routerPutPolicyLocalOnly;

  @Config("router.put.policy.local.barrier")
  @Default("true")
  public final boolean routerPutPolicyLocalBarrier;

  @Config("router.put.policy.success.target")
  @Default("2")
  public final int routerPutPolicySuccessTarget;

  @Config("router.put.policy.local.parallel.factor")
  @Default("3")
  public final int routerPutPolicyLocalParallelFactor;

  @Config("router.put.policy.remote.parallel.factor.per.dc")
  @Default("1")
  public final int routerPutPolicyRemoteParallelFactorPerDc;

  @Config("router.put.policy.total.remote.parallel.factor")
  @Default("2")
  public final int routerPutPolicyTotalRemoteParallelFactor;

  @Config("router.get.policy.local.only")
  @Default("false")
  public final boolean routerGetPolicyLocalOnly;

  @Config("router.get.policy.local.barrier")
  @Default("true")
  public final boolean routerGetPolicyLocalBarrier;

  @Config("router.get.policy.success.target")
  @Default("2")
  public final int routerGetPolicySuccessTarget;

  @Config("router.get.policy.local.parallel.factor")
  @Default("3")
  public final int routerGetPolicyLocalParallelFactor;

  @Config("router.get.policy.remote.parallel.factor.per.dc")
  @Default("1")
  public final int routerGetPolicyRemoteParallelFactorPerDc;

  @Config("router.get.policy.total.remote.parallel.factor")
  @Default("2")
  public final int routerGetPolicyTotalRemoteParallelFactor;

  @Config("router.delete.policy.local.only")
  @Default("false")
  public final boolean routerDeletePolicyLocalOnly;

  @Config("router.delete.policy.local.barrier")
  @Default("true")
  public final boolean routerDeletePolicyLocalBarrier;

  @Config("router.delete.policy.success.target")
  @Default("2")
  public final int routerDeletePolicySuccessTarget;

  @Config("router.delete.policy.local.parallel.factor")
  @Default("3")
  public final int routerDeletePolicyLocalParallelFactor;

  @Config("router.delete.policy.remote.parallel.factor.per.dc")
  @Default("1")
  public final int routerDeletePolicyRemoteParallelFactorPerDc;

  @Config("router.delete.policy.total.remote.parallel.factor")
  @Default("2")
  public final int routerDeletePolicyTotalRemoteParallelFactor;

  public AmbryPolicyConfig(VerifiableProperties verifiableProperties) {
    routerPutPolicyLocalOnly = verifiableProperties.getBoolean("router.put.policy.local.only", false);
    routerPutPolicyLocalBarrier = verifiableProperties.getBoolean("router.put.policy.local.barrier", true);
    routerPutPolicySuccessTarget =
        verifiableProperties.getIntInRange("router.put.policy.success.target", 2, 1, Integer.MAX_VALUE);
    routerPutPolicyLocalParallelFactor =
        verifiableProperties.getIntInRange("router.put.policy.local.parallel.factor", 3, 1, Integer.MAX_VALUE);
    routerPutPolicyRemoteParallelFactorPerDc =
        verifiableProperties.getIntInRange("router.put.policy.remote.parallel.factor.per.dc", 1, 1, Integer.MAX_VALUE);
    routerPutPolicyTotalRemoteParallelFactor =
        verifiableProperties.getIntInRange("router.put.policy.total.remote.parallel.factor", 2, 1, Integer.MAX_VALUE);

    routerGetPolicyLocalOnly = verifiableProperties.getBoolean("router.get.policy.local.only", false);
    routerGetPolicyLocalBarrier = verifiableProperties.getBoolean("router.get.policy.local.barrier", true);
    routerGetPolicySuccessTarget =
        verifiableProperties.getIntInRange("router.get.policy.success.target", 1, 1, Integer.MAX_VALUE);
    routerGetPolicyLocalParallelFactor =
        verifiableProperties.getIntInRange("router.get.policy.local.parallel.factor", 3, 1, Integer.MAX_VALUE);
    routerGetPolicyRemoteParallelFactorPerDc =
        verifiableProperties.getIntInRange("router.get.policy.remote.parallel.factor.per.dc", 1, 1, Integer.MAX_VALUE);
    routerGetPolicyTotalRemoteParallelFactor =
        verifiableProperties.getIntInRange("router.get.policy.total.remote.parallel.factor", 2, 1, Integer.MAX_VALUE);

    routerDeletePolicyLocalOnly = verifiableProperties.getBoolean("router.delete.policy.local.only", false);
    routerDeletePolicyLocalBarrier = verifiableProperties.getBoolean("router.delete.policy.local.barrier", true);
    routerDeletePolicySuccessTarget =
        verifiableProperties.getIntInRange("router.delete.policy.success.target", 2, 1, Integer.MAX_VALUE);
    routerDeletePolicyLocalParallelFactor =
        verifiableProperties.getIntInRange("router.delete.policy.local.parallel.factor", 3, 1, Integer.MAX_VALUE);
    routerDeletePolicyRemoteParallelFactorPerDc = verifiableProperties
        .getIntInRange("router.delete.policy.remote.parallel.factor.per.dc", 1, 1, Integer.MAX_VALUE);
    routerDeletePolicyTotalRemoteParallelFactor = verifiableProperties
        .getIntInRange("router.delete.policy.total.remote.parallel.factor", 2, 1, Integer.MAX_VALUE);
  }
}
