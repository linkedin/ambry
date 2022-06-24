package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.RouterConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimplerOperationTracker implements OperationTracker {
  private static final Logger logger = LoggerFactory.getLogger(SimpleOperationTracker.class);
  protected final String datacenterName;
  protected final String originatingDcName;
  protected final int successTarget;
  protected final int parallelism;

  // How many NotFound responses from originating dc will terminate the operation.
  // It is set to tolerate one random failure in the originating dc if all other responses are not found.
  protected final int totalReplicaCount;
  protected final int totalReplicaInOriginatingDcCount;
  protected final int originatingDcNotFoundFailureThreshold;
  protected final LinkedList<ReplicaId> replicaPool = new LinkedList<>();
  protected final NonBlockingRouterMetrics routerMetrics;
  protected final PartitionId partitionId;
  private final OpTrackerIterator otIterator;
  private final RouterOperation routerOperation;
  private final RouterConfig routerConfig;
  private final boolean crossColoEnabled;
  protected int inflightCount = 0;
  protected int originatingDcResponseCount = 0;
  protected int successCount = 0;
  protected int failedCount = 0;
  protected int disabledCount = 0;
  protected int originatingDcNotFoundCount = 0;
  protected ReplicaId lastReturnedByIterator = null;
  private Iterator<ReplicaId> replicaIterator;
  private final Map<ReplicaState, List<ReplicaId>> allDcReplicasByState;

  private static final Map<RouterOperation, EnumSet<ReplicaState>> routerOperationToEligibleReplicaStates =
      new HashMap<>();

  static {
    routerOperationToEligibleReplicaStates.put(RouterOperation.GetBlobOperation,
        EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.INACTIVE));
    routerOperationToEligibleReplicaStates.put(RouterOperation.GetBlobInfoOperation,
        EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.INACTIVE));
    routerOperationToEligibleReplicaStates.put(RouterOperation.TtlUpdateOperation,
        EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
    routerOperationToEligibleReplicaStates.put(RouterOperation.DeleteOperation,
        EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
    routerOperationToEligibleReplicaStates.put(RouterOperation.UndeleteOperation,
        EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
    routerOperationToEligibleReplicaStates.put(RouterOperation.PutOperation,
        EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
  }

  /**
   * Constructor for an {@code SimpleOperationTracker}. In constructor, there is a config allowing operation tracker to
   * use eligible replicas to populate replica pool. ("eligible" replicas are those in required states for specific
   * operation)
   * Following are different types of operation and their eligible replica states:
   *  ---------------------------------------------------------
   * |  Operation Type  |        Eligible Replica State        |
   *  ---------------------------------------------------------
   * |      GET         | STANDBY, LEADER, BOOTSTRAP, INACTIVE |
   * |    DELETE        | STANDBY, LEADER, BOOTSTRAP           |
   * |   TTLUpdate      | STANDBY, LEADER, BOOTSTRAP           |
   * |   UNDELETE       | STANDBY, LEADER, BOOTSTRAP           |
   * |      PUT         | STANDBY, LEADER                      |
   *  ---------------------------------------------------------
   * Following are dynamic configs when replica state is taken into consideration: (N is number of eligible replicas)
   *  -----------------------------------------------------------------------
   * |  Operation Type  |        Parallelism              |  Success Target  |
   *  -----------------------------------------------------------------------
   * |     GET          | 1~2 decided by adaptive tracker |         1        |
   * |     PUT          |           N                     |       N - 1      |
   * |    DELETE        |          3~N                    |         2        |
   * |   TTLUpdate      |          3~N                    |         2        |
   * |   UNDELETE       |          3~N                    |  Global Quorum   |
   *  -----------------------------------------------------------------------
   *  Note: for now, we still use 3 as parallelism for DELETE/TTLUpdate/UNDELETE even though there are N eligible replicas, this
   *        can be adjusted to any number between 3 and N (inclusive)
   *        For Undelete, it needs to reach global quorum to succeed. A dedicated operation tracker is created to check that.
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param routerOperation The {@link RouterOperation} which {@link SimpleOperationTracker} is associated with.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName The original DC where blob was put.
   * @param shuffleReplicas Indicates if the replicas need to be shuffled.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to use.
   */
  SimplerOperationTracker(RouterConfig routerConfig, RouterOperation routerOperation, PartitionId partitionId,
      String originatingDcName, boolean shuffleReplicas, NonBlockingRouterMetrics routerMetrics) {
    // populate tracker parameters based on operation type
    this.routerConfig = routerConfig;
    this.routerOperation = routerOperation;
    this.routerMetrics = routerMetrics;
    this.datacenterName = routerConfig.routerDatacenterName;
    this.partitionId = partitionId;
    // If the originating DC is null, then the originating DC is decommissioned, we need to assign a new originating DC
    if (originatingDcName == null) {
      originatingDcName = maybeReassignOriginatingDC(partitionId.getReplicaIds());
    }
    this.originatingDcName = originatingDcName;

    // Note that we get a snapshot of replicas by state only once in this class, and use the same snapshot everywhere
    // to avoid the case where a replica state might change in between an operation.
    allDcReplicasByState =
        (Map<ReplicaState, List<ReplicaId>>) partitionId.getReplicaIdsByStates(EnumSet.allOf(ReplicaState.class), null);
    List<ReplicaId> eligibleReplicas = getEligibleReplicasForOperation(routerOperation);
    List<ReplicaId> offlineReplicas = new ArrayList<>();

    switch (routerOperation) {
      case GetBlobOperation:
      case GetBlobInfoOperation:
        successTarget = routerConfig.routerGetSuccessTarget;
        parallelism = routerConfig.routerGetRequestParallelism;
        crossColoEnabled = routerConfig.routerGetCrossDcEnabled;
        break;
      case PutOperation:
        successTarget = routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.max(eligibleReplicas.size() - 1,
            routerConfig.routerPutSuccessTarget) : routerConfig.routerPutSuccessTarget;
        parallelism = routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.min(eligibleReplicas.size(),
            routerConfig.routerPutRequestParallelism) : routerConfig.routerPutRequestParallelism;
        crossColoEnabled = false;
        break;
      case DeleteOperation:
        successTarget = routerConfig.routerDeleteSuccessTarget;
        parallelism = routerConfig.routerDeleteRequestParallelism;
        crossColoEnabled = true;
        break;
      case TtlUpdateOperation:
        successTarget = routerConfig.routerTtlUpdateSuccessTarget;
        parallelism = routerConfig.routerTtlUpdateRequestParallelism;
        crossColoEnabled = true;
        break;
      case UndeleteOperation:
        parallelism = routerConfig.routerUndeleteRequestParallelism;
        crossColoEnabled = true;
        // Undelete operation need to get global quorum. It will require a different criteria for success.
        // Here set the success target to the number of eligible replicas.
        successTarget = eligibleReplicas.size();
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + routerOperation);
    }
    if (parallelism < 1) {
      throw new IllegalArgumentException(
          "Parallelism has to be > 0. diskParallelism=" + parallelism + ", routerOperation=" + routerOperation);
    }

    List<? extends ReplicaId> replicas =
        routerConfig.routerGetEligibleReplicasByStateEnabled ? eligibleReplicas : partitionId.getReplicaIds();

    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    LinkedList<ReplicaId> backupReplicas = new LinkedList<>();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    LinkedList<ReplicaId> downReplicasInOriginatingDc = new LinkedList<>();
    if (shuffleReplicas) {
      Collections.shuffle(replicas);
    }
    // The priority here is local dc replicas, originating dc replicas, other dc replicas, down replicas.
    List<ReplicaId> examinedReplicas = new ArrayList<>();
    for (ReplicaId replicaId : replicas) {
      examinedReplicas.add(replicaId);
      String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
      boolean isLocalDcReplica = replicaDcName.equals(datacenterName);
      boolean isOriginatingDcReplica = replicaDcName.equals(this.originatingDcName);
      if (!replicaId.isDown()) {
        if (isLocalDcReplica) {
          addToBeginningOfPool(replicaId);
        } else if (crossColoEnabled && isOriginatingDcReplica) {
          addToEndOfPool(replicaId);
        } else if (crossColoEnabled) {
          backupReplicas.addFirst(replicaId);
        }
      } else {
        if (isOriginatingDcReplica) {
          downReplicasInOriginatingDc.addFirst(replicaId);
        }
        if (isLocalDcReplica) {
          downReplicas.addFirst(replicaId);
        } else if (crossColoEnabled) {
          downReplicas.addLast(replicaId);
        }
      }
    }
    downReplicas.removeAll(downReplicasInOriginatingDc);
    List<ReplicaId> backupReplicasToCheck = new ArrayList<>(backupReplicas);
    List<ReplicaId> downReplicasToCheck = new ArrayList<>(downReplicas);
    // Add replicas that are neither in local dc nor in originating dc.
    backupReplicas.forEach(this::addToEndOfPool);
    // Add replicas that are down in originating DC
    if (routerOperation != RouterOperation.PutOperation) {
      downReplicasInOriginatingDc.forEach(this::addToEndOfPool);
    }
    if (routerConfig.routerOperationTrackerIncludeDownReplicas) {
      // Add those replicas deemed by native failure detector to be down
      downReplicas.forEach(this::addToEndOfPool);
    }
    totalReplicaCount = replicaPool.size();
    totalReplicaInOriginatingDcCount = (int) replicaPool.stream()
        .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(this.originatingDcName))
        .count();

    // MockPartitionId.getReplicaIds() is returning a shared reference which may cause race condition.
    // Please report the test failure if you run into this exception.
    Supplier<IllegalArgumentException> notEnoughReplicasException = () -> new IllegalArgumentException(
        generateErrorMessage(partitionId, examinedReplicas, replicaPool, backupReplicasToCheck, downReplicasToCheck,
            routerOperation));
    // initialize this to the replica type of the first request to send so that parallelism is set correctly for the
    // first request
    if (totalReplicaCount < successTarget) {
      throw notEnoughReplicasException.get();
    }

    int numActiveReplicasInOriginatingDc =
        getEligibleReplicas(this.originatingDcName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)).size();
    if (routerConfig.routerOperationTrackerTerminateOnNotFoundEnabled && totalReplicaInOriginatingDcCount > 0
        && numActiveReplicasInOriginatingDc >= routerConfig.routerPutSuccessTarget) {
      // This condition accounts for following cases:
      // 1. Intermediate state of moving replicas (there could be 6 replicas in originating dc temporarily).
      // 2. Looks at all replicas (instead of routerPutSuccessTarget) in originating DC since one of the replicas
      // in which the blob was PUT originally could be in error state or is being rebuilt from scratch. By looking at
      // all replicas, we make sure that we don't miss the other replica in which blob was PUT.
      // 3. Uses this feature only if there are at least 'routerPutSuccessTarget' number of replicas in leader or
      // standby state. This ensures that all the replicas in PUT quorum are not down (or being rebuilt) which means
      // that there is at least one active replica containing the blob.
      originatingDcNotFoundFailureThreshold = totalReplicaInOriginatingDcCount - 1;
    } else {
      originatingDcNotFoundFailureThreshold = 0;
    }
    this.otIterator = new OpTrackerIterator();
    logger.debug(
        "Router operation type: {}, successTarget = {}, parallelism = {}, originatingDcNotFoundFailureThreshold = {}, replicaPool = {}",
        routerOperation, successTarget, parallelism, originatingDcNotFoundFailureThreshold, replicaPool);
  }

  /**
   * The dynamic success target is introduced mainly for following use case:
   * In the intermediate state of "move replica", when decommission of old replicas is initiated(but hasn't transited to
   * INACTIVE yet), the PUT requests should be rejected on old replicas. For frontends, they are seeing both old and new
   * replicas(lets say 3 old and 3 new) and the success target should be 6 - 1 = 5. In the aforementioned scenario, PUT
   * request failed on 3 old replicas. It seems we should fail whole PUT operation because number of remaining requests
   * is already less than success target.
   * From another point of view, however, PUT request is highly likely to succeed on 3 new replicas and we actually
   * could consider it success without generating "slip put" (which makes PUT latency worse). The reason is, if new PUTs
   * already succeeded on at least 2 new replicas,  read-after-write should always succeed because frontends are always
   * able to see new replicas and subsequent READ/DELETE/TtlUpdate request should succeed on at least 2 aforementioned
   * new replicas.
   */
  @Override
  public boolean hasSucceeded() {
    boolean hasSucceeded;
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      int dynamicSuccessTarget = Math.max(totalReplicaCount - disabledCount - 1, routerConfig.routerPutSuccessTarget);
      hasSucceeded = successCount >= dynamicSuccessTarget;
    } else {
      hasSucceeded = successCount >= successTarget;
    }
    return hasSucceeded;
  }

  @Override
  public boolean maybeFailedDueToOfflineReplicas() {
    return false;
  }

  @Override
  public boolean hasFailedOnNotFound() {
    if (routerOperation == RouterOperation.PutOperation) {
      return false;
    }
    if (hasSucceeded()) {
      return false;
    }
    return originatingDcNotFoundFailureThreshold > 0 && originatingDcResponseCount == totalReplicaInOriginatingDcCount
        && originatingDcNotFoundCount >= originatingDcNotFoundFailureThreshold;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    inflightCount--;
    boolean fromOriginatingDC = replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName);
    if (fromOriginatingDC) {
      originatingDcResponseCount++;
    }
    // once a response has been received, a replica is no longer in the pool or currently in flight.
    switch (trackedRequestFinalState) {
      case SUCCESS:
        successCount++;
        break;
      // Request disabled may happen when PUT/DELETE/TTLUpdate requests attempt to perform on replicas that are being
      // decommissioned (i.e STANDBY -> INACTIVE). This is because decommission may take some time and frontends still
      // hold old view. Aforementioned requests are rejected by server with Temporarily_Disabled error. For DELETE/TTLUpdate,
      // even though we may receive such errors, the success target is still same(=2). For PUT, we have to adjust the
      // success target (quorum) to let some PUT operations (with at least 2 requests succeeded on new replicas) succeed.
      // Currently, disabledCount only applies to PUT operation.
      case REQUEST_DISABLED:
        disabledCount++;
        break;
      default:
        failedCount++;
        // NOT_FOUND is a special error. When tracker sees >= numReplicasInOriginatingDc - 1 "NOT_FOUND" from the
        // originating DC, we can be sure the operation will end up with a NOT_FOUND error.
        if (trackedRequestFinalState == TrackedRequestFinalState.NOT_FOUND) {
          if (fromOriginatingDC) {
            originatingDcNotFoundCount++;
          }
        }
    }
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  private class OpTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      return inflightCount < parallelism && replicaIterator.hasNext();
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastReturnedByIterator = replicaIterator.next();
      return lastReturnedByIterator;
    }
  }

  String maybeReassignOriginatingDC(List<? extends ReplicaId> replicas) {
    // In a case where a certain dc is decommissioned and blobs previously uploaded to this dc now have an unrecognizable
    // dc id. Current clustermap code will treat originating dc as null if dc id is not identifiable. To improve success
    // rate of cross-colo requests(GET/DELETE/TTLUpdate), operation tracker should be allowed to try remote dc with most
    // replicas first. This is useful in cluster with "unbalanced" replica distribution (i.e. 3 replicas in local dc and
    // 1 replica per remote dc)
    String reassignedOriginDc = null;
    if (routerConfig.routerCrossColoRequestToDcWithMostReplicas) {
      Map<String, Long> dcToReplicaCnt = replicas.stream()
          .collect(Collectors.groupingBy(e -> e.getDataNodeId().getDatacenterName(), Collectors.counting()));
      List<Map.Entry<String, Long>> entryList = new ArrayList<>(dcToReplicaCnt.entrySet());
      entryList.sort(Map.Entry.comparingByValue());
      // we assign a dc with most replicas to "originatingDcName", which only takes effect when populating replica pool
      // (replicas in that colo have higher priority than other remote colos). Note that, "this.originatingDcName" still
      // keeps the actual originating dc name (which is null). This value forces operation tracker to go through replicas
      // in all dc(s) rather than terminating on not found in originating dc.
      reassignedOriginDc = entryList.get(entryList.size() - 1).getKey();
      logger.debug("Originating dc name is null and has been re-assigned to {}", reassignedOriginDc);
    }
    return reassignedOriginDc;
  }

  List<ReplicaId> getEligibleReplicasForOperation(RouterOperation operation) {
    // PutOperation only send requests to local datacenter
    String dcToCheck = operation == RouterOperation.PutOperation ? datacenterName : null;
    List<ReplicaId> replicasByState =
        getEligibleReplicas(dcToCheck, routerOperationToEligibleReplicaStates.get(routerOperation));
    if (operation != RouterOperation.PutOperation) {// All
      // All replicas in originating DC should be valid
      partitionId.getReplicaIds()
          .stream()
          .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName))
          .filter(replicaId -> !replicasByState.contains(replicaId))
          .forEach(replicaId -> replicasByState.add(replicaId));
    }
    return replicasByState;
  }

  /**
   * Get eligible replicas by states for the specified data center. If dcName is null, it gets all eligible
   * replicas from all data centers.
   * @param dcName the name of data center from which the replicas should come from. This can be {@code null}.
   * @param states a set of {@link ReplicaState}(s) that replicas should match.
   * @return a list of eligible replicas that are in specified states.
   */
  private List<ReplicaId> getEligibleReplicas(String dcName, EnumSet<ReplicaState> states) {
    Map<ReplicaState, List<ReplicaId>> replicasByState = getReplicasByState(dcName, states);
    List<ReplicaId> eligibleReplicas = new ArrayList<>();
    for (List<ReplicaId> replicas : replicasByState.values()) {
      eligibleReplicas.addAll(replicas);
    }
    return eligibleReplicas;
  }

  /**
   * Get replicas in required states for the specified datacenter.
   * @param dcName the name of data center from which the replicas should come from. This can be {@code null}.
   * @param states a set of {@link ReplicaState}(s) that replicas should match.
   * @return a map whose key is {@link ReplicaState} and value is a list of {@link ReplicaId}(s) in that state.
   */
  Map<ReplicaState, List<ReplicaId>> getReplicasByState(String dcName, EnumSet<ReplicaState> states) {
    Map<ReplicaState, List<ReplicaId>> map = new HashMap<>();
    for (ReplicaState replicaState : states) {
      if (allDcReplicasByState.containsKey(replicaState)) {
        for (ReplicaId replicaId : allDcReplicasByState.get(replicaState)) {
          if (dcName == null || replicaId.getDataNodeId().getDatacenterName().equals(dcName)) {
            map.putIfAbsent(replicaState, new ArrayList<>());
            map.get(replicaState).add(replicaId);
          }
        }
      }
    }
    return map;
  }

  public boolean hasFailed() {
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      return totalReplicaCount - failedCount < Math.max(totalReplicaCount - 1,
          routerConfig.routerPutSuccessTarget + disabledCount);
    } else {
      // if we already failed FC, and the total is TC, when TC - FC < target, we are no long able to achieve the target.
      // For example, total is 9, target is 3, failed 6, then we still have a chance.
      // But when the failed is 7, we are done.
      if (totalReplicaCount - failedCount < successTarget) {
        return true;
      }
      return hasFailedOnNotFound();
    }
  }

  /**
   * Exposed for testing only.
   * @return the number of replicas in current replica pool.
   */
  int getReplicaPoolSize() {
    return replicaPool.size();
  }

  /**
   * Add a replica to the beginning of the replica pool linked list.
   * @param replicaId the replica to add.
   */
  private void addToBeginningOfPool(ReplicaId replicaId) {
    replicaPool.addFirst(replicaId);
  }

  /**
   * Add a replica to the end of the replica pool linked list.
   * @param replicaId the replica to add.
   */
  private void addToEndOfPool(ReplicaId replicaId) {
    replicaPool.addLast(replicaId);
  }

  /**
   * Helper function to catch a potential race condition in
   * {@link SimpleOperationTracker#SimpleOperationTracker(RouterConfig, RouterOperation, PartitionId, String, boolean, NonBlockingRouterMetrics)}.
   *  @param partitionId The partition on which the operation is performed.
   * @param examinedReplicas All replicas examined.
   * @param replicaPool Replicas added to replicaPool.
   * @param backupReplicas Replicas added to backupReplicas.
   * @param downReplicas Replicas added to downReplicas.
   * @param routerOperation The operation type associated with current operation tracker.
   */
  private static String generateErrorMessage(PartitionId partitionId, List<ReplicaId> examinedReplicas,
      List<ReplicaId> replicaPool, List<ReplicaId> backupReplicas, List<ReplicaId> downReplicas,
      RouterOperation routerOperation) {
    StringBuilder errMsg = new StringBuilder("Total Replica count ").append(replicaPool.size())
        .append(" is less than success target. ")
        .append("Router operation is ")
        .append(routerOperation)
        .append(". Partition is ")
        .append(partitionId)
        .append(", partition class is ")
        .append(partitionId.getPartitionClass())
        .append(" and associated resource is ")
        .append(partitionId.getResourceName())
        .append(". examinedReplicas: ");
    for (ReplicaId replicaId : examinedReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("replicaPool: ");
    for (ReplicaId replicaId : replicaPool) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("backupReplicas: ");
    for (ReplicaId replicaId : backupReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("downReplicas: ");
    for (ReplicaId replicaId : downReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    return errMsg.toString();
  }
}



