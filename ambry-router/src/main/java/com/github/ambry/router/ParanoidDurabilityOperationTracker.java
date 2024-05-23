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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Tommy: paranoidDurabilityAddReplicasToPool() needs all the eligible replicas not just the local ones.
//        Where is addReplicasToPool() called from? ---> Towards the end of the SimpleOperationTracker constructor.
//        Both AmbryPartition and Partition implement getReplicaIds(). What determines which one is used?
/**
 * An implementation of {@link OperationTracker} for PUTs only (it does not support any other request types). It differs
 * from {@link SimpleOperationTracker} in that it is "paranoid" about durability. In addition to writing the new blob to
 * the required number of local replicas, it will also require writes to succeed for at least one replica in each remote
 * data center, effectively circumventing cross-datacenter replication. This is useful for clients such as Samza, that
 * require strong durability guarantees.
 *
 */
public class ParanoidDurabilityOperationTracker extends SimpleOperationTracker {
  private static final Logger logger = LoggerFactory.getLogger(ParanoidDurabilityOperationTracker.class);
  protected final int localReplicaSuccessTarget;
  protected int remoteReplicaSuccessTarget = 0;
  protected final int localReplicaParallelism;
  protected final int remoteReplicaParallelism;
  protected int localInflightCount = 0;
  protected int remoteInflightCount = 0;
  protected int localReplicaSuccessCount = 0;
  protected int remoteReplicaSuccessCount = 0;
  private Map<String, LinkedList<ReplicaId>> replicaPoolByDc = new HashMap<>();
  private Iterator<ReplicaId> replicaByDcIterator;
  private Map<String, Integer> inflightCountByDc;
  private Map<String, Integer> failedCountByDc;
  private Map<String, Integer> replicaSuccessCountByDc;
  private Map<String, Integer> disabledCountByDc;
  private Map<String, Integer> replicaInPoolOrFlightCountByDc;
  private Map<String, Integer> totalReplicaCountByDc;
  private Set<String> allDatacenters;
  private List<String> remoteDatacenters;

  ParanoidDurabilityOperationTracker(RouterConfig routerConfig, PartitionId partitionId, String originatingDcName,
      NonBlockingRouterMetrics routerMetrics) {
    super(routerConfig, RouterOperation.PutOperation, partitionId, originatingDcName, true, routerMetrics);

    localReplicaParallelism = routerConfig.routerPutLocalRequestParallelism;
    remoteReplicaParallelism = routerConfig.routerPutRemoteRequestParallelism;

    // In SimpleOperationTracker, what do we actually do with eligibleReplicas, and do we really need this variable here?
    // Note this statement gets the list of eligible replicas in the LOCAL DC only.
    // Tommy: We should not use the local replicas here, we should just get a list of ALL the replicas like we do with
    //        the allReplicas variable just below here.
    List<ReplicaId> localEligibleReplicas = getEligibleReplicas(datacenterName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
    List<ReplicaId> offlineReplicas = new ArrayList<>();
    // This is a list of all replicas for this partition. This comes from replica information updated by us in helix
    // Data node configs via bootstrap tool and not external view. This would be same as ideal view.
    List<ReplicaId> allReplicas = new ArrayList<>(partitionId.getReplicaIds());

    localReplicaSuccessTarget =
        routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.max(localEligibleReplicas.size() - 1, routerConfig.routerPutSuccessTarget) : routerConfig.routerPutSuccessTarget;
    remoteReplicaSuccessTarget = routerConfig.routerPutRemoteSuccessTarget;

    // Tommy: End of switch statement operations.

    addReplicasToPool(allReplicas, offlineReplicas, true, false);


  }


  // Note that the replica list that is input to this method needs to have all the eligible replicas from all the
  // data centers, not just the local one.
  // Tommy: What did we decide for this method to return? A list or a map?
  private void addReplicasToPool(List<? extends ReplicaId> replicas, List<? extends ReplicaId> offlineReplicas, boolean shuffleReplicas, boolean crossColoEnabled) {
    LinkedList<ReplicaId> backupReplicas = new LinkedList<>();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    if (shuffleReplicas) {
      Collections.shuffle(replicas);
    }

    // The priority here is local dc replicas, originating dc replicas, other dc replicas, down replicas.
    // To improve read-after-write performance across DC, we prefer to take local and originating replicas only,
    // which can be done by setting includeNonOriginatingDcReplicas False.
    List<ReplicaId> examinedReplicas = new ArrayList<>();
    int numLocalAndLiveReplicas = 0;
    int numRemoteOriginatingDcAndLiveReplicas = 0;
    for (ReplicaId replicaId : replicas) {
      examinedReplicas.add(replicaId);
      String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
      boolean isLocalDcReplica = replicaDcName.equals(datacenterName);
      boolean isOriginatingDcReplica = replicaDcName.equals(originatingDcName);

      if (!replicaId.isDown()) {
        if (isLocalDcReplica) {
          numLocalAndLiveReplicas++;
          addToBeginningOfPool(replicaId);
        } else if (crossColoEnabled && isOriginatingDcReplica) {
          numRemoteOriginatingDcAndLiveReplicas++;
          addToEndOfPool(replicaId);
        } else if (crossColoEnabled) {
          backupReplicas.addFirst(replicaId);
        }
      } else {
        if (isLocalDcReplica) {             // Paranoid durability: I think this should just work since we changed
          downReplicas.addFirst(replicaId); // addToBeginningOfPool() and addToEndOfPool().
        } else if (crossColoEnabled) {
          downReplicas.addLast(replicaId);
        }
      }
    }
    List<ReplicaId> backupReplicasToCheck = new ArrayList<>(backupReplicas);
    List<ReplicaId> downReplicasToCheck = new ArrayList<>(downReplicas);

    // Add replicas that are neither in local dc nor in originating dc.
    backupReplicas.forEach(this::addToEndOfPool);

    if(!enoughReplicas()) {
      // Tommy: Add a simpler exception here that doesn't list out all of the replica information, just refer to Helix.
      throw new IllegalArgumentException(
          generateErrorMessage(partitionId, examinedReplicas, replicaPool, backupReplicasToCheck, downReplicasToCheck,
              routerOperation));
    }

    initializeTracking();
  }

  private void initializeTracking() {
    inflightCountByDc = new HashMap<String, Integer>();
    replicaSuccessCountByDc = new HashMap<String, Integer>();
    replicaInPoolOrFlightCountByDc = new HashMap<String, Integer>();
    remoteDatacenters = new ArrayList<String>();

    replicaPoolByDc.forEach((dcName, v) -> {
      inflightCountByDc.put(dcName, 0);
      failedCountByDc.put(dcName, 0);
      replicaSuccessCountByDc.put(dcName, 0);
      replicaInPoolOrFlightCountByDc.put(dcName, 0);
      disabledCountByDc.put(dcName, 0);
      totalReplicaCountByDc.put(dcName, v.size());
      allDatacenters.add(dcName);
      if(!dcName.equals(datacenterName)) {
        remoteDatacenters.add(dcName);
      }
    });
  }

  // Tommy: From here do we have access to ClusterMapUtils??? NO
  // How do you construct ClusterMapUtils?
  private boolean enoughReplicas() {
    for (String currentDc : replicaPoolByDc.keySet()) {
      // return false if the local data center has too few replicas (must check that dc name is the local one first)
      // return false if any SINGLE remote data center has too few replicas
      if(currentDc.equals(datacenterName)) {
        if (replicaPoolByDc.get(currentDc).size() < localReplicaSuccessTarget) {
          logger.error("Not enough replicas in local data center for partition " + partitionId + " to satisfy paranoid durability requirements");
          return false;
        }
      } else {
        if (replicaPoolByDc.get(currentDc).size() < remoteReplicaSuccessTarget) {
          logger.error("Not enough replicas in remote data center " + currentDc + " for partition " + partitionId + " to satisfy paranoid durability requirements");
          return false;
        }
      }
    }
    return true;
  }

  public boolean hasSucceeded() {
    boolean hasSucceeded;
    // Tommy: The original method in SimpleOperationTracker checks whether or not the current routerOperation is
    //        a RouterOperation.PutOperation, but we already know that must be true if this class is being used at all.
    if (routerConfig.routerPutUseDynamicSuccessTarget) {
      // this logic only applies to replicas where the quorum can change during replica movement
      int dynamicSuccessTarget = Math.max(totalReplicaCount - disabledCount - 1, routerConfig.routerPutSuccessTarget);
      hasSucceeded = replicaSuccessCount >= dynamicSuccessTarget; // What do we do here for paranoid durability? TODO for now.
    } else {
      hasSucceeded = (localReplicaSuccessCount >= localReplicaSuccessTarget) && (remoteReplicaSuccessCount >= remoteReplicaSuccessTarget);
    }
    return hasSucceeded;
  }

  // Tommy: Copied from SimpleOperationTracker, due to private access. Should we change SimpleOperationTracker such
  //        that the method is protected instead of private?
  private List<ReplicaId> getEligibleReplicas(String dcName, EnumSet<ReplicaState> states) {
    Map<ReplicaState, List<ReplicaId>> replicasByState = getReplicasByState(dcName, states);
    List<ReplicaId> eligibleReplicas = new ArrayList<>();
    for (List<ReplicaId> replicas : replicasByState.values()) {
      eligibleReplicas.addAll(replicas);
    }
    return eligibleReplicas;
  }

  /**
   * Add a replica to the beginning of the replica pool linked list for the given data center.
   * @param replicaId the replica to add.
   */
  private void addToBeginningOfPool(ReplicaId replicaId) {
    modifyReplicasInPoolOrInFlightCount(replicaId, 1);
    replicaPoolByDc.computeIfAbsent(replicaId.getDataNodeId().getDatacenterName(), k -> new LinkedList<>())
        .addFirst(replicaId);
  }


  /**
   * Add a replica to the end of the replica pool linked list for the given data center.
   * @param replicaId the replica to add.
   */
  private void addToEndOfPool(ReplicaId replicaId) {
    modifyReplicasInPoolOrInFlightCount(replicaId, 1);
    replicaPoolByDc.computeIfAbsent(replicaId.getDataNodeId().getDatacenterName(), k -> new LinkedList<>())
        .addFirst(replicaId);
  }

  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    modifyReplicasInPoolOrInFlightCount(replicaId, -1); // Tommy: This counter only used for offline repair so can ignore?
    modifyReplicasInPoolOrInFlightCount(replicaId, -1);

    String dcName = replicaId.getDataNodeId().getDatacenterName();

    switch (trackedRequestFinalState) { // Paranoid durability - do we need to consider this switch block at all?
      case SUCCESS:
        replicaSuccessCountByDc.put(dcName, replicaSuccessCountByDc.get(dcName) + 1);
        break;
      // Request disabled may happen when PUT/DELETE/TTLUpdate requests attempt to perform on replicas that are being
      // decommissioned (i.e STANDBY -> INACTIVE). This is because decommission may take some time and frontends still
      // hold old view. Aforementioned requests are rejected by server with Temporarily_Disabled error. For DELETE/TTLUpdate,
      // even though we may receive such errors, the success target is still same(=2). For PUT, we have to adjust the
      // success target (quorum) to let some PUT operations (with at least 2 requests succeeded on new replicas) succeed.
      // Currently, disabledCount only applies to PUT operation.
      case REQUEST_DISABLED:
        disabledCountByDc.put(dcName, disabledCountByDc.get(dcName) + 1);
        break;
      default:
        failedCountByDc.put(dcName, failedCountByDc.get(dcName) + 1);
        // NOT_FOUND is a special error. When tracker sees >= numReplicasInOriginatingDc - 1 "NOT_FOUND" from the
        // originating DC, we can be sure the operation will end up with a NOT_FOUND error.
        if (trackedRequestFinalState == TrackedRequestFinalState.NOT_FOUND) {
          totalNotFoundCount++;
          if (replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName)) {
            if (originatingDcLeaderOrStandbyReplicas.contains(replicaId)) {
              // Since bootstrap replicas could still be catching up, treat NotFound responses only from leader or
              // standby replicas as valid.
              originatingDcNotFoundCount++;
            }
          }
        }
    }
  }

  public boolean hasFailed() {
    if (routerConfig.routerPutUseDynamicSuccessTarget) {
      for(String dcName : allDatacenters) {
        if (dcName.equals(datacenterName)) {  // Local colo
          if (hasFailedDynamically(dcName, routerConfig.routerPutSuccessTarget)) return true;
        } else {
          if (hasFailedDynamically(dcName, routerConfig.routerPutRemoteSuccessTarget)) return true;
        }
      }
      return false;
    } else {
      return hasFailedLocally() || hasFailedRemotely();
    }
  }

  private boolean hasFailedDynamically(String dc, int successTarget)  {
    return (totalReplicaCountByDc.get(dc) - failedCountByDc.get(dc)) < Math.max(totalReplicaCountByDc.get(dc) - 1, successTarget + disabledCountByDc.get(dc));
  }

  private boolean hasFailedRemotely() {
    for(String dcName : allDatacenters) {
      if(!dcName.equals(datacenterName)) {
        if (replicaSuccessCountByDc.get(dcName) < Math.max(totalReplicaCountByDc.get(dcName) - 1,
            routerConfig.routerPutRemoteSuccessTarget + disabledCountByDc.get(dcName))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasFailedLocally() {
    return replicaInPoolOrFlightCountByDc.get(datacenterName) + replicaSuccessCountByDc.get(datacenterName) < replicaSuccessTarget;
  }

  /**
   * Add {@code delta} to a replicas in pool or in flight counter.
   * @param delta the value to add to the counter.
   */
  private void modifyReplicasInPoolOrInFlightCount(ReplicaId replica, int delta) {
    String dcName = replica.getDataNodeId().getDatacenterName();
    replicaInPoolOrFlightCountByDc.put(dcName, replicaInPoolOrFlightCountByDc.get(dcName) + delta);
  }

  // Is it OK to use this method instead of the getCurrentParallelism() method in SimpleOperationTracker?
  // The name is different but not the contents.
  int getCurrentLocalParallelism() {
    return replicaParallelism;
  }

  int getCurrentRemoteParallelism() {
    return remoteReplicaParallelism;
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();                  //Iterator<ReplicaId>, since replicaPool is a List<ReplicaId>
    // replicaByDcIterator will be a new iterator for the map
    return otIterator;
  }

  private class ParanoidDurabilityTrackerIterator implements Iterator<ReplicaId> {
    private int currentDatacenterIndex = 0;

    @Override
    public boolean hasNext() {
      return hasNextLocal() || hasNextRemote();
    }

    @Override
    public void remove() {
      if(isLocalReplica(lastReturnedByIterator)){
        localInflightCount++;
      } else {
        remoteInflightCount++;
      }
      replicaPoolByDc.get(lastReturnedByIterator.getDataNodeId().getDatacenterName()).remove(lastReturnedByIterator);
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException(); // Tommy: Double check we're doing the same thing as SimpleOperationTracker so callers can handle  this exception
      }

      if (hasNextLocal()) {
        lastReturnedByIterator = replicaPoolByDc.get(datacenterName).removeFirst();
      } else { // hasNextRemote() must be true
        String nextColo = getNextRemoteDatacenter();
        lastReturnedByIterator = replicaPoolByDc.get(nextColo).removeFirst();
      }
      return lastReturnedByIterator;
    }

    private boolean hasNextLocal() {
      return localInflightCount < getCurrentLocalParallelism() && !replicaPoolByDc.get(datacenterName).isEmpty();
    }

    private boolean hasNextRemote() {
      // need to keep track of EACH remote data center's inflight count
      // so we can send requests to each remote data center in parallel
      // may need to track lastReturnedByIterator by data center also

      // also need to keep track of the remote success target for each colo
      // double check how this will work!

      // If we set the remote success target to be just 1, then after we receive the first success we can say that
      // we have succeeded remotely. If we require all remote colos to return success, then we effectively rely on all
      // colos to function correctly all the time. We should set the remote parallelism to be 2, so that we can send out
      // two remote calls at once - i.e. one to each DC.
      return remoteInflightCount < getCurrentRemoteParallelism() && remoteReplicasLeft();
    }

    private boolean remoteReplicasLeft() {
      for(String dcName : remoteDatacenters) {
        if(!replicaPoolByDc.get(dcName).isEmpty()) {
          return true;
        }
      }
      return false;
    }

    private boolean isLocalReplica(ReplicaId replica) {
      return replica.getDataNodeId().getDatacenterName().equals(datacenterName);
    }

    // getNextRemoteDatacenter() should only get called if we have remote replicas left to send requests to. So there MUST be a
    // remote data center that has at least one replica left. But we don't know which one, so we have to loop through
    // all of them to find the next one that has a replica left. We should throw an exception or similar if we ever
    // end up looping through all of them without finding a replica.
    private String getNextRemoteDatacenter()  {
      int i = 0;
      int remoteDatacenterCount = remoteDatacenters.size();
      String nextDatacenter;
      do {
        nextDatacenter = remoteDatacenters.get(currentDatacenterIndex);
        currentDatacenterIndex = (currentDatacenterIndex + 1) % remoteDatacenterCount;
        i++;
      } while (replicaPoolByDc.get(nextDatacenter).isEmpty() &&
               i <= remoteDatacenterCount);                     // Guard against infinite loop
      return nextDatacenter;
    }
  }
}

