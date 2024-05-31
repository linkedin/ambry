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

/**
 * An implementation of {@link OperationTracker} for PUTs only (it does not support any other request types). It differs
 * from {@link SimpleOperationTracker} in that it is "paranoid" about durability. In addition to writing the new blob to
 * the required number of local replicas, it also requires writes to succeed for at least one replica in one remote
 * data center, effectively circumventing cross-datacenter replication (configurable). This is useful for clients such
 * as Samza, that require strong durability guarantees.
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
  private final Map<String, LinkedList<ReplicaId>> replicaPoolByDc = new HashMap<>();
  private final ParanoidDurabilityTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaByDcIterator;
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
    localReplicaSuccessTarget = routerConfig.routerPutLocalSuccessTarget;
    remoteReplicaSuccessTarget = routerConfig.routerPutRemoteSuccessTarget;

    List<ReplicaId> allEligibleReplicas = getEligibleReplicas(datacenterName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
    addReplicasToPool(allEligibleReplicas);
    this.otIterator = new ParanoidDurabilityTrackerIterator();
  }

  private void addReplicasToPool(List<? extends ReplicaId> replicas) {
    Collections.shuffle(replicas);

    for (ReplicaId replicaId : replicas) {
      if (!replicaId.isDown()) {
          addToBeginningOfPool(replicaId);
      } else {
        addToEndOfPool(replicaId); // As a last ditch attempt, we may still try to write to a down replica.
      }
    }

    if(!enoughReplicas()) {
      throw new IllegalArgumentException(generateErrorMessage(partitionId));
    }
    initializeTracking();
  }

  private void initializeTracking() {
    replicaSuccessCountByDc = new HashMap<String, Integer>();
    replicaInPoolOrFlightCountByDc = new HashMap<String, Integer>();
    remoteDatacenters = new ArrayList<String>();

    replicaPoolByDc.forEach((dcName, v) -> {
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

  private String generateErrorMessage(PartitionId partitionId) {
    StringBuilder errMsg = new StringBuilder("Partition " + partitionId + " has too few replicas to satisfy paranoid durability requirements. ");
    replicaPoolByDc.forEach((dcName, v) -> {
      errMsg.append("Datacenter: ").append(dcName).append(" has ").append(v.size()).append(" replicas in LEADER or STANDBY. ");
    });
    return errMsg.toString();
  }

  private boolean enoughReplicas() {
    for (String currentDc : replicaPoolByDc.keySet()) {
      if(currentDc.equals(datacenterName)) {
        if (replicaPoolByDc.get(currentDc).size() < localReplicaSuccessTarget) {
          logger.error("Not enough replicas in local data center for partition " + partitionId +
              " to satisfy paranoid durability requirements " +
              "(wanted " + localReplicaSuccessTarget + ", but found " + replicaPoolByDc.get(currentDc).size() + ")");
          return false;
        }
      } else {
        if (replicaPoolByDc.get(currentDc).size() < remoteReplicaSuccessTarget) {
          logger.error("Not enough replicas in remote data center " + currentDc + " for partition " + partitionId
              + " to satisfy paranoid durability requirements " +
              "(wanted " + remoteReplicaSuccessTarget + ", but found " + replicaPoolByDc.get(currentDc).size() + ")");
          return false;
        }
      }
    }
    return true;
  }

  public boolean hasSucceeded() {
    return hasSucceededLocally() && hasSucceededRemotely();
  }

  private boolean hasSucceededLocally() {
    return localReplicaSuccessCount >= localReplicaSuccessTarget;
  }

  private boolean hasSucceededRemotely() {
    return remoteReplicaSuccessCount >= remoteReplicaSuccessTarget;
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
        .addLast(replicaId);
  }

  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    modifyReplicasInPoolOrInFlightCount(replicaId, -1);

    String dcName = replicaId.getDataNodeId().getDatacenterName();

    switch (trackedRequestFinalState) {
      case SUCCESS:
        replicaSuccessCountByDc.put(dcName, replicaSuccessCountByDc.get(dcName) + 1);
        if (dcName.equals(datacenterName)) {
          localReplicaSuccessCount++;
        } else {
          remoteReplicaSuccessCount++;
        }
        break;
      case REQUEST_DISABLED:
        disabledCountByDc.put(dcName, disabledCountByDc.get(dcName) + 1);
        break;
      default:
        failedCountByDc.put(dcName, failedCountByDc.get(dcName) + 1);
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


  int getCurrentLocalParallelism() {
    return replicaParallelism;
  }

  int getCurrentRemoteParallelism() {
    return remoteReplicaParallelism;
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
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
        throw new NoSuchElementException();
      }

      if (hasNextLocal()) {
        lastReturnedByIterator = replicaPoolByDc.get(datacenterName).removeFirst();
      } else {
        // hasNextRemote() must be true
        String nextColo = getNextRemoteDatacenter();
        lastReturnedByIterator = replicaPoolByDc.get(nextColo).removeFirst();
      }
      return lastReturnedByIterator;
    }

    private boolean hasNextLocal() {
      return localInflightCount < getCurrentLocalParallelism() && !replicaPoolByDc.get(datacenterName).isEmpty();
    }

    private boolean hasNextRemote() {
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

    /**
     * Get the next remote DC to send a request to. This method should only get called if there are remote
     * replicas left to send requests to, so there must be at least one remote DC that has at least one replica left.
     * @return the name of the next remote data center to send a request to.
     */
    private String getNextRemoteDatacenter()  {
      int i = 0;
      int remoteDatacenterCount = remoteDatacenters.size();
      String nextDatacenter;
      do {
        nextDatacenter = remoteDatacenters.get(currentDatacenterIndex);
        currentDatacenterIndex = (currentDatacenterIndex + 1) % remoteDatacenterCount;
        i++;
      } while (replicaPoolByDc.get(nextDatacenter).isEmpty() &&
          i <= remoteDatacenterCount);                           // Guard against infinite loop
      return nextDatacenter;
    }
  }
}