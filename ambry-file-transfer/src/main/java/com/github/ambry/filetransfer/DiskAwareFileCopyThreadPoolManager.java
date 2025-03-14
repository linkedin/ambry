package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;


public class DiskAwareFileCopyThreadPoolManager implements FileCopyBasedReplicationThreadPoolManager, Runnable {

  private final Map<ReplicaId, FileCopyThread> replicaToFileCopyThread;
  private final Map<DiskId, Set<FileCopyThread>> runningThreads;

  private final int numberOfThreadsPerDisk;
  private final ReentrantLock threadQueueLock;
  private boolean isRunning;
  private final CountDownLatch shutdownLatch;

  public DiskAwareFileCopyThreadPoolManager(List<DiskId> diskIds, int numberOfThreads) {
    this.numberOfThreadsPerDisk = numberOfThreads;
    this.runningThreads = new HashMap<>();
    this.replicaToFileCopyThread = new HashMap<>();
    diskIds.forEach(diskId -> {
      runningThreads.put(diskId, new HashSet<>());
    });
    isRunning = true;
    this.shutdownLatch = new CountDownLatch(1);
    this.threadQueueLock = new ReentrantLock(true);
  }

  @Override
  public int getThreadPoolSize() {
    return numberOfThreadsPerDisk;
  }

  @Override
  public List<DiskId> getDiskIdsToHydrate() {
    List<DiskId> diskIds = new ArrayList<>();
    runningThreads.forEach((diskId, fileCopyThreads) -> {
      if (fileCopyThreads.size() < numberOfThreadsPerDisk) {
        diskIds.add(diskId);
      }
    });
    return diskIds;
  }

  @Override
  public void submitReplicaForHydration(ReplicaId replicaId, FileCopyStatusListener fileCopyStatusListener,
      FileCopyHandler fileCopyHandler) {
    threadQueueLock.lock();
    DiskId diskId = replicaId.getDiskId();
    FileCopyThread fileCopyThread = new FileCopyThread(fileCopyHandler, fileCopyStatusListener);
    fileCopyThread.start();
    runningThreads.get(diskId).add(fileCopyThread);
    replicaToFileCopyThread.put(replicaId, fileCopyThread);
    threadQueueLock.unlock();
  }

  @Override
  public void stopAndRemoveReplicaFromThreadPool(ReplicaId replicaId) throws InterruptedException {
    threadQueueLock.lock();
    FileCopyThread fileCopyThread = replicaToFileCopyThread.get(replicaId);
    if (fileCopyThread == null || !fileCopyThread.isAlive()) {
      return;
    }
    fileCopyThread.shutDown();
    threadQueueLock.unlock();
  }

  @Override
  public void run() {
    while (isRunning || areThreadsRunning()) {
      threadQueueLock.lock();
      runningThreads.forEach((diskId, fileCopyThreads) -> {
        fileCopyThreads.removeIf(fileCopyThread -> !fileCopyThread.isAlive());
      });
      threadQueueLock.unlock();
    }
    shutdownLatch.countDown();
  }

  boolean areThreadsRunning() {
    return runningThreads.values().stream().noneMatch(Set::isEmpty);
  }

  public void shutDown() throws InterruptedException {
    isRunning = false;
    shutdownLatch.await();
  }
}
