package com.github.ambry.filetransfer;

import com.github.ambry.replica.prioritization.FCFSPrioritizationManager;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import org.junit.Before;
import org.junit.Test;


public class FileCopyBasedReplicationSchedulerTest {
  FileCopyBasedReplicationSchedulerImpl fileCopyBasedReplicationScheduler;

  @Before
  public void initialize(){
    PrioritizationManager prioritizationManager= new FCFSPrioritizationManager();
    prioritizationManager.addReplica();
    fileCopyBasedReplicationScheduler = new FileCopyBasedReplicationSchedulerImpl();
  }
  @Test
  public void testScheduleFileCopy(){

  }
}
