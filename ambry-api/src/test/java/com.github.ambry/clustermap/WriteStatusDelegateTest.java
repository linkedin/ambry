/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.github.ambry.clustermap;

import org.junit.Test;

import static org.mockito.Mockito.*;


public class WriteStatusDelegateTest {

  /**
   * Tests WriteStatusDelegate
   */
  @Test
  public void testDelegate() {
    //Initializes delegate and arguments
    ClusterParticipant clusterParticipant = mock(ClusterParticipant.class);
    ReplicaId replicaId = mock(ReplicaId.class);
    WriteStatusDelegate delegate = new WriteStatusDelegate(clusterParticipant);

    //Checks that the right underlying ClusterParticipant methods are called
    verifyZeroInteractions(clusterParticipant);
    delegate.seal(replicaId);
    verify(clusterParticipant).setReplicaSealedState(replicaId, true);
    verifyNoMoreInteractions(clusterParticipant);
    delegate.unseal(replicaId);
    verify(clusterParticipant).setReplicaSealedState(replicaId, false);
    verifyNoMoreInteractions(clusterParticipant);
  }
}
