package com.github.ambry.filetransfer;

import com.github.ambry.utils.Time;


public class BootstrapSession {
  private final String snapShotId;
  private final Time deferralTimer;
  private final Time totalTimerSinceCompactionDisabled;
  private boolean isSessionActive;
  private final String bootstrappingNodeId;

  public BootstrapSession(String snapShotId, Time deferralTimer, Time totalTimerSinceCompactionDisabled,
      boolean isSessionActive, String bootstrappingNodeId) {
    this.snapShotId = snapShotId;
    this.deferralTimer = deferralTimer;
    this.totalTimerSinceCompactionDisabled = totalTimerSinceCompactionDisabled;
    this.isSessionActive = isSessionActive;
    this.bootstrappingNodeId = bootstrappingNodeId;
  }

  public boolean isSessionActive() {
    return isSessionActive;
  }

  public void setSessionInactive() {
    isSessionActive = false;
  }
}
