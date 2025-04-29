package com.github.ambry.filetransfer;

import java.util.HashMap;


public class BootstrapSessionManager {
  private final HashMap<String, BootstrapSession> bootstrapSessions = new HashMap<>();

  void addBootstrapSession(String snapShotId, String bootstrappingNodeId) {
    BootstrapSession session = new BootstrapSession(snapShotId, null, null,
        true, bootstrappingNodeId);
    bootstrapSessions.put(snapShotId, session);
  }

  void removeBootstrapSession(String snapShotId) {
    bootstrapSessions.remove(snapShotId);
  }

  BootstrapSession getBootstrapSession(String snapShotId) {
    return bootstrapSessions.get(snapShotId);
  }

  boolean isSessionActive(String snapShotId) {
    BootstrapSession session = bootstrapSessions.get(snapShotId);
    return session != null && session.isSessionActive();
  }

  void setSessionInactive(String snapShotId) {
    BootstrapSession session = bootstrapSessions.get(snapShotId);
    if (session != null) {
      session.setSessionInactive();
    }
  }

  void clearAllSessions() {
    bootstrapSessions.clear();
  }
}
