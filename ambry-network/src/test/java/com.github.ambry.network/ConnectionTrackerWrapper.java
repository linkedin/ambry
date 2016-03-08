package com.github.ambry.network;

/**
 * A helper class that just exposes the package private {@link ConnectionTracker} publicly for tests outside of this
 * package.
 */
public class ConnectionTrackerWrapper extends ConnectionTracker {
  public ConnectionTrackerWrapper(int maxConnectionPerPortPlainText, int maxConnectionsPerPortSsl) {
    super(maxConnectionPerPortPlainText, maxConnectionsPerPortSsl);
  }
}
