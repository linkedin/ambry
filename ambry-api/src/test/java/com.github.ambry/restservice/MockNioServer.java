package com.github.ambry.restservice;

/**
 * Implementation of {@link NioServer} that can be used in tests.
 * <p/>
 * Does no network I/O for now and is useful only for start(), shutdown() tests.
 */
public class MockNioServer implements NioServer {
  // Defines whether this server is faulty (mainly to check for behaviour under NioServer failures).
  private boolean isFaulty;

  public MockNioServer(boolean isFaulty) {
    this.isFaulty = isFaulty;
  }

  @Override
  public void start()
      throws InstantiationException {
    if (isFaulty) {
      throw new InstantiationException("This is a faulty MockNioServer");
    }
  }

  @Override
  public void shutdown() {
    if (isFaulty) {
      throw new RuntimeException("This is a faulty MockNioServer");
    }
  }

  /**
   * Makes the MockNioServer faulty.
   */
  public void breakdown() {
    isFaulty = true;
  }

  /**
   * Fixes the MockNioServer (not faulty anymore).
   */
  public void fix() {
    isFaulty = false;
  }
}
