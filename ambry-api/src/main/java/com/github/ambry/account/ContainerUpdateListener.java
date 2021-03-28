package com.github.ambry.account;

/**
 * Interface to implement a listener that will be called on updates to {@link Container}.
 */
public interface ContainerUpdateListener {

  /**
   * Called when there is an update to a container.
   * @param container {@link Container} object that was updated.
   */
  void onContainerUpdate(Container container);
}
